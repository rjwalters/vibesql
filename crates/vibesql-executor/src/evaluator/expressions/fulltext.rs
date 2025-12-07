//! Full-text search evaluation for MATCH...AGAINST expressions

use crate::errors::ExecutorError;
use std::collections::HashSet;
use vibesql_ast::FulltextMode;

/// Common English stopwords to exclude from searches
const STOPWORDS: &[&str] = &[
    "a",
    "about",
    "after",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "been",
    "before",
    "being",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "down",
    "during",
    "each",
    "even",
    "for",
    "from",
    "further",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "just",
    "me",
    "might",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "out",
    "over",
    "own",
    "same",
    "should",
    "so",
    "some",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "with",
    "would",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
];

/// Tokenize text into words, removing punctuation and converting to lowercase
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '*')
        .filter(|s| !s.is_empty() && s.len() >= 3) // Minimum word length of 3
        .map(|s| s.to_string())
        .collect()
}

/// Check if a word is a stopword
fn is_stopword(word: &str) -> bool {
    STOPWORDS.contains(&word.to_lowercase().as_str())
}

/// Filter out stopwords from tokens
fn remove_stopwords(tokens: Vec<String>) -> Vec<String> {
    tokens.into_iter().filter(|token| !is_stopword(token)).collect()
}

/// Evaluate natural language full-text search
/// Returns true if all search terms are found in any of the text values
pub fn eval_natural_language_search(search_terms: &str, text_values: &[String]) -> bool {
    let search_tokens = remove_stopwords(tokenize(search_terms));
    if search_tokens.is_empty() {
        return true; // Empty search matches everything
    }

    let combined_text = text_values.join(" ");
    let text_tokens = tokenize(&combined_text);
    let text_token_set: HashSet<_> = text_tokens.into_iter().collect();

    // For natural language mode: at least one search term must match
    // (we're implementing basic matching here, not full TF-IDF ranking)
    search_tokens.iter().any(|term| text_token_set.contains(term))
}

/// Evaluate boolean mode full-text search
/// Supports operators: + (required), - (prohibited), * (wildcard), "..." (phrase)
pub fn eval_boolean_search(search_query: &str, text_values: &[String]) -> bool {
    let combined_text = text_values.join(" ");
    let text_lower = combined_text.to_lowercase();

    // Parse boolean query into terms and operators
    let mut required_terms = Vec::new();
    let mut prohibited_terms = Vec::new();
    let mut optional_terms = Vec::new();
    let mut phrases = Vec::new();

    let mut current_op = ' '; // ' ' for optional, '+' for required, '-' for prohibited
    let mut in_phrase = false;
    let mut current_term = String::new();

    for ch in search_query.chars() {
        match ch {
            '+' | '-' => {
                if !current_term.is_empty() && !in_phrase {
                    match current_op {
                        '+' => required_terms.push(current_term.clone()),
                        '-' => prohibited_terms.push(current_term.clone()),
                        _ => optional_terms.push(current_term.clone()),
                    }
                    current_term.clear();
                }
                current_op = ch;
            }
            '"' => {
                if in_phrase {
                    // End of phrase
                    if !current_term.is_empty() {
                        phrases.push(current_term.clone());
                        current_term.clear();
                    }
                    in_phrase = false;
                } else {
                    // Start of phrase
                    in_phrase = true;
                }
            }
            ' ' | '\t' if !in_phrase => {
                if !current_term.is_empty() {
                    match current_op {
                        '+' => required_terms.push(current_term.clone()),
                        '-' => prohibited_terms.push(current_term.clone()),
                        _ => optional_terms.push(current_term.clone()),
                    }
                    current_term.clear();
                    current_op = ' ';
                }
            }
            _ => {
                current_term.push(ch);
            }
        }
    }

    // Handle last term
    if !current_term.is_empty() {
        if in_phrase {
            phrases.push(current_term.clone());
        } else {
            match current_op {
                '+' => required_terms.push(current_term),
                '-' => prohibited_terms.push(current_term),
                _ => optional_terms.push(current_term),
            }
        }
    }

    // Check required terms
    for term in &required_terms {
        if !text_lower.contains(&term.to_lowercase()) {
            return false;
        }
    }

    // Check prohibited terms
    for term in &prohibited_terms {
        if text_lower.contains(&term.to_lowercase()) {
            return false;
        }
    }

    // Check phrases (exact matches)
    for phrase in &phrases {
        if !text_lower.contains(&phrase.to_lowercase()) {
            return false;
        }
    }

    // If we have required terms, they must match
    // If we have optional terms, at least one must match (or we just check required/prohibited)
    optional_terms.is_empty()
        || optional_terms.iter().any(|term| text_lower.contains(&term.to_lowercase()))
}

/// Simple query expansion: extract keywords and re-search
/// This is a simplified implementation - full implementation would use relevance
pub fn eval_query_expansion_search(search_terms: &str, text_values: &[String]) -> bool {
    let combined_text = text_values.join(" ");
    let tokens = tokenize(&combined_text);
    let _tokens_filtered = remove_stopwords(tokens);

    // If the search terms match, do the expansion
    if eval_natural_language_search(search_terms, text_values) {
        // In a full implementation, we would extract the most relevant terms
        // from matched documents and re-search with expanded terms
        // For now, just return true if the initial search matched
        return true;
    }

    false
}

/// Evaluate a MATCH...AGAINST expression
pub fn eval_match_against(
    search_query: &str,
    text_values: &[arcstr::ArcStr],
    mode: &FulltextMode,
) -> Result<bool, ExecutorError> {
    // Skip empty values and convert to strings
    let values: Vec<String> = text_values
        .iter()
        .filter_map(|val| if val.is_empty() { None } else { Some(val.to_string()) })
        .collect();

    if values.is_empty() {
        return Ok(false);
    }

    let result = match mode {
        FulltextMode::NaturalLanguage => eval_natural_language_search(search_query, &values),
        FulltextMode::Boolean => eval_boolean_search(search_query, &values),
        FulltextMode::QueryExpansion => eval_query_expansion_search(search_query, &values),
    };

    Ok(result)
}
