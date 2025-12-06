# Contributing Translations to VibeSQL

This guide explains how to contribute translations to VibeSQL. Whether you want to add a new language or improve existing translations, you can do so without any Rust knowledge.

## Table of Contents

- [Quick Start](#quick-start)
- [How Fluent Works](#how-fluent-works)
- [Message ID Naming Conventions](#message-id-naming-conventions)
- [Adding a New Language](#adding-a-new-language)
- [Updating Existing Translations](#updating-existing-translations)
- [Testing Your Translations](#testing-your-translations)
- [CI Validation](#ci-validation)
- [Best Practices](#best-practices)

## Quick Start

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/vibesql.git
   cd vibesql
   ```

2. **Find the translation files**
   ```
   crates/vibesql-l10n/resources/
   ├── en-US/           # English (US) - reference locale
   │   ├── cli.ftl      # CLI messages
   │   ├── executor.ftl # Query executor errors
   │   ├── storage.ftl  # Storage layer errors
   │   └── catalog.ftl  # Catalog/schema errors
   ├── es/              # Spanish
   │   └── ...
   └── ja-JP/           # Japanese
       └── ...
   ```

3. **Edit or create translation files** using any text editor

4. **Test your changes**
   ```bash
   # Run the translation validation script
   python scripts/check-translations.py

   # Test with the CLI
   VIBESQL_LANG=es cargo run --release --bin vibesql
   ```

5. **Submit a pull request**

## How Fluent Works

VibeSQL uses [Project Fluent](https://projectfluent.org/) for localization. Fluent is a modern localization system designed to be both powerful and easy to use.

### Basic Syntax

**Simple messages:**
```ftl
cli-goodbye = Goodbye!
```

**Messages with variables:**
```ftl
cli-banner = VibeSQL v{ $version } - SQL:1999 FULL Compliance Database
```

**Messages with plural forms:**
```ftl
rows-count = { $count ->
    [one] { $count } row
   *[other] { $count } rows
}
```

**Multi-line messages:**
```ftl
cli-long-about = VibeSQL command-line interface

    USAGE MODES:
      Interactive REPL:    vibesql (--database <FILE>)
      Execute Command:     vibesql -c "SELECT * FROM users"
```

**Messages with attributes:**
```ftl
executor-procedure-not-found-with-suggestion = Procedure '{ $procedure_name }' not found
    .available = Available procedures: { $available_procedures }
    .suggestion = Did you mean '{ $suggestion }'?
```

### Variables

Variables use the `{ $variable_name }` syntax:
- `{ $name }` - simple variable
- `{ $count }` - numeric variable (can be used with plurals)
- `{ NUMBER($bytes, useGrouping: 0) }` - formatted number

### Selectors (Plurals and Variants)

```ftl
executor-select-into-row-count = Procedural SELECT INTO must return exactly { $expected } row, got { $actual } row{ $plural ->
    [s] s
   *[]
}
```

The `*` marks the default variant.

## Message ID Naming Conventions

All message IDs follow a consistent naming pattern:

```
<crate>-<category>-<description>
```

### Prefix by Crate

| Prefix | Crate | Description |
|--------|-------|-------------|
| `cli-` | vibesql (CLI) | User-facing CLI messages |
| `executor-` | vibesql-executor | Query execution errors |
| `storage-` | vibesql-storage | Storage layer errors |
| `catalog-` | vibesql-catalog | Schema/catalog errors |

### Category Examples

| Category | Example Message ID |
|----------|-------------------|
| Table operations | `executor-table-not-found` |
| Column operations | `executor-column-not-found-simple` |
| Index operations | `storage-index-already-exists` |
| Type errors | `executor-type-mismatch` |
| Constraints | `storage-null-constraint-violation` |

### Variants

When a message has multiple forms, use suffixes:
- `-simple` - basic form without extra context
- `-with-available` - includes list of available options
- `-with-suggestion` - includes a "did you mean?" suggestion

Example:
```ftl
executor-function-not-found-simple = Function '{ $function_name }' not found
executor-function-not-found-with-available = Function '{ $function_name }' not found
    .available = Available functions: { $available_functions }
executor-function-not-found-with-suggestion = Function '{ $function_name }' not found
    .suggestion = Did you mean '{ $suggestion }'?
```

## Adding a New Language

### Step 1: Create the Locale Directory

Create a new directory under `crates/vibesql-l10n/resources/` with the locale code:

```bash
mkdir crates/vibesql-l10n/resources/fr  # French
# or
mkdir crates/vibesql-l10n/resources/zh-CN  # Chinese (Simplified)
```

Use standard locale codes:
- `fr` - French
- `de` - German
- `ja-JP` - Japanese (Japan)
- `zh-CN` - Chinese (Simplified)
- `pt-BR` - Portuguese (Brazil)

### Step 2: Copy the Reference Files

Copy all `.ftl` files from `en-US`:

```bash
cp crates/vibesql-l10n/resources/en-US/*.ftl crates/vibesql-l10n/resources/fr/
```

### Step 3: Translate the Messages

Edit each `.ftl` file and translate the messages. Keep the message IDs unchanged - only translate the values.

**Before (English):**
```ftl
cli-goodbye = Goodbye!
executor-table-not-found = Table '{ $name }' not found
```

**After (French):**
```ftl
cli-goodbye = Au revoir !
executor-table-not-found = Table '{ $name }' non trouvée
```

### Step 4: Update the Checklist

Add your language to the translation coverage in the README if applicable.

### Language Addition Checklist

- [ ] Create locale directory with proper code
- [ ] Copy all `.ftl` files from `en-US`
- [ ] Translate all messages in `cli.ftl`
- [ ] Translate all messages in `executor.ftl`
- [ ] Translate all messages in `storage.ftl`
- [ ] Translate all messages in `catalog.ftl`
- [ ] Run `python scripts/check-translations.py`
- [ ] Test with `VIBESQL_LANG=<locale> cargo run --bin vibesql`
- [ ] Submit pull request

## Updating Existing Translations

### Finding Outdated Translations

Run the check script to find missing or outdated translations:

```bash
python scripts/check-translations.py
```

This will show:
- Missing message IDs (in en-US but not in your locale)
- Extra message IDs (in your locale but not in en-US)
- Syntax errors in `.ftl` files

### Updating Messages

1. Open the relevant `.ftl` file for your locale
2. Find the message ID that needs updating
3. Update the translation while keeping variables intact
4. Run the validation script to verify

## Testing Your Translations

### Local Testing

**Set the locale and run the CLI:**
```bash
# Using environment variable
VIBESQL_LANG=es cargo run --release --bin vibesql

# Or set it for the session
export VIBESQL_LANG=es
cargo run --release --bin vibesql
```

**Test specific error messages:**
```sql
-- This will trigger a table-not-found error
vibesql> SELECT * FROM nonexistent_table;
```

### Running the Validation Script

```bash
# Check all translations
python scripts/check-translations.py

# Check specific locale
python scripts/check-translations.py --locale es
```

### Running the Fluent Syntax Check

```bash
cargo run -p vibesql-l10n --bin check-ftl
```

This validates:
- All `.ftl` files have valid Fluent syntax
- No parse errors or malformed messages
- All selectors have default variants

## CI Validation

Every pull request automatically runs:

1. **Fluent syntax validation** - Ensures all `.ftl` files are valid
2. **Translation completeness check** - Reports missing translations
3. **Variable consistency check** - Ensures variables match between locales

The CI will:
- **Fail** if any `.ftl` file has invalid syntax
- **Warn** if translations are incomplete (< 100% coverage)
- **Report** coverage percentage for each locale

## Best Practices

### Do

- Keep variable names unchanged: `{ $name }` stays `{ $name }`
- Match the tone of the original message
- Use appropriate plural forms for your language
- Test your translations with actual CLI usage
- Keep translations concise - error messages should be scannable

### Don't

- Translate variable names or message IDs
- Add extra variables that don't exist in the English version
- Remove variables from the translation
- Change the structure of messages with attributes

### Handling Technical Terms

Some technical terms should remain in English:
- SQL keywords: `SELECT`, `INSERT`, `PRIMARY KEY`
- Type names: `VARCHAR`, `INTEGER`
- Error codes or identifiers

**Example:**
```ftl
# Good - keeps SQL keyword
executor-type-mismatch = Nepareizi tipi: { $left } { $op } { $right }

# Bad - translates SQL-related term
executor-type-mismatch = Nepareizi tipi: { $kreisā } { $operators } { $labā }
```

### Consistency

Use consistent terminology throughout:
- If you translate "table" as "Tabelle" in German, use it everywhere
- Keep error message structure similar to the English version
- Use the same punctuation style as other messages in your locale

## Questions?

- Open an issue on GitHub with the `i18n` label
- Check existing translations for reference
- Look at the [Project Fluent documentation](https://projectfluent.org/fluent/guide/)

Thank you for contributing to VibeSQL internationalization!
