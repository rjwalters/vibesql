# VibeSQL Parser/Lexer-Lokalisierung - Deutsch
# Diese Datei enthält alle benutzerfreundlichen Zeichenketten für Lexer- und Parser-Fehler.

# =============================================================================
# Lexer-Fehlerkopf
# =============================================================================

lexer-error-at-position = Lexer-Fehler an Position { $position }: { $message }

# =============================================================================
# Zeichenkettenliteral-Fehler
# =============================================================================

lexer-unterminated-string = Nicht abgeschlossenes Zeichenkettenliteral

# =============================================================================
# Bezeichnerfehler
# =============================================================================

lexer-unterminated-delimited-identifier = Nicht abgeschlossener begrenzter Bezeichner
lexer-empty-delimited-identifier = Leerer begrenzter Bezeichner ist nicht erlaubt

# =============================================================================
# Zahlenliteral-Fehler
# =============================================================================

lexer-invalid-scientific-notation = Ungültige wissenschaftliche Notation: Ziffern nach 'E' erwartet

# =============================================================================
# Platzhalter-Fehler
# =============================================================================

lexer-expected-digit-after-dollar = Ziffer nach '$' für nummerierten Platzhalter erwartet
lexer-invalid-numbered-placeholder = Ungültiger nummerierter Platzhalter: ${ $placeholder }
lexer-numbered-placeholder-zero = Nummerierter Platzhalter muss $1 oder höher sein (kein $0)
lexer-expected-identifier-after-colon = Bezeichner nach ':' für benannten Platzhalter erwartet

# =============================================================================
# Variablenfehler
# =============================================================================

lexer-expected-variable-after-at-at = Variablenname nach @@ erwartet
lexer-expected-variable-after-at = Variablenname nach @ erwartet

# =============================================================================
# Operatorfehler
# =============================================================================

lexer-unexpected-pipe = Unerwartetes Zeichen: '|' (meinten Sie '||'?)

# =============================================================================
# Allgemeine Fehler
# =============================================================================

lexer-unexpected-character = Unerwartetes Zeichen: '{ $character }'
