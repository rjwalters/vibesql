# VibeSQL Katalog-Fehlermeldungen - Deutsch
# Diese Datei enthält alle Fehlermeldungen für das vibesql-catalog-Crate.

# =============================================================================
# Tabellenfehler
# =============================================================================

catalog-table-already-exists = Tabelle '{ $name }' existiert bereits
catalog-table-not-found = Tabelle '{ $table_name }' nicht gefunden

# =============================================================================
# Spaltenfehler
# =============================================================================

catalog-column-already-exists = Spalte '{ $name }' existiert bereits
catalog-column-not-found = Spalte '{ $column_name }' in Tabelle '{ $table_name }' nicht gefunden

# =============================================================================
# Schema-Fehler
# =============================================================================

catalog-schema-already-exists = Schema '{ $name }' existiert bereits
catalog-schema-not-found = Schema '{ $name }' nicht gefunden
catalog-schema-not-empty = Schema '{ $name }' ist nicht leer

# =============================================================================
# Rollenfehler
# =============================================================================

catalog-role-already-exists = Rolle '{ $name }' existiert bereits
catalog-role-not-found = Rolle '{ $name }' nicht gefunden

# =============================================================================
# Domänenfehler
# =============================================================================

catalog-domain-already-exists = Domäne '{ $name }' existiert bereits
catalog-domain-not-found = Domäne '{ $name }' nicht gefunden
catalog-domain-in-use = Domäne '{ $domain_name }' wird noch von { $count } Spalte(n) verwendet: { $columns }

# =============================================================================
# Sequenzfehler
# =============================================================================

catalog-sequence-already-exists = Sequenz '{ $name }' existiert bereits
catalog-sequence-not-found = Sequenz '{ $name }' nicht gefunden
catalog-sequence-in-use = Sequenz '{ $sequence_name }' wird noch von { $count } Spalte(n) verwendet: { $columns }

# =============================================================================
# Typfehler
# =============================================================================

catalog-type-already-exists = Typ '{ $name }' existiert bereits
catalog-type-not-found = Typ '{ $name }' nicht gefunden
catalog-type-in-use = Typ '{ $name }' wird noch von einer oder mehreren Tabellen verwendet

# =============================================================================
# Kollations- und Zeichensatzfehler
# =============================================================================

catalog-collation-already-exists = Kollation '{ $name }' existiert bereits
catalog-collation-not-found = Kollation '{ $name }' nicht gefunden
catalog-character-set-already-exists = Zeichensatz '{ $name }' existiert bereits
catalog-character-set-not-found = Zeichensatz '{ $name }' nicht gefunden
catalog-translation-already-exists = Übersetzung '{ $name }' existiert bereits
catalog-translation-not-found = Übersetzung '{ $name }' nicht gefunden

# =============================================================================
# Sichtfehler
# =============================================================================

catalog-view-already-exists = Sicht '{ $name }' existiert bereits
catalog-view-not-found = Sicht '{ $name }' nicht gefunden
catalog-view-in-use = Sicht oder Tabelle '{ $view_name }' wird noch von { $count } Sicht(en) verwendet: { $views }

# =============================================================================
# Trigger-Fehler
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' existiert bereits
catalog-trigger-not-found = Trigger '{ $name }' nicht gefunden

# =============================================================================
# Assertionsfehler
# =============================================================================

catalog-assertion-already-exists = Assertion '{ $name }' existiert bereits
catalog-assertion-not-found = Assertion '{ $name }' nicht gefunden

# =============================================================================
# Funktions- und Prozedurfehler
# =============================================================================

catalog-function-already-exists = Funktion '{ $name }' existiert bereits
catalog-function-not-found = Funktion '{ $name }' nicht gefunden
catalog-procedure-already-exists = Prozedur '{ $name }' existiert bereits
catalog-procedure-not-found = Prozedur '{ $name }' nicht gefunden

# =============================================================================
# Constraint-Fehler
# =============================================================================

catalog-constraint-already-exists = Constraint '{ $name }' existiert bereits
catalog-constraint-not-found = Constraint '{ $name }' nicht gefunden

# =============================================================================
# Indexfehler
# =============================================================================

catalog-index-already-exists = Index '{ $index_name }' auf Tabelle '{ $table_name }' existiert bereits
catalog-index-not-found = Index '{ $index_name }' auf Tabelle '{ $table_name }' nicht gefunden

# =============================================================================
# Fremdschlüsselfehler
# =============================================================================

catalog-circular-foreign-key = Zirkuläre Fremdschlüssel-Abhängigkeit für Tabelle '{ $table_name }' erkannt: { $message }
