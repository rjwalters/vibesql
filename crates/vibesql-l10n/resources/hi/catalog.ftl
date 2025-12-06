# VibeSQL Catalog Error Messages - Hindi (hi)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = टेबल '{ $name }' पहले से मौजूद है
catalog-table-not-found = टेबल '{ $table_name }' नहीं मिला

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = कॉलम '{ $name }' पहले से मौजूद है
catalog-column-not-found = कॉलम '{ $column_name }' टेबल '{ $table_name }' में नहीं मिला

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = स्कीमा '{ $name }' पहले से मौजूद है
catalog-schema-not-found = स्कीमा '{ $name }' नहीं मिला
catalog-schema-not-empty = स्कीमा '{ $name }' खाली नहीं है

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = भूमिका '{ $name }' पहले से मौजूद है
catalog-role-not-found = भूमिका '{ $name }' नहीं मिली

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = डोमेन '{ $name }' पहले से मौजूद है
catalog-domain-not-found = डोमेन '{ $name }' नहीं मिला
catalog-domain-in-use = डोमेन '{ $domain_name }' अभी भी { $count } कॉलम(ओं) द्वारा उपयोग में है: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = सीक्वेंस '{ $name }' पहले से मौजूद है
catalog-sequence-not-found = सीक्वेंस '{ $name }' नहीं मिला
catalog-sequence-in-use = सीक्वेंस '{ $sequence_name }' अभी भी { $count } कॉलम(ओं) द्वारा उपयोग में है: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = टाइप '{ $name }' पहले से मौजूद है
catalog-type-not-found = टाइप '{ $name }' नहीं मिला
catalog-type-in-use = टाइप '{ $name }' अभी भी एक या अधिक टेबल द्वारा उपयोग में है

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = कोलेशन '{ $name }' पहले से मौजूद है
catalog-collation-not-found = कोलेशन '{ $name }' नहीं मिला
catalog-character-set-already-exists = कैरेक्टर सेट '{ $name }' पहले से मौजूद है
catalog-character-set-not-found = कैरेक्टर सेट '{ $name }' नहीं मिला
catalog-translation-already-exists = अनुवाद '{ $name }' पहले से मौजूद है
catalog-translation-not-found = अनुवाद '{ $name }' नहीं मिला

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = व्यू '{ $name }' पहले से मौजूद है
catalog-view-not-found = व्यू '{ $name }' नहीं मिला
catalog-view-in-use = व्यू या टेबल '{ $view_name }' अभी भी { $count } व्यू(ज) द्वारा उपयोग में है: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = ट्रिगर '{ $name }' पहले से मौजूद है
catalog-trigger-not-found = ट्रिगर '{ $name }' नहीं मिला

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = अभिकथन '{ $name }' पहले से मौजूद है
catalog-assertion-not-found = अभिकथन '{ $name }' नहीं मिला

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = फ़ंक्शन '{ $name }' पहले से मौजूद है
catalog-function-not-found = फ़ंक्शन '{ $name }' नहीं मिला
catalog-procedure-already-exists = प्रोसीजर '{ $name }' पहले से मौजूद है
catalog-procedure-not-found = प्रोसीजर '{ $name }' नहीं मिला

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = कॉन्स्ट्रेंट '{ $name }' पहले से मौजूद है
catalog-constraint-not-found = कॉन्स्ट्रेंट '{ $name }' नहीं मिला

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = इंडेक्स '{ $index_name }' टेबल '{ $table_name }' पर पहले से मौजूद है
catalog-index-not-found = इंडेक्स '{ $index_name }' टेबल '{ $table_name }' पर नहीं मिला

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = टेबल '{ $table_name }' के लिए सर्कुलर फॉरेन की निर्भरता पाई गई: { $message }
