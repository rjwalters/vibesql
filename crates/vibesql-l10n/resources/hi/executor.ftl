# VibeSQL Executor Error Messages - Hindi (hi)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = टेबल '{ $name }' नहीं मिला
executor-table-already-exists = टेबल '{ $name }' पहले से मौजूद है

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = कॉलम '{ $column_name }' टेबल '{ $table_name }' में नहीं मिला
executor-column-not-found-searched = कॉलम '{ $column_name }' नहीं मिला (खोजी गई टेबल: { $searched_tables })
executor-column-not-found-with-available = कॉलम '{ $column_name }' नहीं मिला (खोजी गई टेबल: { $searched_tables })। उपलब्ध कॉलम: { $available_columns }
executor-invalid-table-qualifier = कॉलम '{ $column }' के लिए अमान्य टेबल क्वालिफायर '{ $qualifier }'। उपलब्ध टेबल: { $available_tables }
executor-column-already-exists = कॉलम '{ $name }' पहले से मौजूद है
executor-column-index-out-of-bounds = कॉलम इंडेक्स { $index } सीमा से बाहर है

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = इंडेक्स '{ $name }' नहीं मिला
executor-index-already-exists = इंडेक्स '{ $name }' पहले से मौजूद है
executor-invalid-index-definition = अमान्य इंडेक्स परिभाषा: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = ट्रिगर '{ $name }' नहीं मिला
executor-trigger-already-exists = ट्रिगर '{ $name }' पहले से मौजूद है

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = स्कीमा '{ $name }' नहीं मिला
executor-schema-already-exists = स्कीमा '{ $name }' पहले से मौजूद है
executor-schema-not-empty = स्कीमा '{ $name }' को हटाया नहीं जा सकता: स्कीमा खाली नहीं है

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = भूमिका '{ $name }' नहीं मिली
executor-permission-denied = अनुमति अस्वीकृत: भूमिका '{ $role }' के पास { $object } पर { $privilege } विशेषाधिकार नहीं है
executor-dependent-privileges-exist = निर्भर विशेषाधिकार मौजूद हैं: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = टाइप '{ $name }' नहीं मिला
executor-type-already-exists = टाइप '{ $name }' पहले से मौजूद है
executor-type-in-use = टाइप '{ $name }' को हटाया नहीं जा सकता: टाइप अभी भी उपयोग में है
executor-type-mismatch = टाइप मेल नहीं खाता: { $left } { $op } { $right }
executor-type-error = टाइप त्रुटि: { $message }
executor-cast-error = { $from_type } को { $to_type } में कास्ट नहीं किया जा सकता
executor-type-conversion-error = { $from } को { $to } में परिवर्तित नहीं किया जा सकता

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = शून्य से विभाजन
executor-invalid-where-clause = अमान्य WHERE क्लॉज: { $message }
executor-unsupported-expression = असमर्थित एक्सप्रेशन: { $message }
executor-unsupported-feature = असमर्थित फीचर: { $message }
executor-parse-error = पार्स त्रुटि: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = स्केलर सबक्वेरी ने { $actual } पंक्तियां लौटाईं, अपेक्षित { $expected }
executor-subquery-column-count-mismatch = सबक्वेरी ने { $actual } कॉलम लौटाए, अपेक्षित { $expected }
executor-column-count-mismatch = व्युत्पन्न कॉलम सूची में { $provided } कॉलम हैं लेकिन क्वेरी { $expected } कॉलम उत्पन्न करती है

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = कॉन्स्ट्रेंट उल्लंघन: { $message }
executor-multiple-primary-keys = एकाधिक PRIMARY KEY कॉन्स्ट्रेंट की अनुमति नहीं है
executor-cannot-drop-column = कॉलम हटाया नहीं जा सकता: { $message }
executor-constraint-not-found = कॉन्स्ट्रेंट '{ $constraint_name }' टेबल '{ $table_name }' में नहीं मिला

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = एक्सप्रेशन गहराई सीमा पार हो गई: { $depth } > { $max_depth } (स्टैक ओवरफ्लो रोकता है)
executor-query-timeout-exceeded = क्वेरी टाइमआउट पार हो गया: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = पंक्ति प्रसंस्करण सीमा पार हो गई: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = मेमोरी सीमा पार हो गई: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = वेरिएबल '{ $variable_name }' नहीं मिला
executor-variable-not-found-with-available = वेरिएबल '{ $variable_name }' नहीं मिला। उपलब्ध वेरिएबल: { $available_variables }
executor-label-not-found = लेबल '{ $name }' नहीं मिला

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = प्रोसीजरल SELECT INTO को ठीक { $expected } पंक्ति लौटानी चाहिए, { $actual } पंक्ति{ $plural } मिली
executor-select-into-column-count = प्रोसीजरल SELECT INTO कॉलम गणना मेल नहीं खाती: { $expected } वेरिएबल{ $expected_plural } लेकिन क्वेरी ने { $actual } कॉलम{ $actual_plural } लौटाए

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = प्रोसीजर '{ $procedure_name }' स्कीमा '{ $schema_name }' में नहीं मिला
executor-procedure-not-found-with-available = प्रोसीजर '{ $procedure_name }' स्कीमा '{ $schema_name }' में नहीं मिला
    .available = उपलब्ध प्रोसीजर: { $available_procedures }
executor-procedure-not-found-with-suggestion = प्रोसीजर '{ $procedure_name }' स्कीमा '{ $schema_name }' में नहीं मिला
    .available = उपलब्ध प्रोसीजर: { $available_procedures }
    .suggestion = क्या आपका मतलब '{ $suggestion }' था?

executor-function-not-found-simple = फ़ंक्शन '{ $function_name }' स्कीमा '{ $schema_name }' में नहीं मिला
executor-function-not-found-with-available = फ़ंक्शन '{ $function_name }' स्कीमा '{ $schema_name }' में नहीं मिला
    .available = उपलब्ध फ़ंक्शन: { $available_functions }
executor-function-not-found-with-suggestion = फ़ंक्शन '{ $function_name }' स्कीमा '{ $schema_name }' में नहीं मिला
    .available = उपलब्ध फ़ंक्शन: { $available_functions }
    .suggestion = क्या आपका मतलब '{ $suggestion }' था?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' को { $expected } पैरामीटर{ $expected_plural } ({ $parameter_signature }) अपेक्षित, { $actual } आर्गुमेंट{ $actual_plural } मिले
executor-parameter-type-mismatch = पैरामीटर '{ $parameter_name }' को { $expected_type } अपेक्षित, { $actual_type } '{ $actual_value }' मिला
executor-argument-count-mismatch = आर्गुमेंट गणना मेल नहीं खाती: अपेक्षित { $expected }, मिला { $actual }

executor-recursion-limit-exceeded = अधिकतम रिकर्शन गहराई ({ $max_depth }) पार हो गई: { $message }
executor-recursion-call-stack = कॉल स्टैक:
executor-function-must-return = फ़ंक्शन को एक मान लौटाना होगा
executor-invalid-control-flow = अमान्य नियंत्रण प्रवाह: { $message }
executor-invalid-function-body = अमान्य फ़ंक्शन बॉडी: { $message }
executor-function-read-only-violation = फ़ंक्शन रीड-ओनली उल्लंघन: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = { $value_type } मान से { $field } निकाला नहीं जा सकता

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Arrow एरे को { $expected_type } में डाउनकास्ट करने में विफल ({ $context })
executor-columnar-type-mismatch-binary = { $operation } के लिए असंगत टाइप: { $left_type } बनाम { $right_type }
executor-columnar-type-mismatch-unary = { $operation } के लिए असंगत टाइप: { $left_type }
executor-simd-operation-failed = SIMD { $operation } विफल: { $reason }
executor-columnar-column-not-found = कॉलम इंडेक्स { $column_index } सीमा से बाहर है (बैच में { $batch_columns } कॉलम हैं)
executor-columnar-column-not-found-by-name = कॉलम नहीं मिला: { $column_name }
executor-columnar-length-mismatch = { $context } में कॉलम लंबाई मेल नहीं खाती: अपेक्षित { $expected }, मिला { $actual }
executor-unsupported-array-type = { $operation } के लिए असमर्थित एरे टाइप: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } को { $expected } अपेक्षित, { $actual } मिला

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = कर्सर '{ $name }' पहले से मौजूद है
executor-cursor-not-found = कर्सर '{ $name }' नहीं मिला
executor-cursor-already-open = कर्सर '{ $name }' पहले से खुला है
executor-cursor-not-open = कर्सर '{ $name }' खुला नहीं है
executor-cursor-not-scrollable = कर्सर '{ $name }' स्क्रॉल करने योग्य नहीं है (SCROLL निर्दिष्ट नहीं)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = स्टोरेज त्रुटि: { $message }
executor-other = { $message }
