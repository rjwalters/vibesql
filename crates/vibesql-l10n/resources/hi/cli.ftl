# VibeSQL CLI Localization - Hindi (hi)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 पूर्ण अनुपालन डेटाबेस
cli-help-hint = मदद के लिए \help टाइप करें, बाहर निकलने के लिए \quit
cli-goodbye = अलविदा!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - SQL:1999 पूर्ण अनुपालन डेटाबेस

cli-long-about = VibeSQL कमांड-लाइन इंटरफेस

    उपयोग मोड:
      इंटरैक्टिव REPL:    vibesql (--database <FILE>)
      कमांड निष्पादित करें:     vibesql -c "SELECT * FROM users"
      फ़ाइल निष्पादित करें:        vibesql -f script.sql
      stdin से निष्पादित करें:  cat data.sql | vibesql
      टाइप जनरेट करें:      vibesql codegen --schema schema.sql --output types.ts

    इंटरैक्टिव REPL:
      जब -c, -f, या पाइप्ड इनपुट के बिना शुरू किया जाता है, VibeSQL एक इंटरैक्टिव
      REPL में प्रवेश करता है जिसमें readline समर्थन, कमांड इतिहास, और मेटा-कमांड जैसे:
        \d (table)  - टेबल का वर्णन करें या सभी टेबल सूचीबद्ध करें
        \dt         - टेबल सूचीबद्ध करें
        \f <format> - आउटपुट फ़ॉर्मेट सेट करें
        \copy       - CSV/JSON आयात/निर्यात करें
        \help       - सभी REPL कमांड दिखाएं

    उपकमांड:
      codegen           डेटाबेस स्कीमा से TypeScript टाइप जनरेट करें

    कॉन्फ़िगरेशन:
      सेटिंग्स ~/.vibesqlrc (TOML फ़ॉर्मेट) में कॉन्फ़िगर की जा सकती हैं।
      अनुभाग: display, database, history, query

    उदाहरण:
      # इन-मेमोरी डेटाबेस के साथ इंटरैक्टिव REPL शुरू करें
      vibesql

      # स्थायी डेटाबेस फ़ाइल का उपयोग करें
      vibesql --database mydata.db

      # एकल कमांड निष्पादित करें
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # SQL स्क्रिप्ट फ़ाइल चलाएं
      vibesql -f schema.sql -v

      # CSV से डेटा आयात करें
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # क्वेरी परिणाम JSON के रूप में निर्यात करें
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # स्कीमा फ़ाइल से TypeScript टाइप जनरेट करें
      vibesql codegen --schema schema.sql --output src/types.ts

      # चल रहे डेटाबेस से TypeScript टाइप जनरेट करें
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = डेटाबेस फ़ाइल पथ (यदि निर्दिष्ट नहीं है, तो इन-मेमोरी डेटाबेस का उपयोग करता है)
arg-file-help = फ़ाइल से SQL कमांड निष्पादित करें
arg-command-help = SQL कमांड सीधे निष्पादित करें और बाहर निकलें
arg-stdin-help = stdin से SQL कमांड पढ़ें (पाइप होने पर स्वचालित रूप से पता लगाया जाता है)
arg-verbose-help = फ़ाइल/stdin निष्पादन के दौरान विस्तृत आउटपुट दिखाएं
arg-format-help = क्वेरी परिणामों के लिए आउटपुट फ़ॉर्मेट
arg-lang-help = प्रदर्शन भाषा सेट करें (जैसे, en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = डेटाबेस स्कीमा से TypeScript टाइप जनरेट करें

codegen-long-about = VibeSQL डेटाबेस स्कीमा से TypeScript टाइप परिभाषाएं जनरेट करें।

    यह कमांड डेटाबेस में सभी टेबल के लिए TypeScript इंटरफेस बनाता है,
    रनटाइम टाइप चेकिंग और IDE समर्थन के लिए मेटाडेटा ऑब्जेक्ट्स के साथ।

    इनपुट स्रोत:
      --database <FILE>  मौजूदा डेटाबेस फ़ाइल से जनरेट करें
      --schema <FILE>    SQL स्कीमा फ़ाइल से जनरेट करें (CREATE TABLE स्टेटमेंट्स)

    आउटपुट:
      --output <FILE>    जनरेट किए गए टाइप इस फ़ाइल में लिखें (डिफ़ॉल्ट: types.ts)

    विकल्प:
      --camel-case       कॉलम नामों को camelCase में बदलें
      --no-metadata      tables मेटाडेटा ऑब्जेक्ट जनरेट करना छोड़ें

    उदाहरण:
      # डेटाबेस फ़ाइल से
      vibesql codegen --database mydata.db --output src/db/types.ts

      # SQL स्कीमा फ़ाइल से
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # camelCase प्रॉपर्टी नामों के साथ
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = CREATE TABLE स्टेटमेंट्स वाली SQL स्कीमा फ़ाइल
codegen-output-help = जनरेट किए गए TypeScript के लिए आउटपुट फ़ाइल पथ
codegen-camel-case-help = कॉलम नामों को camelCase में बदलें
codegen-no-metadata-help = टेबल मेटाडेटा ऑब्जेक्ट जनरेट करना छोड़ें

codegen-from-schema = स्कीमा फ़ाइल से TypeScript टाइप जनरेट हो रहे हैं: { $path }
codegen-from-database = डेटाबेस से TypeScript टाइप जनरेट हो रहे हैं: { $path }
codegen-written = TypeScript टाइप यहां लिखे गए: { $path }
codegen-error-no-source = --database या --schema में से कोई एक निर्दिष्ट होना चाहिए।
    उपयोग जानकारी के लिए 'vibesql codegen --help' का उपयोग करें।

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = मेटा-कमांड:
help-describe = \d (table)      - टेबल का वर्णन करें या सभी टेबल सूचीबद्ध करें
help-tables = \dt             - टेबल सूचीबद्ध करें
help-schemas = \ds             - स्कीमा सूचीबद्ध करें
help-indexes = \di             - इंडेक्स सूचीबद्ध करें
help-roles = \du             - भूमिकाएं/उपयोगकर्ता सूचीबद्ध करें
help-format = \f <format>     - आउटपुट फ़ॉर्मेट सेट करें (table, json, csv, markdown, html)
help-timing = \timing         - क्वेरी समय टॉगल करें
help-copy-to = \copy <table> TO <file>   - टेबल को CSV/JSON फ़ाइल में निर्यात करें
help-copy-from = \copy <table> FROM <file> - CSV फ़ाइल को टेबल में आयात करें
help-save = \save (file)    - डेटाबेस को SQL डंप फ़ाइल में सहेजें
help-errors = \errors         - हाल की त्रुटि इतिहास दिखाएं
help-help = \h, \help      - यह सहायता दिखाएं
help-quit = \q, \quit      - बाहर निकलें

help-sql-title = SQL आत्मनिरीक्षण:
help-show-tables = SHOW TABLES                  - सभी टेबल सूचीबद्ध करें
help-show-databases = SHOW DATABASES               - सभी स्कीमा/डेटाबेस सूचीबद्ध करें
help-show-columns = SHOW COLUMNS FROM <table>    - टेबल कॉलम दिखाएं
help-show-index = SHOW INDEX FROM <table>      - टेबल इंडेक्स दिखाएं
help-show-create = SHOW CREATE TABLE <table>    - CREATE TABLE स्टेटमेंट दिखाएं
help-describe-sql = DESCRIBE <table>             - SHOW COLUMNS के लिए उपनाम

help-examples-title = उदाहरण:
help-example-create = CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM users;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM users;
help-example-describe = DESCRIBE users;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy users TO '/tmp/users.csv'
help-example-copy-from = \copy users FROM '/tmp/users.csv'
help-example-copy-json = \copy users TO '/tmp/users.json'
help-example-errors = \errors

# =============================================================================
# Status Messages
# =============================================================================

format-changed = आउटपुट फ़ॉर्मेट सेट किया गया: { $format }
database-saved = डेटाबेस सहेजा गया: { $path }
no-database-file = त्रुटि: कोई डेटाबेस फ़ाइल निर्दिष्ट नहीं है। \save <filename> का उपयोग करें या --database फ्लैग के साथ शुरू करें

# =============================================================================
# Error Display
# =============================================================================

no-errors = इस सत्र में कोई त्रुटि नहीं।
recent-errors = हाल की त्रुटियां:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = स्क्रिप्ट में कोई SQL स्टेटमेंट नहीं मिला
script-executing = स्टेटमेंट { $current } / { $total } निष्पादित हो रहा है...
script-error = स्टेटमेंट { $index } निष्पादित करने में त्रुटि: { $error }
script-summary-title = === स्क्रिप्ट निष्पादन सारांश ===
script-total = कुल स्टेटमेंट: { $count }
script-successful = सफल: { $count }
script-failed = विफल: { $count }
script-failed-error = { $count } स्टेटमेंट विफल हुए

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } पंक्तियां सेट में ({ $time }s)
rows-count = { $count } पंक्तियां

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = चेतावनी: कॉन्फ़िग फ़ाइल लोड नहीं हो सकी: { $error }
warning-auto-save-failed = चेतावनी: डेटाबेस ऑटो-सेव विफल: { $error }
warning-save-on-exit-failed = चेतावनी: बाहर निकलते समय डेटाबेस सहेजना विफल: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = फ़ाइल '{ $path }' पढ़ने में विफल: { $error }
stdin-read-error = stdin से पढ़ने में विफल: { $error }
database-load-error = डेटाबेस लोड करने में विफल: { $error }
