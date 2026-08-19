# ODBC Compatibility Tests

This directory contains tests for verifying ODBC driver compatibility with VibeSQL.

## Prerequisites

1. VibeSQL server running on localhost:5432
2. PostgreSQL ODBC driver (psqlODBC) installed
3. ODBC configuration completed (see `/docs/ODBC_JDBC_CONNECTIVITY.md`)

## Test Scripts

### Basic Connectivity Tests

- `test_connection.sh` - Test basic connection to VibeSQL
- `test_queries.py` - Test various SQL queries via ODBC (Python)
- `test_crud.py` - Test CRUD operations via ODBC (Python)

### Application Integration Tests

- `test_excel.md` - Manual testing guide for Excel integration
- `test_powerbi.md` - Manual testing guide for Power BI integration
- `test_tableau.md` - Manual testing guide for Tableau integration

## Running Tests

### Shell Script Tests

```bash
# Make scripts executable
chmod +x *.sh

# Run connection test
./test_connection.sh
```

### Python Tests

```bash
# Install dependencies
pip install pyodbc

# Run tests
python test_queries.py
python test_crud.py
```

## Expected Results

All tests should pass with VibeSQL server running. If tests fail:
1. Verify server is running: `lsof -i :5432`
2. Check ODBC configuration: `odbcinst -q -d`
3. Test with psql first: `psql -h localhost -p 5432 -c "SELECT 1"`

## Test Results

Document your test results:

| Test | Status | Notes |
|------|--------|-------|
| test_connection.sh | | |
| test_queries.py | | |
| test_crud.py | | |
| Excel integration | | |
| Power BI integration | | |

## Reporting Issues

If you find compatibility issues:
1. Note the specific ODBC driver version
2. Document the error message
3. Include connection settings used
4. Report at: https://github.com/rjwalters/vibesql/issues
