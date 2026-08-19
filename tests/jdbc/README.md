# JDBC Compatibility Tests

This directory contains tests for verifying JDBC driver compatibility with VibeSQL.

## Prerequisites

1. VibeSQL server running on localhost:5432
2. Java Development Kit (JDK) 11+ installed
3. PostgreSQL JDBC driver (see below for download)

## Setup

### Download PostgreSQL JDBC Driver

**Maven:**
```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.1</version>
</dependency>
```

**Gradle:**
```gradle
dependencies {
    implementation 'org.postgresql:postgresql:42.7.1'
}
```

**Manual Download:**
```bash
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

## Test Files

- `TestConnection.java` - Basic connection test
- `TestQueries.java` - SQL query tests
- `TestCRUD.java` - CRUD operation tests
- `TestPreparedStatement.java` - Prepared statement tests (when supported)
- `TestConnectionPool.java` - Connection pooling test (HikariCP)

## Running Tests

### Compile and Run (Manual)

```bash
# Download JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Compile
javac -cp postgresql-42.7.1.jar TestConnection.java
javac -cp postgresql-42.7.1.jar TestQueries.java

# Run
java -cp .:postgresql-42.7.1.jar TestConnection
java -cp .:postgresql-42.7.1.jar TestQueries
```

### Using Maven

```bash
mvn test
```

### Using Gradle

```bash
./gradlew test
```

## Expected Results

All tests should pass with VibeSQL server running. If tests fail:
1. Verify server is running: `lsof -i :5432`
2. Check JDBC driver is in classpath
3. Test with psql first: `psql -h localhost -p 5432 -c "SELECT 1"`

## Test Results

Document your test results:

| Test | Status | Notes |
|------|--------|-------|
| TestConnection.java | | |
| TestQueries.java | | |
| TestCRUD.java | | |
| Connection pooling | | |
| Spring Boot integration | | |

## Known Limitations

- ✅ Prepared statements supported (extended query protocol: Parse/Bind/Describe/Execute; parameters are substituted server-side as literals rather than natively typed)
- ✅ Simple queries work
- ✅ Transactions work
- ❌ SSL/TLS not yet supported

## Reporting Issues

If you find compatibility issues:
1. Note the JDBC driver version
2. Note the JDK version
3. Document the error message
4. Include connection code used
5. Report at: https://github.com/rjwalters/vibesql/issues
