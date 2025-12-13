# Documentation

## User Guides

### Getting Started
- [CLI Guide](CLI_GUIDE.md) - Command-line interface usage
- [Python Bindings](PYTHON_BINDINGS.md) - Python DB-API 2.0 integration
- [ODBC/JDBC Connectivity](ODBC_JDBC_CONNECTIVITY.md) - Database connectivity via PostgreSQL wire protocol

### Server & APIs
- [Server Configuration](server-config.md) - Server configuration reference
- [HTTP API](http-api.md) - REST and GraphQL endpoints
- [Transactions](transactions.md) - Transaction handling and durability hints

### Extended Features
- [Scheduled Functions](scheduled-functions.md) - Cron jobs and scheduled tasks
- [Vector Search](vector-search.md) - AI/ML embeddings and similarity search
- [File Storage](file-storage.md) - Blob storage with SQL integration

---

## Reference

- [Feature Status](reference/FEATURE_STATUS.md) - SQL feature implementation status
- [Comparisons](reference/COMPARISONS.md) - Comparison with other databases
- [Procedures & Functions](reference/PROCEDURES_FUNCTIONS.md) - Stored procedure reference
- [Trigger Status](reference/TRIGGER_IMPLEMENTATION_STATUS.md) - Trigger implementation details

---

## Project

- [Roadmap](ROADMAP.md) - Current status and future plans
- [History](HISTORY.md) - Development timeline
- [Contributing Translations](CONTRIBUTING_TRANSLATIONS.md) - i18n guide (19 languages)

---

## Development

### Core Development
- [Benchmarking Guide](development/BENCHMARKING.md) - **Authoritative** benchmarking documentation
- [Dogfooding Benchmarks](development/DOGFOODING_BENCHMARKS.md) - Internal performance tracking
- [MIRI Testing](development/MIRI.md) - Undefined behavior detection
- [Publishing Checklist](PUBLISHING_CHECKLIST.md) - Release process

### Performance
- [Performance Overview](performance/) - Performance documentation hub
- [CPU Profiling](performance/CPU_PROFILING.md) - Profiling tool selection decision tree
- [Optimization Guide](performance/OPTIMIZATION.md) - Performance optimization strategies
- [TPC-H Benchmarking](performance/TPC-H_BENCHMARKING.md) - TPC-H benchmark guide

### Testing
- [Testing Overview](testing/) - Testing documentation hub
- [Testing Strategy](testing/TESTING_STRATEGY.md) - Overall testing approach
- [SQL:1999 Conformance](testing/SQL1999_CONFORMANCE.md) - Standards compliance (100% Core)
- [SQLLogicTest](testing/sqllogictest/QUICKSTART.md) - SQLLogicTest suite usage

### Architecture
- [Columnar Architecture](architecture/COLUMNAR_ARCHITECTURE.md) - Columnar execution engine design
- [Architecture Decision Records](decisions/) - Key design decisions

---

## Internal

- [archive/](archive/) - Completed investigations and historical docs
- [lessons/](lessons/) - Development insights and lessons learned
- [templates/](templates/) - Document templates (ADR, architecture, implementation)
