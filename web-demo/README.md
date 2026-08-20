# VibeSQL Web Studio

Interactive SQL database demo running entirely in your browser using WebAssembly.

**[🚀 Live Demo](https://vibesql.org/)**

---

## Features

- **Zero Setup** - Start querying immediately with pre-loaded sample data
- **Monaco Editor** - Professional SQL editor with syntax highlighting and IntelliSense
- **WASM-Powered** - Rust database engine compiled to WebAssembly
- **Persistent Storage** - Data persists across browser sessions using OPFS (Origin Private File System)
- **SQL Comment Support** - Use `--` for inline documentation
- **Real-time Results** - See query results instantly with formatted tables
- **Export Options** - Copy to clipboard or download as CSV
- **Dark Mode** - Beautiful Tailwind CSS interface with theme toggle
- **Keyboard Shortcuts** - Ctrl/Cmd+Enter to execute, Ctrl/Cmd+/ to toggle comments

---

## Pre-loaded Sample Data

The demo includes a sample `employees` table with 6 records:

| id  | name           | department   | salary |
| --- | -------------- | ------------ | ------ |
| 1   | Alice Johnson  | Engineering  | 95000  |
| 2   | Bob Smith      | Engineering  | 87000  |
| 3   | Carol White    | Sales        | 72000  |
| 4   | David Brown    | Sales        | 68000  |
| 5   | Eve Martinez   | HR           | 65000  |
| 6   | Frank Wilson   | Engineering  | 92000  |

---

## Example Queries

### Basic SELECT

```sql
-- See all employees
SELECT * FROM employees;

-- Select specific columns
SELECT name, department FROM employees;
```

### Filtering with WHERE

```sql
-- Find engineering employees
SELECT name, salary FROM employees WHERE department = 'Engineering';

-- Find high earners
SELECT name, department, salary FROM employees WHERE salary > 80000;
```

### Aggregation

```sql
-- Count employees per department
SELECT department, COUNT(*) as count FROM employees GROUP BY department;

-- Average salary by department
SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department;
```

### Sorting

```sql
-- Highest paid employees first
SELECT name, salary FROM employees ORDER BY salary DESC;

-- Alphabetical by department, then name
SELECT * FROM employees ORDER BY department, name;
```

---

## Persistent Storage with OPFS

VibeSQL Web Demo uses the **Origin Private File System (OPFS)** API to persist database data across browser sessions. This means your tables, data, and indexes remain intact even after closing and reopening your browser.

### Browser Compatibility

| Browser  | OPFS Support | Minimum Version |
| -------- | ------------ | --------------- |
| Chrome   | ✅ Yes       | 86+             |
| Edge     | ✅ Yes       | 86+             |
| Firefox  | ✅ Yes       | 111+            |
| Safari   | ✅ Yes       | 15.2+           |

### How It Works

- **Automatic Persistence**: When you create tables or insert data, it's automatically saved to OPFS
- **Browser-Specific**: Each origin (domain) has its own isolated storage
- **Secure**: Data is private to your browser and cannot be accessed by other origins
- **Storage Status**: Check the "Storage" indicator next to the SQL Editor to see if OPFS is active

### Persistence Example

```sql
-- Create a table (persists across sessions)
CREATE TABLE my_notes (id INTEGER, note TEXT);

-- Insert data (persists across sessions)
INSERT INTO my_notes VALUES (1, 'Remember to buy milk');

-- Close browser and reopen demo

-- Your data is still here!
SELECT * FROM my_notes;
```

### Fallback Behavior

If OPFS is not supported in your browser, the demo automatically falls back to in-memory storage. The storage status indicator will show "Memory (Temporary)" in this case.

### Clearing Stored Data

To clear all persistent data:

1. Open browser DevTools (F12)
2. Go to Application/Storage tab
3. Find "Origin Private File System" or "File System"
4. Delete the vibesql data

Or clear all site data for the demo origin through browser settings.

---

## Development

### Prerequisites

- Node.js 20+
- pnpm 9+
- Rust toolchain with `wasm32-unknown-unknown` target
- wasm-pack 0.13+
- **macOS only:** Homebrew LLVM (`brew install llvm`)

### Quick Start (Local Development)

```bash
# 1. One-time setup: Install Rust wasm target and wasm-pack
rustup target add wasm32-unknown-unknown
cargo install wasm-pack

# macOS only: Install LLVM (required for wasm32 support)
brew install llvm

# 2. Build the WASM module (from repository root)
./scripts/build-wasm.sh

# 3. Install web dependencies and start dev server
cd web-demo
pnpm install
pnpm dev
```

The dev server will start at **http://localhost:5173/vibesql/**

### Building WASM Module

The `./scripts/build-wasm.sh` script handles platform-specific configuration automatically:

```bash
# Release build (default, optimized for size)
./scripts/build-wasm.sh

# Development build (faster compile, larger output)
./scripts/build-wasm.sh --dev
```

**What the script does:**
- Detects macOS and automatically uses Homebrew LLVM
- Cleans old build artifacts
- Builds with size-optimized settings (opt-level=z, LTO, stripped)
- Outputs to `web-demo/public/pkg/`

> **Note:** On macOS, the script requires Homebrew LLVM because the default Xcode clang doesn't support the `wasm32-unknown-unknown` target.

### Development Server

```bash
cd web-demo
pnpm dev     # Start at http://localhost:5173/vibesql/
```

### Available Scripts

```bash
pnpm dev          # Start development server
pnpm build        # Build for production
pnpm preview      # Preview production build
pnpm test         # Run tests
pnpm test:ui      # Run tests with UI
pnpm test:coverage # Run tests with coverage
pnpm lint         # Lint code
pnpm lint:fix     # Lint and fix
pnpm format       # Format code
pnpm format:check # Check formatting
```

---

## Architecture

### Tech Stack

- **Frontend**: TypeScript, Vite, Tailwind CSS
- **Editor**: Monaco Editor (VS Code engine)
- **Database**: Rust compiled to WebAssembly via wasm-pack
- **Testing**: Vitest
- **CI/CD**: GitHub Actions

### Project Structure

```
web-demo/
├── src/
│   ├── app/           # Demo app modules (database/editor managers, init, query execution)
│   ├── components/    # UI components (Results, HelpModal)
│   ├── data/          # Example queries, sample databases, compliance data
│   ├── db/            # WASM database interface
│   ├── editor/        # SQL validation
│   ├── examples/      # Interactive example usage
│   ├── i18n/          # Localization (Fluent resources)
│   ├── showcase/      # Feature showcase
│   ├── styles/        # Tailwind CSS
│   ├── utils/         # Shared utilities (formatting, benchmark stats)
│   ├── workers/       # Web workers (Monaco editor)
│   ├── homepage.ts    # Homepage entry point (index.html)
│   ├── main.ts        # Demo app entry point (demo.html)
│   ├── benchmarks.ts  # Benchmarks page entry point (benchmarks.html)
│   ├── conformance.ts # Conformance page entry point (conformance.html)
│   ├── trends.ts      # Trends page entry point (trends.html)
│   └── theme.ts       # Dark/light mode
├── public/
│   ├── pkg/           # WASM bindings (generated)
│   ├── badges/        # Badge JSON endpoints
│   ├── benchmarks/    # Exported benchmark JSON
│   ├── conformance/   # Conformance report data
│   └── data/          # Dashboard JSON
├── examples/          # Sample SQL files
├── index.html         # Homepage (plus demo.html, benchmarks.html, conformance.html, trends.html)
└── vite.config.ts     # Build configuration
```

### Key Components

#### Database Interface (`src/db/types.ts`)

```typescript
interface Database {
  execute(sql: string): ExecuteResult // DDL/DML
  query(sql: string): QueryResult // SELECT
  list_tables(): string[]
  describe_table(table: string): TableSchema
}
```

#### Results Component (`src/components/Results.ts`)

Displays query results with:

- Formatted tables for SELECT queries
- Success messages for DDL/DML operations
- Error display with syntax highlighting
- Export to CSV or clipboard

---

## Deployment

### Manual Deployment (Cloudflare Pages)

The web demo is deployed manually — there is no automatic deploy workflow. From
the `main` branch:

1. Build the WASM module using Rust + wasm-pack
2. Build the Vite app with `pnpm build`
3. Publish with `wrangler deploy` (config: `wrangler.toml`)

**Live URL:** https://vibesql.org/

### Updating Benchmark Data

Benchmark data is stored in `web-demo/public/benchmarks/` and committed to the repository. To update:

```bash
# Export latest benchmark data from dogfooded VibeSQL database
make website

# Or run the script directly with options:
python3 ./scripts/export_website_data.py --verbose

# Verify the exported files
ls -la web-demo/public/benchmarks/

# Commit the updated data
git add web-demo/public/benchmarks/ web-demo/public/data/
git commit -m "chore(web): Update benchmark data"
```

The export script reads from `~/.vibesql/test_results/benchmark_results.vbsql` (our dogfooded database) and generates:

| File | Description |
|------|-------------|
| `benchmark_results.json` | TPC-H comparison (VibeSQL, SQLite, DuckDB) |
| `tpcds_results.json` | TPC-DS decision support queries |
| `tpcc_results.json` | TPC-C OLTP benchmark |
| `sysbench_results.json` | Sysbench microbenchmarks |
| `trends_results.json` | Historical performance trends |
| `data/dashboard.json` | Summary dashboard for web UI |

### Local Build & Preview (Production Build)

Test the production build locally before deploying:

```bash
# From repository root
./scripts/build-wasm.sh          # Build WASM module

# Build and preview
cd web-demo
pnpm build                       # Creates optimized dist/ folder
pnpm preview                     # Serves at http://localhost:4173/vibesql/
```

---

## Troubleshooting

### WASM build fails on macOS

**Error:** `unable to create target: 'No available targets are compatible with triple "wasm32-unknown-unknown"'`

**Cause:** The default macOS clang (from Xcode) doesn't support WebAssembly targets.

**Solution:** Install Homebrew LLVM and use the build script:
```bash
# Install LLVM (one-time setup)
brew install llvm

# Use the build script (auto-detects macOS and uses LLVM)
./scripts/build-wasm.sh
```

The `build-wasm.sh` script automatically detects macOS and configures the correct LLVM paths.

### WASM module not loading

If you see "Use query() method for SELECT statements" error:

1. Verify WASM files exist:
   ```bash
   ls web-demo/public/pkg/
   # Should contain: vibesql_wasm.js, vibesql_wasm_bg.wasm, *.d.ts files
   ```

2. Rebuild WASM module (see "Building WASM Module" section)

3. Clear browser cache and reload

### TypeScript errors about missing WASM module

**Error:** `Cannot find module '../../public/pkg/vibesql_wasm.js'`

**Cause:** WASM module hasn't been built yet.

**Solution:** Build the WASM module first (see "Building WASM Module" section).

### TypeScript errors

```bash
pnpm exec tsc --noEmit  # Type check without building
```

### Formatting issues

```bash
pnpm format  # Auto-fix formatting
```

---

## Features in Development

- [ ] Multiple example databases (Northwind, etc.)
- [ ] SQL:1999 feature showcase
- [ ] Query history
- [ ] Save/load queries
- [ ] Schema explorer sidebar
- [ ] Query performance metrics

---

## Contributing

See the main [README.md](../README.md) for contribution guidelines.

For web demo specific improvements:

1. UI/UX enhancements
2. Additional example queries
3. Better error messages
4. Performance optimizations
5. Accessibility improvements

---

## License

MIT License - See [LICENSE-MIT](../LICENSE-MIT) for details.
