# NIST MemSQL Web Studio

Interactive SQL database demo running entirely in your browser using WebAssembly.

**[🚀 Live Demo](https://rjwalters.github.io/nistmemsql/)**

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
- LLVM with wasm32 support (for local builds on macOS)

### Setup

```bash
# Install Rust wasm target
rustup target add wasm32-unknown-unknown

# Install wasm-pack
cargo install wasm-pack

# Install dependencies
cd web-demo
pnpm install
```

### Building WASM Module

**On Linux/CI** (uses system clang):
```bash
wasm-pack build --target web --out-dir web-demo/public/pkg crates/vibesql-wasm-bindings --release
```

**On macOS with Homebrew LLVM** (required for wasm32 support):
```bash
# Install LLVM if not already installed
brew install llvm

# Build with LLVM's clang (has wasm32 target support)
CC="/opt/homebrew/opt/llvm/bin/clang" \
AR="/opt/homebrew/opt/llvm/bin/llvm-ar" \
wasm-pack build --target web --out-dir web-demo/public/pkg crates/vibesql-wasm-bindings --release
```

> **Note:** The default macOS clang (Xcode) doesn't support `wasm32-unknown-unknown`. You must use Homebrew's LLVM.

### Development Server

```bash
cd web-demo
pnpm dev
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
│   ├── components/     # UI components (Results, HelpModal)
│   ├── db/            # WASM database interface
│   ├── editor/        # SQL validation
│   ├── showcase/      # Feature showcase
│   ├── styles/        # Tailwind CSS
│   ├── main.ts        # Application entry point
│   └── theme.ts       # Dark/light mode
├── public/
│   ├── pkg/           # WASM bindings (generated)
│   └── examples/      # Sample SQL files
├── index.html         # HTML entry point
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

### Automatic Deployment (CI)

The web demo is automatically deployed to GitHub Pages on push to `main`:

1. Builds WASM module using Rust + wasm-pack
2. Builds Vite app with `pnpm build`
3. Deploys to GitHub Pages

**Live URL:** https://rjwalters.github.io/vibesql/

### Updating Benchmark Data

Benchmark data is stored in `web-demo/public/benchmarks/` and committed to the repository. To update:

```bash
# Export latest benchmark data from dogfooded VibeSQL database
./scripts/export_benchmark_json.py --all --verbose

# Verify the exported files
ls -la web-demo/public/benchmarks/

# Commit the updated data
git add web-demo/public/benchmarks/
git commit -m "chore(bench): Update benchmark data"
```

The export script reads from `~/.vibesql/test_results/benchmark_results.vbsql` (our dogfooded database) and generates:

| File | Description |
|------|-------------|
| `benchmark_results.json` | TPC-H comparison (vibesql vs sqlite) |
| `tpcc_results.json` | TPC-C OLTP benchmark |
| `sysbench_results.json` | Sysbench microbenchmarks |
| `tpcds_results.json` | TPC-DS decision support queries |
| `tpch_vibesql_only.json` | TPC-H vibesql-only timings |
| `footprint_results.json` | Memory footprint measurements |

### Local Build & Preview

```bash
# Build WASM (see "Building WASM Module" section above)

# Build the website
cd web-demo
pnpm build

# Preview locally
pnpm preview
```

### Manual Deployment

You can trigger a manual deployment from GitHub Actions:
1. Go to Actions → "Deploy to GitHub Pages"
2. Click "Run workflow"
3. Optionally provide a reason for the deployment

---

## Troubleshooting

### WASM build fails on macOS

**Error:** `unable to create target: 'No available targets are compatible with triple "wasm32-unknown-unknown"'`

**Cause:** The default macOS clang (from Xcode) doesn't support WebAssembly targets.

**Solution:** Use Homebrew's LLVM which includes wasm32 support:
```bash
brew install llvm

CC="/opt/homebrew/opt/llvm/bin/clang" \
AR="/opt/homebrew/opt/llvm/bin/llvm-ar" \
wasm-pack build --target web --out-dir web-demo/public/pkg crates/vibesql-wasm-bindings --release
```

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

MIT License - See [LICENSE](../LICENSE) for details.
