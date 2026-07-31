# scripts/

Helper scripts for testing, benchmarking, and result processing. This README
covers only the **Python test suite** (`test_*.py`); the individual scripts are
documented in their own headers.

## The `test_*.py` suite

Pytest collects these files (configured in the root `pyproject.toml` under
`[tool.pytest.ini_options]`, added in #6321):

| File | Covers | Needs `vibesql` bindings? |
|------|--------|---------------------------|
| `test_generate_punchlist.py` | Storing SQLLogicTest results in VibeSQL, SQL dump export/import | **Yes** |
| `test_process_test_results.py` | Test-result processing helpers | No |
| `test_tcl_parser.py` | Static TCL test-file parser | No |
| `test_tcl_triage.py` | TCL failure triage helpers | No |

> **Naming caveat:** `test_results_config.py` matches the `test_*.py` glob but
> is a shared **config module** imported by other scripts, not a test file. It
> is explicitly excluded from collection in `pyproject.toml` — keep it that way.

> **Known bug:** two `test_generate_punchlist.py` tests are marked
> `@unittest.expectedFailure` pending #6359 (the bindings' statement cache
> replays the first execution's parameters for repeated parameterized SQL).
> They will report *unexpected success* once #6359 is fixed — remove the
> decorators then.

### Running the suite

The recommended entry point builds the Python bindings **from this checkout**,
installs the wheel, and then runs pytest:

```bash
make test-scripts
```

Notes:

- The wheel is installed into whatever `python3` resolves to. If your system
  Python is externally managed (PEP 668, e.g. Homebrew), activate a
  virtualenv first.
- Alternative for an activated virtualenv:
  `maturin develop --release -m crates/vibesql-python-bindings/Cargo.toml`,
  then `pytest`.

A plain `pytest` from the repo root also works, but only exercises the
bindings-dependent tests if a **current** `vibesql` wheel is installed.

### The bindings guard (`conftest.py`)

`import vibesql` resolves to whatever wheel is installed — historically this
silently tested a weeks-stale wheel, producing failures that looked like severe
engine bugs (spurious `UNIQUE constraint failed`; issue #6323). `conftest.py`
guards against both failure modes:

- **Stale wheel** (installed bindings version — `vibesql.__version__` when
  defined, else the distribution metadata — != workspace version in the root
  `Cargo.toml`): the whole pytest session **aborts immediately**, naming the
  stale module path and the rebuild command.
- **No wheel installed**: the bindings-dependent tests **skip** with a message
  naming the fix. Set `VIBESQL_REQUIRE_BINDINGS=1` (done automatically by
  `make test-scripts` and CI) to turn that skip into a hard failure, so a green
  run proves the bindings were actually exercised.

The version check is a cheap tripwire, not proof of freshness — only rebuilding
(`make test-scripts`) guarantees the repo build is under test.

CI runs this suite in the `python-scripts` job of
`.github/workflows/ci-extended.yml` with `VIBESQL_REQUIRE_BINDINGS=1`.
