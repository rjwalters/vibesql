# MIRI Undefined Behavior Detection

This document explains how VibeSQL uses MIRI to validate unsafe code for undefined behavior.

## What is MIRI?

**MIRI** (MIR Interpreter) is Rust's official tool for detecting undefined behavior in unsafe code. It runs your code in an interpreter that tracks every memory operation, catching bugs that tests might miss.

## Why MIRI?

VibeSQL uses unsafe code in performance-critical paths:
- `Row::get_*_unchecked()` methods bypass enum tag checks
- Monomorphic execution paths eliminate runtime type checks

While we have comprehensive tests and debug assertions, MIRI provides an additional safety layer by detecting:

- ✅ Use-after-free
- ✅ Out-of-bounds access
- ✅ Data races
- ✅ Invalid enum discriminants
- ✅ Unaligned reads
- ✅ Violations of aliasing rules (Stacked Borrows)

## Running MIRI Locally

### Setup

```bash
# Install MIRI
rustup +nightly component add miri

# Setup MIRI environment
cargo +nightly miri setup
```

### Run MIRI Tests

```bash
# Test Row unsafe methods
cargo +nightly miri test --package vibesql-storage row

# Test executor (slower)
cargo +nightly miri test --package vibesql-executor --lib
```

**Note**: MIRI is ~100x slower than normal tests. Row tests take ~2-5 minutes.

## CI Integration

MIRI runs automatically on every PR that modifies:
- `crates/vibesql-storage/src/row.rs`
- `crates/vibesql-executor/src/**`
- `.github/workflows/miri.yml`

See: `.github/workflows/miri.yml`

The CI job will **fail** if MIRI detects any undefined behavior.

## Interpreting MIRI Output

### Success (No UB)

```
test row::tests::safety_float_unchecked ... ok
test row::tests::safety_int_unchecked ... ok
test result: ok. 15 passed; 0 failed
```

### Failure (UB Detected)

```
error: Undefined Behavior: dereferencing pointer failed:
       null pointer is a dangling pointer (it has no provenance)
  --> src/row.rs:75:13
   |
75 |             *ptr
   |             ^^^^ dereferencing pointer failed
```

**Actions**:
1. Analyze the error message (shows exact line of UB)
2. Fix the unsafe code
3. Re-run MIRI locally
4. Push fix

## Known MIRI Limitations

### False Positives

MIRI's **Stacked Borrows** model may flag safe code as UB. If you encounter this:

1. Verify it's actually safe with careful reasoning
2. Check if it's a known MIRI limitation
3. Consider disabling the check:

```rust
#[cfg(not(miri))]
fn optimized_version() { /* fast but complex */ }

#[cfg(miri)]
fn simple_version() { /* slower but clearly safe */ }
```

### Performance

MIRI is very slow (~100x overhead). For large test suites:
- Run selectively in CI (only Row tests by default)
- Use `--test-threads=1` to reduce memory usage
- Skip integration tests under MIRI

### Platform-Specific UB

MIRI simulates x86_64 by default. Some UB may be platform-specific. Consider:
- Running on different architectures in CI
- Using LLVM sanitizers (AddressSanitizer, MemorySanitizer) as cross-check

## Best Practices

### When Writing Unsafe Code

1. **Document safety invariants**:
   ```rust
   // SAFETY: Caller must ensure index < values.len()
   unsafe fn get_unchecked(&self, index: usize) -> &Value
   ```

2. **Add debug assertions**:
   ```rust
   #[inline(always)]
   pub unsafe fn get_f64_unchecked(&self, index: usize) -> f64 {
       debug_assert!(matches!(self.values[index], SqlValue::Float(_)));
       // ...
   }
   ```

3. **Write safety tests**:
   ```rust
   #[test]
   fn test_unchecked_accessor_safety() {
       // Test correct usage
       // MIRI will catch any UB
   }
   ```

4. **Run MIRI locally** before submitting PR:
   ```bash
   cargo +nightly miri test --package vibesql-storage row
   ```

### When MIRI Fails

1. **Don't ignore it** - MIRI catches real bugs
2. **Analyze carefully** - understand the root cause
3. **Fix properly** - don't just disable checks
4. **Re-test** - ensure fix doesn't break functionality

## Resources

- [MIRI Repository](https://github.com/rust-lang/miri)
- [The Rustonomicon - Undefined Behavior](https://doc.rust-lang.org/nomicon/what-unsafe-does.html)
- [Stacked Borrows](https://github.com/rust-lang/unsafe-code-guidelines/blob/master/wip/stacked-borrows.md)
- [Rust UB Sanitizers](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html)

## Related Issues

- #2221 - Monomorphic Execution (introduced unsafe code)
- #2229 - Add MIRI validation (this feature)
- #2220 - Performance Optimization Epic
