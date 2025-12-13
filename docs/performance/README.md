# Performance Documentation

This directory contains performance-related documentation including benchmarking, optimization, and profiling guides.

## Quick Links

| Document | Purpose |
|----------|---------|
| [CPU_PROFILING.md](CPU_PROFILING.md) | Decision tree for choosing profiling tools |
| [OPTIMIZATION.md](OPTIMIZATION.md) | Optimization strategies and techniques |
| [PROFILING_GUIDE.md](PROFILING_GUIDE.md) | How to profile queries |
| [TPC-H_BENCHMARKING.md](TPC-H_BENCHMARKING.md) | TPC-H benchmark guide |

## Benchmarking

### Running Benchmarks

```bash
# Quick benchmark (CI mode)
make benchmark-quick

# Full benchmarks - VibeSQL only
make benchmark

# Full matrix - all engines
make benchmark-all
```

### Individual Suites

```bash
make benchmark-tpch     # TPC-H decision support
make benchmark-tpcds    # TPC-DS decision support
make benchmark-tpcc     # TPC-C OLTP transactions
make benchmark-sysbench # Sysbench micro-benchmarks
```

## Profiling

See [CPU_PROFILING.md](CPU_PROFILING.md) for a decision tree on choosing the right tool:
- **samply** (`make profile-tpch Q=X`) for CPU profiling / flame graphs
- **Environment variables** for optimizer decision logging

## Documents

### Optimization
- [OPTIMIZATION.md](OPTIMIZATION.md) - Comprehensive optimization guide (Phase 1-9)
- [OPTIMIZATION_ROADMAP.md](OPTIMIZATION_ROADMAP.md) - Performance improvement roadmap

### Benchmarking
- [BENCHMARK_STRATEGY.md](BENCHMARK_STRATEGY.md) - Benchmarking methodology
- [TPC-H_BENCHMARKING.md](TPC-H_BENCHMARKING.md) - TPC-H benchmark details
- [TPCDS_RESULTS.md](TPCDS_RESULTS.md) - TPC-DS benchmark results
- [TPCH_SCALING_VALIDATION.md](TPCH_SCALING_VALIDATION.md) - Scaling validation

### Profiling
- [CPU_PROFILING.md](CPU_PROFILING.md) - Profiling tool selection
- [PROFILING_GUIDE.md](PROFILING_GUIDE.md) - Profiling how-to
- [PROFILING_AUDIT.md](PROFILING_AUDIT.md) - Profiling methodology audit

### Analysis
- [MORSEL_SIZE_INVESTIGATION.md](MORSEL_SIZE_INVESTIGATION.md) - Morsel size research
- [PYO3_OPTIMIZATION_OPPORTUNITIES.md](PYO3_OPTIMIZATION_OPPORTUNITIES.md) - Python bindings optimization

## Related Documentation

- [Testing Strategy](../testing/TESTING_STRATEGY.md) - Overall testing approach
- [Lessons Learned](../lessons/LESSONS_LEARNED.md) - Development insights
- [Benchmarking Guide](../development/BENCHMARKING.md) - Authoritative benchmarking docs
