# arrow-playground

To evaluate performance characteristics, I implemented the query "SELECT a + b FROM t WHERE c" using both Arrow and
traditional row-based data structures for comparative analysis.
It aims to uncover the underlying reasons for the superior performance of arrow-based query engines compared to
traditional row-oriented systems.

# Benchmark

The benchmark shows that arrow's implementation is 5x faster than row-oriented.

```bash
cargo bench
```
