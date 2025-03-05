# arrow-playground

To evaluate performance characteristics, I implemented the query "SELECT a + b FROM t WHERE c" using both Arrow and
traditional row-based data structures for comparative analysis.
It aims to uncover the underlying reasons for the superior performance of arrow-based query engines compared to
traditional row-oriented systems.

# Benchmark

The benchmark of my mac-m1-pro laptop shows that arrow's implementation is 25x faster than row-oriented implementation and 5x faster than compiled execution.

```bash
cargo bench
```
