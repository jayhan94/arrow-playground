# arrow-playground

To evaluate performance characteristics, I implemented the query "SELECT a + b FROM t WHERE c" using both Arrow and
traditional row-based data structures for comparative analysis.
It aims to uncover the underlying reasons for the superior performance of arrow-based query engines compared to
traditional row-oriented systems.

# Benchmark
I use my own macbook m1 pro to do this benchmark whose width of its simd register should be 128.
The benchmark shows that arrow's implementation is 25x faster than row-oriented implementation and 5x faster than compiled execution.

```bash
cargo bench
```
