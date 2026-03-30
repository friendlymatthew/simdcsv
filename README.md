# arrow-csv2

This project researches how far we can push CSV-to-Arrow performance, from single threaded decoding to parallel ingestion from object store, and offers a performant parallel file opener for Datafusion and vectorized CSV decoder for Arrow.

# Background

To read a CSV file in parallel, you split it into byte ranges and assign each range to a thread. Since a split point almost certainly lands mid-row, each thread seeks forward to the next newline to find a record boundary. This is the approach used by state of the art databases like DuckDB's [parallel CSV file reader](https://github.com/duckdb/duckdb/pull/6977).

However, this assumes every newline is a record boundary. But [RFC-4180](https://www.rfc-editor.org/rfc/rfc4180.html) allows newlines inside quoted fields (e.g. `a,"b\nc",d`). A split point inside a quoted field causes threads to treat a quoted newline as a record boundary, producing silently wrong results like dropped rows ([#13787](https://github.com/duckdb/duckdb/issues/13787), [#7578](https://github.com/duckdb/duckdb/issues/7578), [#13047](https://github.com/duckdb/duckdb/issues/13047)) or incorrect field values ([#9036](https://github.com/duckdb/duckdb/pull/9036)).

This project solves it by framing CSV quote tracking as a monoid, which makes it safe to parallelize. The only state needed to determine whether a newline is a record boundary is whether the parser is currently inside a quoted field or not, which is just the parity of the quote count before that position. _Quote parity forms a monoid under XOR with identity false, which means partitions can be classified independently and combined in any order_. Though, the combination step is sequential since each partition's true starting state depends on the accumulated parity of all preceding partitions. In practice, this is trivially cheap and could be restructured as a parallel prefix scan if needed, since the op is associative.

Once the correct newline bitsets are selected, the resolver finds the first record boundary newline in each partition to determine record aligned byte ranges. Each partition then parses its range independently, producing Arrow record batches in parallel.

# Features

This project hooks into the Datafusion pipeline at the `FileSource` level. It offers a `ParallelCsvSource` and `ParallelCsvOpener` which implements Datafusion's `FileSource` and `FileOpener` traits.

The file opener uses a custom CSV decoder that follows the existing `arrow-csv`'s Decoder API.

# Status

On a M4 Macbook, `arrow-csv2` reads the full Clickbench `hits.csv` dataset (82gb, uncompressed) in **21.861s (3.46gb/s)** with the default settings (64MB partitions, concurrency 16).

Benchmarks on a 100MB ClickBench slice (M4 MacBook):

**DISCLAIMERS**

- For correct RFC 4180 parsing, DataFusion falls back to single-threaded (204ms). arrow-csv2 achieves 36ms at 16 partitions while maintaining correctness, a **5.7x speedup**.
- DuckDB numbers include Arrow IPC conversion overhead. (I'm also generally dubious about these numbers)
- arrow-csv2 is RFC 4180 compliant at all partition counts.

| Partitions/Threads | arrow-csv2 | DataFusion    | DuckDB        |
| ------------------ | ---------- | ------------- | ------------- |
| 1                  | 199ms      | 205ms (1.03x) | 600ms (3.02x) |
| 2                  | 107ms      | 122ms (1.14x) | 458ms (4.28x) |
| 4                  | 61ms       | 74ms (1.21x)  | 387ms (6.34x) |
| 8                  | 38ms       | 49ms (1.29x)  | 338ms (8.89x) |
| 12                 | 37ms       | 50ms (1.35x)  | 330ms (8.92x) |
| 16                 | 36ms       | 54ms (1.50x)  | 324ms (9.00x) |

# Usage

At the moment, `arrow-csv2` makes use of NEON intrinsics (sorry).

```sh
# run the full 82gb uncompressed Clickbench dataset
cargo r --bin parse_clickbench --release

# run benchmarks (uses a 100MB slice of the Clickbench dataset)
./download_clickbench.sh
cargo r --bin slice_clickbench
cargo bench

# tests (includes roundtripping with arrow-csv!)
cargo t
```

# Reading

https://branchfree.org/2019/03/06/code-fragment-finding-quote-pairs-with-carry-less-multiply-pclmulqdq/<br>
https://www.rfc-editor.org/rfc/rfc4180.html<br>
https://arxiv.org/pdf/1902.08318<br>
