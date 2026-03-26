# arrow-csv2

Vectorized CSV parsing for Apache Arrow.

This project aims to be a faster drop-in replacement for `arrow-csv`, the csv-to-arrow decoder in the `arrow-rs` ecosystem. The parser employs techniques highlighted in the `simdjson` paper like vectorized classification and prefix xor.

A secondary goal is to demonstrate a performant parallel object store reader that uses speculative quote-state reconciliation to enable byte-range splitting for files with quoted newlines, something Datafusion currently disables.

# Status

Currently, `arrow-csv2` decodes **3.8x faster** than `arrow-csv` (44ms vs 168ms). This is measured on a 100MB slice (~130K rows) of the [ClickBench](https://github.com/ClickHouse/ClickBench) `hits.csv` dataset.

```sh
# run benchmarks
./download_clickbench.sh
cargo r --bin slice_clickbench
cargo bench
```

The goal is not full feature parity with `arrow-csv`, but a proof of concept that explores how far we can push CSV-to-Arrow performance, from single threaded decoding to parallel ingestion from object store. As such, the parser currently targets ARM (Neon) and does not implement the full `arrow-csv` configuration surface (though it covers the interesting ones).

# Reading

https://branchfree.org/2019/03/06/code-fragment-finding-quote-pairs-with-carry-less-multiply-pclmulqdq/<br>
https://www.rfc-editor.org/rfc/rfc4180.html<br>
https://arxiv.org/pdf/1902.08318<br>
