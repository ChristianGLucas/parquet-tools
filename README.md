# christiangeorgelucas/parquet-tools

Composable [Axiom](https://axiom.co) nodes for deterministic, offline inspection and
bounded transformation of Apache Parquet and Apache Arrow IPC columnar files, wrapping
[pyarrow](https://arrow.apache.org/docs/python/) (Apache-2.0) — the reference Python
binding to the Arrow C++ library, which also owns the canonical Parquet reader/writer.

Built for the Axiom marketplace. Every node is a pure bytes-in/bytes-or-struct-out
transform — no filesystem, network, state, or secrets.

## Nodes

- **ReadSchema** — columns, Arrow logical types, nullability, Parquet field ids, and
  schema-level key/value metadata.
- **ReadFileMetadata** — file- and row-group-level metadata: row/column counts, format
  version, `created_by`, footer size, and a per-row-group summary.
- **ReadColumnStatistics** — per-column null counts, min/max, distinct counts,
  compression, and encodings (from Parquet's footer where available; computed by one
  pass over decoded data for Arrow IPC).
- **ValidateFile** — a cheap structural well-formedness check.
- **ConvertFormat** — convert a whole small file between Parquet, Arrow IPC, CSV, and
  JSON.
- **Project** — select/project a bounded subset of columns and rows into any of those
  formats.

## Bounds

Every input is capped at 640 KiB and every output at 640 KiB (comfortably under the
platform's deployed-invocation ingress limit). `ConvertFormat` rejects an over-large
source by row count before reading it, and represents the whole file — if it would not
fit the output cap it is rejected with a structured error rather than silently
truncated (use `Project` for a deliberately bounded subset). `Project`'s row handling
is bounded by a hard cap regardless of what is requested.

## License

MIT (see `LICENSE`).
