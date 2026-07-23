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
- **ConvertFormat** — convert a whole file between Parquet, Arrow IPC, CSV, and
  JSON.
- **Project** — select/project a bounded subset of columns and rows into any of those
  formats.

## Sizing

Every node is a pure input->output function with no self-imposed payload-size,
row-count, or decoded-size limit — payload size, memory, and DoS containment are the
platform's job, not this package's. `ConvertFormat` represents the whole file, so its
output scales with input size. `Project` lets a caller select a deliberately bounded
column/row subset (row offset/limit default to 5,000 rows when unspecified, and are
otherwise honored as requested) — a domain feature for narrowing what's read, not a
resource guard.

## License

MIT (see `LICENSE`).
