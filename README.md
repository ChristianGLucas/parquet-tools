# christiangeorgelucas/parquet-tools

Composable [Axiom](https://axiomide.com) nodes for deterministic, offline inspection and
bounded transformation of Apache Parquet and Apache Arrow IPC columnar files, wrapping
[pyarrow](https://arrow.apache.org/docs/python/) (Apache-2.0) — the reference Python
binding to the Arrow C++ library, which also owns the canonical Parquet reader/writer.

Built for the Axiom marketplace. Every node is a pure bytes-in/bytes-or-struct-out
transform — no filesystem, network, state, or secrets.

## Use it from your agent or app

Every node in this package is a **live, auto-scaling API endpoint** on the
[Axiom](https://axiomide.com) marketplace — call it from an AI agent or your own
code, with nothing to self-host.

**📦 See it on the marketplace:**
https://dev.axiomide.com/marketplace/christiangeorgelucas/parquet-tools@0.1.2

**Hook it up to an AI agent (MCP).** Add Axiom's hosted MCP server to any MCP
client and every node becomes a typed tool your agent can call — search the
catalog, inspect a schema, and invoke it directly.

```bash
# Claude Code
claude mcp add --transport http axiom https://api.axiomide.com/mcp \
  --header "Authorization: Bearer $AXIOM_API_KEY"
```

Claude Desktop, Cursor, or any config-based client:

```json
{
  "mcpServers": {
    "axiom": {
      "type": "http",
      "url": "https://api.axiomide.com/mcp",
      "headers": { "Authorization": "Bearer YOUR_AXIOM_API_KEY" }
    }
  }
}
```

**Call it from the CLI.**

```bash
axiom invoke christiangeorgelucas/parquet-tools/ReadSchema --input '{ ... }'
```

**Call it over HTTP.**

```bash
curl -X POST https://api.axiomide.com/invocations/v1/nodes/christiangeorgelucas/parquet-tools/0.1.2/ReadSchema \
  -H "Authorization: Bearer $AXIOM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{ ... }'
```

> Input/output schema for each node is on the marketplace page above, or via
> `axiom inspect node christiangeorgelucas/parquet-tools/ReadSchema`.

### Get started free

Install the CLI:

```bash
# macOS / Linux — Homebrew
brew install axiomide/tap/axiom

# macOS / Linux — install script
curl -fsSL https://raw.githubusercontent.com/AxiomIDE/axiom-releases/main/install.sh | sh
```

**Windows:** download the `windows/amd64` `.zip` from the
[releases page](https://github.com/AxiomIDE/axiom-releases/releases), unzip it,
and put `axiom.exe` on your `PATH`.

Then `axiom version` to verify, `axiom login` (GitHub or Google) to authenticate,
and create an API key under **Console → API Keys**. Docs and sign-up at
**[axiomide.com](https://axiomide.com)**.

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
