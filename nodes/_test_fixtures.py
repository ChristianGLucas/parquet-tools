"""Shared fixture builders for parquet-tools node tests."""
import io

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from gen.axiom_context import SecretStatus


class FakeAxiomContext:
    """Minimal AxiomContext implementation for unit tests."""

    class _Logger:
        def debug(self, msg: str, **attrs) -> None: pass
        def info(self, msg: str, **attrs) -> None: pass
        def warn(self, msg: str, **attrs) -> None: pass
        def error(self, msg: str, **attrs) -> None: pass

    class _Secrets:
        def __init__(self, m: dict, revoked: set) -> None:
            self._m = m or {}
            self._revoked = revoked or set()
        def get(self, name: str):
            v = self._m.get(name)
            return (v, True) if v is not None else ("", False)
        def status(self, name: str) -> SecretStatus:
            if name in self._m:
                return SecretStatus.AVAILABLE
            if name in self._revoked:
                return SecretStatus.REVOKED
            return SecretStatus.UNSET

    def __init__(self, secrets_map=None, revoked_names=None) -> None:
        self.log = self._Logger()
        self.secrets = self._Secrets(secrets_map or {}, revoked_names)
        self.execution_id = "test-execution-id"
        self.flow_id = "test-flow-id"
        self.tenant_id = "test-tenant-id"

# A small, hand-known table used across tests. Every expected value derived
# from these Python literals in the tests is an independent, hand-computed
# oracle -- not something re-derived by reading the file back through the
# same pyarrow machinery under test.
IDS = [1, 2, 3, None, 5]
NAMES = ["alice", "bob", "carol", "dave", "eve"]
SCORES = [10.5, 20.0, None, 40.25, 50.0]


def sample_table() -> "pa.Table":
    return pa.table(
        {
            "id": pa.array(IDS, type=pa.int64()),
            "name": pa.array(NAMES, type=pa.string()),
            "score": pa.array(SCORES, type=pa.float64()),
        }
    )


def parquet_bytes(table=None, row_group_size=None, compression="snappy") -> bytes:
    table = table if table is not None else sample_table()
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression, row_group_size=row_group_size)
    return buf.getvalue()


def arrow_ipc_file_bytes(table=None) -> bytes:
    table = table if table is not None else sample_table()
    buf = io.BytesIO()
    with ipc.new_file(buf, table.schema) as writer:
        writer.write_table(table)
    return buf.getvalue()


def arrow_ipc_stream_bytes(table=None) -> bytes:
    table = table if table is not None else sample_table()
    buf = io.BytesIO()
    with ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    return buf.getvalue()


def csv_bytes(table=None) -> bytes:
    table = table if table is not None else sample_table()
    buf = io.BytesIO()
    pacsv.write_csv(table, buf)
    return buf.getvalue()


def json_array_bytes(records=None) -> bytes:
    import json as _json

    if records is None:
        records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    return _json.dumps(records).encode("utf-8")
