"""Shared helpers for christiangeorgelucas/parquet-tools nodes.

A node is a pure input->output function; payload size, decode-memory, and
DoS containment are the platform's job (ingress/gateway/sidecar limits and
sandboxed execution), not this package's. This module intentionally
contains no byte-size, row-count, or decoded-size ceiling.
"""

import datetime
import io
import json as _json
from decimal import Decimal

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from gen.messages_pb2 import Error, FileFormat


class FormatError(ValueError):
    """Raised when bytes cannot be parsed as the declared FileFormat.
    Callers map this to the PARSE_ERROR error code."""


# --- Error helpers ---------------------------------------------------------


def make_error(code: str, message: str) -> Error:
    return Error(code=code, message=message)


def invalid_input(message: str) -> Error:
    return make_error("INVALID_INPUT", message)


def invalid_argument(message: str) -> Error:
    return make_error("INVALID_ARGUMENT", message)


def parse_error(message: str) -> Error:
    return make_error("PARSE_ERROR", message)


def check_input_not_empty(data: bytes):
    """Returns an Error if `data` is empty, else None."""
    if not data:
        return invalid_input("data is empty")
    return None


def require_columnar_format(fmt) -> None:
    """Raises ValueError unless fmt is Parquet or Arrow IPC."""
    if fmt not in (FileFormat.FILE_FORMAT_PARQUET, FileFormat.FILE_FORMAT_ARROW_IPC):
        raise ValueError(
            "format must be FILE_FORMAT_PARQUET or FILE_FORMAT_ARROW_IPC"
        )


# --- Arrow IPC framing detection -------------------------------------------


class OpenArrowIpcResult:
    __slots__ = ("reader", "kind")

    def __init__(self, reader, kind):
        self.reader = reader
        self.kind = kind


def open_arrow_ipc(data: bytes) -> OpenArrowIpcResult:
    """Open Arrow IPC bytes, trying the (seekable) file framing first, then
    falling back to the streaming framing. Raises FormatError if neither
    parses."""
    try:
        reader = ipc.open_file(io.BytesIO(data))
        return OpenArrowIpcResult(reader, "arrow_ipc_file")
    except pa.lib.ArrowInvalid:
        pass
    try:
        reader = ipc.open_stream(io.BytesIO(data))
        return OpenArrowIpcResult(reader, "arrow_ipc_stream")
    except pa.lib.ArrowInvalid as e:
        raise FormatError(f"not a valid Arrow IPC file or stream: {e}")


# --- Value/metadata stringification -----------------------------------------


def decode_kv_metadata(raw_metadata) -> dict:
    """Decode a pyarrow schema/file key-value metadata mapping (bytes keys
    and values) to a str->str dict, dropping entries that are not valid
    UTF-8 (a `map<string,string>` proto field cannot carry them)."""
    out = {}
    if not raw_metadata:
        return out
    for k, v in raw_metadata.items():
        try:
            key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val = v.decode("utf-8") if isinstance(v, bytes) else str(v)
        except UnicodeDecodeError:
            continue
        out[key] = val
    return out


def stringify_scalar(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return str(value)


def parquet_field_id(field) -> int:
    """Best-effort Parquet field id from Arrow field metadata; -1 if the
    file doesn't declare one (the common case)."""
    meta = field.metadata or {}
    raw = meta.get(b"PARQUET:field_id")
    if raw is None:
        return -1
    try:
        return int(raw)
    except (TypeError, ValueError):
        return -1


# --- Format conversion -------------------------------------------------------


def _json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
        return o.isoformat()
    if isinstance(o, bytes):
        return o.hex()
    if isinstance(o, Decimal):
        return str(o)
    return str(o)


def table_to_json_bytes(table: "pa.Table") -> bytes:
    records = table.to_pylist()
    return _json.dumps(records, default=_json_default).encode("utf-8")


def json_bytes_to_table(data: bytes) -> "pa.Table":
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as e:
        raise FormatError(f"JSON input is not valid UTF-8: {e}")
    try:
        obj = _json.loads(text)
    except _json.JSONDecodeError as e:
        raise FormatError(f"malformed JSON: {e}")
    if not isinstance(obj, list):
        raise FormatError("JSON input must be an array of flat record objects")
    if not all(isinstance(r, dict) for r in obj):
        raise FormatError("every element of the JSON array must be an object")
    if not obj:
        return pa.table({})
    try:
        return pa.Table.from_pylist(obj)
    except (pa.lib.ArrowInvalid, pa.lib.ArrowTypeError) as e:
        raise FormatError(f"JSON records have inconsistent/unsupported types: {e}")


def csv_bytes_to_table(data: bytes) -> "pa.Table":
    try:
        return pacsv.read_csv(io.BytesIO(data))
    except (pa.lib.ArrowInvalid, ValueError) as e:
        raise FormatError(f"malformed CSV: {e}")


def table_to_csv_bytes(table: "pa.Table") -> bytes:
    buf = io.BytesIO()
    pacsv.write_csv(table, buf)
    return buf.getvalue()


def table_to_parquet_bytes(table: "pa.Table") -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def table_to_arrow_ipc_bytes(table: "pa.Table") -> bytes:
    buf = io.BytesIO()
    with ipc.new_file(buf, table.schema) as writer:
        writer.write_table(table)
    return buf.getvalue()


def read_table_from_bytes(data: bytes, fmt) -> "pa.Table":
    """Decode `data` per FileFormat `fmt` into a pa.Table (all rows/columns).
    Raises FormatError on malformed input."""
    if fmt == FileFormat.FILE_FORMAT_PARQUET:
        try:
            pf = pq.ParquetFile(io.BytesIO(data))
        except Exception as e:
            raise FormatError(f"malformed Parquet file: {e}")
        try:
            return pf.read()
        except Exception as e:
            raise FormatError(f"malformed Parquet file: {e}")
    if fmt == FileFormat.FILE_FORMAT_ARROW_IPC:
        opened = open_arrow_ipc(data)
        try:
            return opened.reader.read_all()
        except Exception as e:
            raise FormatError(f"malformed Arrow IPC file: {e}")
    if fmt == FileFormat.FILE_FORMAT_CSV:
        return csv_bytes_to_table(data)
    if fmt == FileFormat.FILE_FORMAT_JSON:
        return json_bytes_to_table(data)
    raise ValueError("format must be a supported FileFormat")


def write_table_to_bytes(table: "pa.Table", fmt) -> bytes:
    if fmt == FileFormat.FILE_FORMAT_PARQUET:
        return table_to_parquet_bytes(table)
    if fmt == FileFormat.FILE_FORMAT_ARROW_IPC:
        return table_to_arrow_ipc_bytes(table)
    if fmt == FileFormat.FILE_FORMAT_CSV:
        return table_to_csv_bytes(table)
    if fmt == FileFormat.FILE_FORMAT_JSON:
        return table_to_json_bytes(table)
    raise ValueError("format must be a supported FileFormat")
