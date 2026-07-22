"""Shared helpers for christiangeorgelucas/parquet-tools nodes.

Bounds and rationale
---------------------
The platform's deployed-invocation HTTP ingress accepts up to a 16 MiB
invoke payload (request body up to 17 MiB; the node gRPC transport itself
caps at 24 MiB) — that is the real binding limit for a payload-bearing node.
(An earlier revision of this package capped MAX_INPUT_BYTES/MAX_OUTPUT_BYTES
at 640 KiB to work around a since-fixed ingress bug that silently truncated
the limit to ~1 MiB; that workaround no longer applies.) A `data` field is
carried as base64 inside a JSON envelope, which expands raw bytes by ~1.33x
before framing overhead, so MAX_INPUT_BYTES/MAX_OUTPUT_BYTES are set to
11 MiB raw — ~14.6 MiB base64-encoded, leaving headroom under the 16 MiB
invoke-payload ceiling for JSON envelope/field overhead.

Cost bound on decoding: Parquet's footer reports `num_rows` for free (no
decompression). Its *declared* per-column "uncompressed size" is NOT a
reliable proxy for post-decode memory — dictionary/RLE encoding can make it
tiny even when the fully-materialized array is huge (e.g. 20M repeated
int64 values encode to ~80 KB but decode to ~160 MB) — so the pre-decode
guard here instead estimates decoded size as `num_rows * per-column fixed
width` (or a conservative fixed estimate for variable-width types), which is
insensitive to encoding tricks. Arrow IPC has no equivalent cheap
pre-check (its optional LZ4/ZSTD body compression can expand a payload well
under MAX_INPUT_BYTES into tens of MB once decoded), so its Table is
size-checked immediately after materialization and discarded on overflow.
"""

import datetime
import io
import json as _json
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from gen.messages_pb2 import Error, FileFormat

# --- Bounds --------------------------------------------------------------

MAX_INPUT_BYTES = 11 * 1024 * 1024
MAX_OUTPUT_BYTES = 11 * 1024 * 1024

MAX_ROW_GROUPS_RETURNED = 1000
MAX_COLUMNS_RETURNED = 500

DEFAULT_ROW_LIMIT = 5_000
MAX_ROWS_HARD_CAP = 50_000

# Pre-decode cost guard (Parquet): reject before materializing rows if the
# estimated decoded size would exceed this.
MAX_ESTIMATED_DECODE_BYTES = 100 * 1024 * 1024
# Post-decode guard (Arrow IPC): if the materialized Table ends up bigger
# than this, discard it and error rather than doing further work.
MAX_DECODED_BYTES = 100 * 1024 * 1024

DEFAULT_VARWIDTH_ESTIMATE_BYTES = 64


class TooLargeError(ValueError):
    """Raised when an input or an estimated/actual decoded size exceeds a
    documented bound. Callers map this to the TOO_LARGE error code."""


class FormatError(ValueError):
    """Raised when bytes cannot be parsed as the declared FileFormat.
    Callers map this to the PARSE_ERROR error code."""


# --- Error helpers ---------------------------------------------------------


def make_error(code: str, message: str) -> Error:
    return Error(code=code, message=message)


def too_large(message: str) -> Error:
    return make_error("TOO_LARGE", message)


def invalid_input(message: str) -> Error:
    return make_error("INVALID_INPUT", message)


def invalid_argument(message: str) -> Error:
    return make_error("INVALID_ARGUMENT", message)


def parse_error(message: str) -> Error:
    return make_error("PARSE_ERROR", message)


def check_input_size(data: bytes):
    """Returns an Error if `data` is empty or exceeds MAX_INPUT_BYTES, else
    None."""
    if not data:
        return invalid_input("data is empty")
    if len(data) > MAX_INPUT_BYTES:
        return too_large(
            f"input is {len(data)} bytes, over the {MAX_INPUT_BYTES}-byte cap"
        )
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


# --- Decode-cost estimation -------------------------------------------------


def arrow_type_row_width_bytes(arrow_type) -> int:
    """Best-effort estimate of one value's decoded in-memory footprint.
    Fixed-width types use their real bit width; anything else (strings,
    lists, structs, ...) uses a fixed conservative estimate."""
    try:
        bits = arrow_type.bit_width
        if bits and bits > 0:
            return max(1, bits // 8)
    except (ValueError, AttributeError):
        pass
    return DEFAULT_VARWIDTH_ESTIMATE_BYTES


def estimate_decoded_bytes(schema: "pa.Schema", column_names, num_rows: int) -> int:
    fields = list(schema) if not column_names else [schema.field(n) for n in column_names]
    per_row = sum(arrow_type_row_width_bytes(f.type) for f in fields)
    return per_row * max(0, num_rows)


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
    Raises FormatError on malformed input, TooLargeError if the estimated or
    actual decoded size exceeds the documented bound."""
    if fmt == FileFormat.FILE_FORMAT_PARQUET:
        try:
            pf = pq.ParquetFile(io.BytesIO(data))
        except Exception as e:
            raise FormatError(f"malformed Parquet file: {e}")
        est = estimate_decoded_bytes(pf.schema_arrow, None, pf.metadata.num_rows)
        if est > MAX_ESTIMATED_DECODE_BYTES:
            raise TooLargeError(
                f"estimated decoded size ~{est} bytes exceeds the "
                f"{MAX_ESTIMATED_DECODE_BYTES}-byte cap "
                f"({pf.metadata.num_rows} rows)"
            )
        try:
            return pf.read()
        except Exception as e:
            raise FormatError(f"malformed Parquet file: {e}")
    if fmt == FileFormat.FILE_FORMAT_ARROW_IPC:
        opened = open_arrow_ipc(data)
        try:
            table = opened.reader.read_all()
        except Exception as e:
            raise FormatError(f"malformed Arrow IPC file: {e}")
        if table.nbytes > MAX_DECODED_BYTES:
            raise TooLargeError(
                f"decoded size {table.nbytes} bytes exceeds the "
                f"{MAX_DECODED_BYTES}-byte cap"
            )
        return table
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
