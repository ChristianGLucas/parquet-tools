import csv as _stdlib_csv
import io
import json as _stdlib_json

import pyarrow as pa
import pyarrow.parquet as pq

from gen.messages_pb2 import ConvertFormatRequest, FileFormat
from nodes.convert_format import convert_format
from nodes._helpers import MAX_INPUT_BYTES
from nodes._test_fixtures import (
    FakeAxiomContext,
    arrow_ipc_file_bytes,
    csv_bytes,
    json_array_bytes,
    parquet_bytes,
)


def test_convert_format_parquet_to_csv_oracle():
    """Independent oracle: parse the CSV output with Python's stdlib `csv`
    module (not pyarrow.csv, which the implementation itself uses) and
    check it against the hand-known fixture values."""
    ax = FakeAxiomContext()
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=parquet_bytes(),
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == ""
    assert result.output_format == FileFormat.FILE_FORMAT_CSV
    assert result.num_rows == 5
    rows = list(_stdlib_csv.reader(io.StringIO(result.data.decode("utf-8"))))
    assert rows[0] == ["id", "name", "score"]
    assert rows[1] == ["1", "alice", "10.5"]
    assert rows[4] == ["", "dave", "40.25"]  # row index 3 (dave) = id None


def test_convert_format_parquet_to_json_oracle():
    """Independent oracle: parse with stdlib `json` (not anything from the
    implementation) and check record values directly."""
    ax = FakeAxiomContext()
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=parquet_bytes(),
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code == ""
    records = _stdlib_json.loads(result.data.decode("utf-8"))
    assert isinstance(records, list)
    assert len(records) == 5
    assert records[0] == {"id": 1, "name": "alice", "score": 10.5}
    assert records[3]["id"] is None
    assert records[3]["name"] == "dave"


def test_convert_format_json_to_parquet_roundtrip():
    ax = FakeAxiomContext()
    records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=json_array_bytes(records),
            input_format=FileFormat.FILE_FORMAT_JSON,
            output_format=FileFormat.FILE_FORMAT_PARQUET,
        ),
    )
    assert result.error.code == ""
    assert result.num_rows == 3
    table = pq.read_table(pa.BufferReader(result.data))
    assert table.to_pylist() == records


def test_convert_format_csv_to_arrow_ipc_roundtrip():
    ax = FakeAxiomContext()
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=csv_bytes(),
            input_format=FileFormat.FILE_FORMAT_CSV,
            output_format=FileFormat.FILE_FORMAT_ARROW_IPC,
        ),
    )
    assert result.error.code == ""
    import pyarrow.ipc as ipc

    table = ipc.open_file(pa.BufferReader(result.data)).read_all()
    assert table.num_rows == 5
    assert table.column("name").to_pylist() == ["alice", "bob", "carol", "dave", "eve"]


def test_convert_format_arrow_ipc_to_parquet_roundtrip():
    ax = FakeAxiomContext()
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=arrow_ipc_file_bytes(),
            input_format=FileFormat.FILE_FORMAT_ARROW_IPC,
            output_format=FileFormat.FILE_FORMAT_PARQUET,
        ),
    )
    assert result.error.code == ""
    table = pq.read_table(pa.BufferReader(result.data))
    assert table.num_rows == 5
    assert table.column("id").to_pylist() == [1, 2, 3, None, 5]


def test_convert_format_malformed_input_is_parse_error():
    ax = FakeAxiomContext()
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=b"not parquet at all",
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == "PARSE_ERROR"
    assert result.data == b""


def test_convert_format_malformed_json_is_parse_error():
    ax = FakeAxiomContext()
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=b'{"not": "an array"}',
            input_format=FileFormat.FILE_FORMAT_JSON,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == "PARSE_ERROR"


def test_convert_format_rejects_oversized_source_row_count():
    """A source whose declared row count implies a decoded size well past
    the estimate cap is rejected with TOO_LARGE before conversion, even
    though its on-disk (RLE/dictionary-compressed) size is tiny and well
    under the input-byte cap."""
    ax = FakeAxiomContext()
    n = 20_000_000
    huge_table = pa.table({"x": pa.array([1] * n, type=pa.int64())})
    buf = io.BytesIO()
    pq.write_table(huge_table, buf, compression="snappy")
    data = buf.getvalue()
    assert len(data) < MAX_INPUT_BYTES  # confirms this is a genuine "small
    # on disk, huge decoded" case, not just hitting the input-size cap first

    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=data,
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == "TOO_LARGE"


def test_convert_format_rejects_oversized_raw_input():
    """Regression test for the raw input-size error contract at the current
    MAX_INPUT_BYTES cap: bytes over the cap are rejected cleanly with
    TOO_LARGE before any parsing is attempted, not a crash or a timeout."""
    ax = FakeAxiomContext()
    oversized = b"x" * (MAX_INPUT_BYTES + 1)
    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=oversized,
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == "TOO_LARGE"
    assert result.data == b""


def test_convert_format_legit_payload_above_old_cap_now_flows():
    """A whole-file conversion result well over the package's old 640 KiB
    output cap (from before the platform's ingress bug was fixed) but
    comfortably under the current MAX_OUTPUT_BYTES cap must succeed, not be
    rejected as TOO_LARGE — proving the raised cap actually governs
    behavior, not just the row-count pre-check."""
    ax = FakeAxiomContext()
    rows = 20_000
    table = pa.table(
        {
            "id": pa.array(list(range(rows)), type=pa.int64()),
            "label": pa.array([f"row-{i:06d}-{'x' * 40}" for i in range(rows)], type=pa.string()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    data = buf.getvalue()

    result = convert_format(
        ax,
        ConvertFormatRequest(
            data=data,
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == ""
    assert len(result.data) > 640 * 1024  # bigger than the old cap...
    assert len(result.data) < MAX_INPUT_BYTES  # ...but well under the new one
    assert result.num_rows == rows


def test_convert_format_is_deterministic():
    ax = FakeAxiomContext()
    req = ConvertFormatRequest(
        data=parquet_bytes(),
        input_format=FileFormat.FILE_FORMAT_PARQUET,
        output_format=FileFormat.FILE_FORMAT_CSV,
    )
    r1 = convert_format(ax, req)
    r2 = convert_format(ax, req)
    assert r1.data == r2.data
