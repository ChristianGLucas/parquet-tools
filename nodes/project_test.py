import io
import json as _stdlib_json

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from gen.messages_pb2 import FileFormat, ProjectRequest
from nodes.project import project
from nodes._test_fixtures import FakeAxiomContext, arrow_ipc_file_bytes, parquet_bytes

N = 20


def _bigger_parquet_bytes(row_group_size=7):
    table = pa.table(
        {
            "id": pa.array(list(range(N)), type=pa.int64()),
            "label": pa.array([f"row{i}" for i in range(N)], type=pa.string()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", row_group_size=row_group_size)
    return buf.getvalue()


def test_project_column_selection_parquet():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=parquet_bytes(),
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            columns=["name"],
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code == ""
    assert list(result.columns) == ["name"]
    records = _stdlib_json.loads(result.data.decode("utf-8"))
    assert records == [{"name": "alice"}, {"name": "bob"}, {"name": "carol"}, {"name": "dave"}, {"name": "eve"}]
    assert result.num_rows == 5
    assert result.total_rows_available == 5
    assert result.truncated is False


def test_project_row_offset_and_limit_spans_row_groups():
    ax = FakeAxiomContext()
    data = _bigger_parquet_bytes(row_group_size=7)  # groups: 0-6, 7-13, 14-19
    result = project(
        ax,
        ProjectRequest(
            data=data,
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            row_offset=10,
            row_limit=5,
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code == ""
    records = _stdlib_json.loads(result.data.decode("utf-8"))
    assert [r["id"] for r in records] == [10, 11, 12, 13, 14]
    assert result.num_rows == 5
    assert result.total_rows_available == N
    assert result.truncated is True  # 15..19 still remain


def test_project_default_row_limit_returns_everything_when_under_default():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=parquet_bytes(),  # 5 rows, well under DEFAULT_ROW_LIMIT
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_CSV,
        ),
    )
    assert result.error.code == ""
    assert result.num_rows == 5
    assert result.truncated is False


def test_project_offset_past_end_returns_empty_not_error():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=parquet_bytes(),
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            row_offset=1000,
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code == ""
    assert result.num_rows == 0
    assert result.total_rows_available == 5


def test_project_unknown_column_is_error():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=parquet_bytes(),
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            columns=["does_not_exist"],
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code == "INVALID_ARGUMENT"


def test_project_negative_offset_is_error():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=parquet_bytes(),
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            row_offset=-1,
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code == "INVALID_ARGUMENT"


def test_project_arrow_ipc_source_to_parquet_output():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=arrow_ipc_file_bytes(),
            input_format=FileFormat.FILE_FORMAT_ARROW_IPC,
            columns=["id", "score"],
            row_limit=3,
            output_format=FileFormat.FILE_FORMAT_PARQUET,
        ),
    )
    assert result.error.code == ""
    table = pq.read_table(pa.BufferReader(result.data))
    assert table.column_names == ["id", "score"]
    assert table.num_rows == 3
    assert result.truncated is True
    assert result.total_rows_available == 5


def test_project_malformed_input_is_error():
    ax = FakeAxiomContext()
    result = project(
        ax,
        ProjectRequest(
            data=b"garbage bytes",
            input_format=FileFormat.FILE_FORMAT_PARQUET,
            output_format=FileFormat.FILE_FORMAT_JSON,
        ),
    )
    assert result.error.code != ""
    assert result.data == b""
