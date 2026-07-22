from gen.messages_pb2 import FileFormat, ReadSchemaRequest
from nodes.read_schema import read_schema
from nodes._test_fixtures import (
    FakeAxiomContext,
    arrow_ipc_file_bytes,
    arrow_ipc_stream_bytes,
    parquet_bytes,
    sample_table,
)


def test_read_schema_parquet_golden():
    ax = FakeAxiomContext()
    result = read_schema(
        ax, ReadSchemaRequest(data=parquet_bytes(), format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.error.code == ""
    assert result.format_detected == "parquet"
    names = [c.name for c in result.columns]
    assert names == ["id", "name", "score"]
    by_name = {c.name: c for c in result.columns}
    assert by_name["id"].arrow_type == "int64"
    assert by_name["name"].arrow_type == "string"
    assert by_name["score"].arrow_type == "double"
    # Every column in the fixture is nullable (built from a plain Python
    # list containing None).
    assert all(c.nullable for c in result.columns)


def test_read_schema_arrow_ipc_file_detects_framing():
    ax = FakeAxiomContext()
    result = read_schema(
        ax,
        ReadSchemaRequest(data=arrow_ipc_file_bytes(), format=FileFormat.FILE_FORMAT_ARROW_IPC),
    )
    assert result.error.code == ""
    assert result.format_detected == "arrow_ipc_file"
    assert [c.name for c in result.columns] == ["id", "name", "score"]


def test_read_schema_arrow_ipc_stream_detects_framing():
    ax = FakeAxiomContext()
    result = read_schema(
        ax,
        ReadSchemaRequest(data=arrow_ipc_stream_bytes(), format=FileFormat.FILE_FORMAT_ARROW_IPC),
    )
    assert result.error.code == ""
    assert result.format_detected == "arrow_ipc_stream"


def test_read_schema_rejects_unspecified_format():
    ax = FakeAxiomContext()
    result = read_schema(
        ax, ReadSchemaRequest(data=parquet_bytes(), format=FileFormat.FILE_FORMAT_UNSPECIFIED)
    )
    assert result.error.code == "INVALID_ARGUMENT"


def test_read_schema_error_path_malformed_input():
    ax = FakeAxiomContext()
    result = read_schema(
        ax, ReadSchemaRequest(data=b"not a parquet file", format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.error.code == "PARSE_ERROR"
    assert result.columns == []


def test_read_schema_error_path_empty_input():
    ax = FakeAxiomContext()
    result = read_schema(ax, ReadSchemaRequest(data=b"", format=FileFormat.FILE_FORMAT_PARQUET))
    assert result.error.code == "INVALID_INPUT"


def test_read_schema_is_deterministic():
    ax = FakeAxiomContext()
    req = ReadSchemaRequest(data=parquet_bytes(), format=FileFormat.FILE_FORMAT_PARQUET)
    r1 = read_schema(ax, req)
    r2 = read_schema(ax, req)
    assert list(r1.columns) == list(r2.columns)
