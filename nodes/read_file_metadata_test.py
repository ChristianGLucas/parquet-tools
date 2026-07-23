from gen.messages_pb2 import FileFormat, ReadFileMetadataRequest
from nodes.read_file_metadata import read_file_metadata
from nodes._test_fixtures import (
    FakeAxiomContext,
    arrow_ipc_file_bytes,
    arrow_ipc_stream_bytes,
    parquet_bytes,
)


def test_read_file_metadata_parquet_golden():
    ax = FakeAxiomContext()
    # Hand-known: sample_table() has 5 rows; force 2 row groups via
    # row_group_size=3 (rows 0-2, 3-4) -- an independent, author-computed
    # expectation, not something read back from pyarrow.
    data = parquet_bytes(row_group_size=3)
    result = read_file_metadata(
        ax, ReadFileMetadataRequest(data=data, format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.error.code == ""
    assert result.num_rows == 5
    assert result.num_columns == 3
    assert result.num_row_groups == 2
    assert len(result.row_groups) == 2
    assert result.row_groups[0].num_rows == 3
    assert result.row_groups[1].num_rows == 2
    assert sum(rg.num_rows for rg in result.row_groups) == 5
    assert result.created_by != ""


def test_read_file_metadata_arrow_ipc_file():
    ax = FakeAxiomContext()
    result = read_file_metadata(
        ax,
        ReadFileMetadataRequest(data=arrow_ipc_file_bytes(), format=FileFormat.FILE_FORMAT_ARROW_IPC),
    )
    assert result.error.code == ""
    assert result.num_rows == 5
    assert result.num_columns == 3
    assert result.format_version == "arrow_ipc_file"
    assert result.num_row_groups == 1  # one record batch written


def test_read_file_metadata_arrow_ipc_stream_has_no_row_groups():
    ax = FakeAxiomContext()
    result = read_file_metadata(
        ax,
        ReadFileMetadataRequest(
            data=arrow_ipc_stream_bytes(), format=FileFormat.FILE_FORMAT_ARROW_IPC
        ),
    )
    assert result.error.code == ""
    assert result.num_rows == 5
    assert result.format_version == "arrow_ipc_stream"
    assert result.num_row_groups == 0
    assert list(result.row_groups) == []


def test_read_file_metadata_error_path():
    ax = FakeAxiomContext()
    result = read_file_metadata(
        ax, ReadFileMetadataRequest(data=b"garbage", format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.error.code == "PARSE_ERROR"


def test_read_file_metadata_rejects_bad_format():
    ax = FakeAxiomContext()
    result = read_file_metadata(
        ax,
        ReadFileMetadataRequest(data=parquet_bytes(), format=FileFormat.FILE_FORMAT_CSV),
    )
    assert result.error.code == "INVALID_ARGUMENT"
