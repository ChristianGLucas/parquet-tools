from gen.messages_pb2 import FileFormat, ValidateFileRequest
from nodes.validate_file import validate_file
from nodes._test_fixtures import FakeAxiomContext, arrow_ipc_stream_bytes, parquet_bytes


def test_validate_file_valid_parquet():
    ax = FakeAxiomContext()
    result = validate_file(
        ax, ValidateFileRequest(data=parquet_bytes(), format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.error.code == ""
    assert result.valid is True
    assert result.detected_format == "parquet"
    assert result.detail == ""


def test_validate_file_valid_arrow_ipc_stream():
    ax = FakeAxiomContext()
    result = validate_file(
        ax,
        ValidateFileRequest(data=arrow_ipc_stream_bytes(), format=FileFormat.FILE_FORMAT_ARROW_IPC),
    )
    assert result.valid is True
    assert result.detected_format == "arrow_ipc_stream"


def test_validate_file_rejects_wrong_declared_format():
    ax = FakeAxiomContext()
    # Real Arrow IPC bytes declared as Parquet must fail structurally.
    result = validate_file(
        ax, ValidateFileRequest(data=arrow_ipc_stream_bytes(), format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.valid is False
    assert result.detail != ""


def test_validate_file_garbage_is_invalid_not_a_crash():
    ax = FakeAxiomContext()
    result = validate_file(
        ax,
        ValidateFileRequest(
            data=b"definitely not a columnar file", format=FileFormat.FILE_FORMAT_PARQUET
        ),
    )
    assert result.valid is False
    assert result.error.code == ""  # business outcome, not a hard input error
    assert result.detail != ""


def test_validate_file_empty_input_is_hard_error():
    ax = FakeAxiomContext()
    result = validate_file(ax, ValidateFileRequest(data=b"", format=FileFormat.FILE_FORMAT_PARQUET))
    assert result.error.code == "INVALID_INPUT"
