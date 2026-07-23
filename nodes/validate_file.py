import pyarrow as pa
import pyarrow.parquet as pq

from gen.messages_pb2 import FileFormat, ValidateFileRequest, ValidateFileResult
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    check_input_not_empty,
    invalid_argument,
    open_arrow_ipc,
)


def validate_file(ax: AxiomContext, input: ValidateFileRequest) -> ValidateFileResult:
    """Cheaply check whether bytes are a structurally well-formed Parquet or
    Arrow IPC file (footer/schema parses), without paying for a full schema
    or statistics read. Returns valid=false with a human-readable detail
    (not a crash or a raised error) for anything unparseable; a hard
    input-level failure (e.g. empty input) is reported via the separate
    structured error field.
    """
    empty_err = check_input_not_empty(input.data)
    if empty_err is not None:
        return ValidateFileResult(error=empty_err)

    if input.format == FileFormat.FILE_FORMAT_PARQUET:
        try:
            pf = pq.ParquetFile(pa.BufferReader(input.data))
        except Exception as e:
            return ValidateFileResult(valid=False, detail=f"malformed Parquet file: {e}")
        return ValidateFileResult(valid=True, detected_format="parquet", detail="")

    if input.format == FileFormat.FILE_FORMAT_ARROW_IPC:
        try:
            opened = open_arrow_ipc(input.data)
        except Exception as e:
            return ValidateFileResult(valid=False, detail=str(e))
        return ValidateFileResult(valid=True, detected_format=opened.kind, detail="")

    return ValidateFileResult(
        error=invalid_argument("format must be FILE_FORMAT_PARQUET or FILE_FORMAT_ARROW_IPC")
    )
