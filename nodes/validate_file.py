import pyarrow as pa
import pyarrow.parquet as pq

from gen.messages_pb2 import FileFormat, ValidateFileRequest, ValidateFileResult
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    MAX_ESTIMATED_DECODE_BYTES,
    check_input_size,
    estimate_decoded_bytes,
    invalid_argument,
    open_arrow_ipc,
)


def validate_file(ax: AxiomContext, input: ValidateFileRequest) -> ValidateFileResult:
    """Cheaply check whether bytes are a structurally well-formed Parquet or
    Arrow IPC file (footer/schema parses) within the package's documented
    size caps, without paying for a full schema or statistics read. Returns
    valid=false with a human-readable detail (not a crash or a raised
    error) for anything unparseable or over the decompressed-size cap; a
    hard input-level failure (e.g. the raw payload itself is oversized) is
    reported via the separate structured error field.
    """
    size_err = check_input_size(input.data)
    if size_err is not None:
        return ValidateFileResult(error=size_err)

    if input.format == FileFormat.FILE_FORMAT_PARQUET:
        try:
            pf = pq.ParquetFile(pa.BufferReader(input.data))
        except Exception as e:
            return ValidateFileResult(valid=False, detail=f"malformed Parquet file: {e}")
        est = estimate_decoded_bytes(pf.schema_arrow, None, pf.metadata.num_rows)
        if est > MAX_ESTIMATED_DECODE_BYTES:
            return ValidateFileResult(
                valid=False,
                detected_format="parquet",
                detail=(
                    f"parses, but estimated decoded size ~{est} bytes exceeds "
                    f"the {MAX_ESTIMATED_DECODE_BYTES}-byte cap "
                    f"({pf.metadata.num_rows} rows)"
                ),
            )
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
