import pyarrow as pa
import pyarrow.parquet as pq

from gen.messages_pb2 import ColumnSchema, FileFormat, ReadSchemaRequest, ReadSchemaResult
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    check_input_not_empty,
    decode_kv_metadata,
    invalid_argument,
    open_arrow_ipc,
    parquet_field_id,
    parse_error,
)


def read_schema(ax: AxiomContext, input: ReadSchemaRequest) -> ReadSchemaResult:
    """Read a Parquet or Arrow IPC file's schema: every column's name, Arrow
    logical type, nullability, and Parquet field id (-1 for Arrow IPC), plus
    schema-level key/value metadata. Detects whether Arrow IPC is
    file-framed or stream-framed. Malformed input or an unsupported/
    unspecified format returns a structured error rather than crashing.
    """
    empty_err = check_input_not_empty(input.data)
    if empty_err is not None:
        return ReadSchemaResult(error=empty_err)

    if input.format == FileFormat.FILE_FORMAT_PARQUET:
        try:
            pf = pq.ParquetFile(pa.BufferReader(input.data))
        except Exception as e:
            return ReadSchemaResult(error=parse_error(f"malformed Parquet file: {e}"))
        schema = pf.schema_arrow
        kv = decode_kv_metadata(pf.metadata.metadata)
        detected = "parquet"
    elif input.format == FileFormat.FILE_FORMAT_ARROW_IPC:
        try:
            opened = open_arrow_ipc(input.data)
        except Exception as e:
            return ReadSchemaResult(error=parse_error(str(e)))
        schema = opened.reader.schema
        kv = decode_kv_metadata(schema.metadata)
        detected = opened.kind
    else:
        return ReadSchemaResult(
            error=invalid_argument(
                "format must be FILE_FORMAT_PARQUET or FILE_FORMAT_ARROW_IPC"
            )
        )

    columns = [
        ColumnSchema(
            name=field.name,
            arrow_type=str(field.type),
            nullable=field.nullable,
            field_id=parquet_field_id(field),
        )
        for field in schema
    ]
    return ReadSchemaResult(columns=columns, metadata=kv, format_detected=detected)
