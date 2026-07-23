from gen.messages_pb2 import ConvertFormatRequest, ConvertFormatResult, FileFormat
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    FormatError,
    check_input_not_empty,
    invalid_argument,
    parse_error,
    read_table_from_bytes,
    write_table_to_bytes,
)

_VALID_FORMATS = {
    FileFormat.FILE_FORMAT_PARQUET,
    FileFormat.FILE_FORMAT_ARROW_IPC,
    FileFormat.FILE_FORMAT_CSV,
    FileFormat.FILE_FORMAT_JSON,
}


def convert_format(ax: AxiomContext, input: ConvertFormatRequest) -> ConvertFormatResult:
    """Convert a whole file between Parquet, Arrow IPC, CSV, and JSON (any
    pairing, including the same format in and out as a normalize/rewrite
    pass). Represents the ENTIRE input in the target format — use Project
    instead for a deliberately bounded column/row subset. Malformed input
    for the declared input_format returns a structured PARSE_ERROR.
    """
    empty_err = check_input_not_empty(input.data)
    if empty_err is not None:
        return ConvertFormatResult(error=empty_err)

    if input.input_format not in _VALID_FORMATS:
        return ConvertFormatResult(error=invalid_argument("input_format is not a supported FileFormat"))
    if input.output_format not in _VALID_FORMATS:
        return ConvertFormatResult(error=invalid_argument("output_format is not a supported FileFormat"))

    try:
        table = read_table_from_bytes(input.data, input.input_format)
    except FormatError as e:
        return ConvertFormatResult(error=parse_error(str(e)))
    except ValueError as e:
        return ConvertFormatResult(error=parse_error(str(e)))

    try:
        out_bytes = write_table_to_bytes(table, input.output_format)
    except Exception as e:
        return ConvertFormatResult(error=parse_error(f"could not encode as the requested output_format: {e}"))

    return ConvertFormatResult(data=out_bytes, output_format=input.output_format, num_rows=table.num_rows)
