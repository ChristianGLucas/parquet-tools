import pyarrow as pa
import pyarrow.parquet as pq

from gen.messages_pb2 import FileFormat, ProjectRequest, ProjectResult
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    FormatError,
    check_input_not_empty,
    invalid_argument,
    open_arrow_ipc,
    parse_error,
    write_table_to_bytes,
)

# Default row_limit when the caller passes <=0 (a domain default, not a
# resource ceiling — the caller can always ask for more via row_limit).
DEFAULT_ROW_LIMIT = 5_000

_OUTPUT_FORMATS = {
    FileFormat.FILE_FORMAT_PARQUET,
    FileFormat.FILE_FORMAT_ARROW_IPC,
    FileFormat.FILE_FORMAT_CSV,
    FileFormat.FILE_FORMAT_JSON,
}


class UnknownColumnError(ValueError):
    pass


def _resolve_columns(all_names, requested):
    if not requested:
        return list(all_names)
    unknown = [c for c in requested if c not in all_names]
    if unknown:
        raise UnknownColumnError(f"unknown column(s): {', '.join(unknown)}")
    return list(requested)


def _project_parquet(data: bytes, columns, row_offset: int, row_limit: int):
    pf = pq.ParquetFile(pa.BufferReader(data))
    schema = pf.schema_arrow
    all_names = [f.name for f in schema]
    selected = _resolve_columns(all_names, columns)

    total_rows = pf.metadata.num_rows
    need = 0 if row_offset >= total_rows else min(row_offset + row_limit, total_rows)

    if need == 0:
        table = pf.schema_arrow.empty_table().select(selected)
    else:
        chunks = []
        rows_seen = 0
        for i in range(pf.metadata.num_row_groups):
            if rows_seen >= need:
                break
            chunks.append(pf.read_row_group(i, columns=selected))
            rows_seen += chunks[-1].num_rows
        table = pa.concat_tables(chunks) if chunks else pf.schema_arrow.empty_table().select(selected)

    result = table.slice(row_offset, row_limit)
    truncated = (row_offset + result.num_rows) < total_rows
    return result, selected, total_rows, truncated


def _project_arrow_ipc(data: bytes, columns, row_offset: int, row_limit: int):
    opened = open_arrow_ipc(data)
    all_names = opened.reader.schema.names
    selected = _resolve_columns(all_names, columns)

    need = row_offset + row_limit
    chunks = []
    rows_seen = 0
    total_rows = 0
    if opened.kind == "arrow_ipc_file":
        for i in range(opened.reader.num_record_batches):
            batch = opened.reader.get_batch(i)
            total_rows += batch.num_rows
            if rows_seen < need:
                chunks.append(pa.Table.from_batches([batch.select(selected)]))
                rows_seen += batch.num_rows
    else:
        for batch in opened.reader:
            total_rows += batch.num_rows
            if rows_seen < need:
                chunks.append(pa.Table.from_batches([batch.select(selected)]))
                rows_seen += batch.num_rows

    table = pa.concat_tables(chunks) if chunks else pa.schema(
        [f for f in opened.reader.schema if f.name in selected]
    ).empty_table()

    result = table.slice(row_offset, row_limit)
    truncated = (row_offset + result.num_rows) < total_rows
    return result, selected, total_rows, truncated


def project(ax: AxiomContext, input: ProjectRequest) -> ProjectResult:
    """Select/project a bounded subset of columns and rows out of a Parquet
    or Arrow IPC file and emit it as Parquet, Arrow IPC, CSV, or JSON.
    Columns (empty = all) and a row offset/limit narrow what is read; the
    limit defaults to 5,000 rows when not specified. truncated=true and
    total_rows_available report whenever the result is a strict subset of
    what was available at the given offset. An unknown requested column
    name or malformed input returns a structured error.
    """
    empty_err = check_input_not_empty(input.data)
    if empty_err is not None:
        return ProjectResult(error=empty_err)

    if input.input_format not in (FileFormat.FILE_FORMAT_PARQUET, FileFormat.FILE_FORMAT_ARROW_IPC):
        return ProjectResult(
            error=invalid_argument("input_format must be FILE_FORMAT_PARQUET or FILE_FORMAT_ARROW_IPC")
        )
    if input.output_format not in _OUTPUT_FORMATS:
        return ProjectResult(error=invalid_argument("output_format is not a supported FileFormat"))
    if input.row_offset < 0:
        return ProjectResult(error=invalid_argument("row_offset must be >= 0"))

    row_limit = input.row_limit if input.row_limit > 0 else DEFAULT_ROW_LIMIT

    try:
        if input.input_format == FileFormat.FILE_FORMAT_PARQUET:
            table, selected, total_rows, truncated = _project_parquet(
                input.data, input.columns, input.row_offset, row_limit
            )
        else:
            table, selected, total_rows, truncated = _project_arrow_ipc(
                input.data, input.columns, input.row_offset, row_limit
            )
    except UnknownColumnError as e:
        return ProjectResult(error=invalid_argument(str(e)))
    except (FormatError, pa.lib.ArrowInvalid) as e:
        return ProjectResult(error=parse_error(f"malformed input: {e}"))
    except Exception as e:
        return ProjectResult(error=parse_error(f"malformed input: {e}"))

    try:
        out_bytes = write_table_to_bytes(table, input.output_format)
    except Exception as e:
        return ProjectResult(error=parse_error(f"could not encode as the requested output_format: {e}"))

    return ProjectResult(
        data=out_bytes,
        columns=selected,
        num_rows=table.num_rows,
        total_rows_available=total_rows,
        truncated=truncated,
    )
