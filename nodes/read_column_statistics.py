import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from gen.messages_pb2 import (
    ColumnStatistics,
    FileFormat,
    ReadColumnStatisticsRequest,
    ReadColumnStatisticsResult,
)
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    MAX_COLUMNS_RETURNED,
    MAX_DECODED_BYTES,
    check_input_size,
    invalid_argument,
    open_arrow_ipc,
    parse_error,
    stringify_scalar,
    too_large,
)


def _parquet_column_index_by_name(row_group) -> dict:
    """Map top-level flat column name -> column-chunk index for a Parquet
    row group. Only exact (non-nested) path matches are indexed; nested/
    struct leaf paths are intentionally not flattened here (see the node's
    published description for the flat-schema scope)."""
    out = {}
    for j in range(row_group.num_columns):
        path = str(row_group.column(j).path_in_schema)
        out[path] = j
    return out


def _read_column_statistics_parquet(data: bytes, requested_columns):
    try:
        pf = pq.ParquetFile(pa.BufferReader(data))
    except Exception as e:
        return None, parse_error(f"malformed Parquet file: {e}")

    schema = pf.schema_arrow
    all_names = [f.name for f in schema]
    requested = list(requested_columns) if requested_columns else all_names
    unknown = [c for c in requested if c not in all_names]
    if unknown:
        return None, invalid_argument(f"unknown column(s): {', '.join(unknown)}")

    truncated = len(requested) > MAX_COLUMNS_RETURNED
    requested = requested[:MAX_COLUMNS_RETURNED]

    md = pf.metadata
    results = []
    for name in requested:
        physical_type = ""
        null_count = None
        distinct_count = -1
        min_val = None
        max_val = None
        has_min_max = True
        any_stats_missing = False
        compression = ""
        encodings = set()
        total_compressed = 0
        total_uncompressed = 0
        matched_any = False

        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            idx_by_name = _parquet_column_index_by_name(rg)
            j = idx_by_name.get(name)
            if j is None:
                continue
            matched_any = True
            col = rg.column(j)
            physical_type = col.physical_type
            if not compression:
                compression = col.compression
            encodings.update(col.encodings)
            total_compressed += col.total_compressed_size
            total_uncompressed += col.total_uncompressed_size

            stats = col.statistics
            if stats is None:
                any_stats_missing = True
                has_min_max = False
                continue
            if stats.null_count is None:
                any_stats_missing = True
            else:
                null_count = (null_count or 0) + stats.null_count
            if md.num_row_groups == 1 and stats.distinct_count is not None:
                distinct_count = stats.distinct_count
            if stats.has_min_max:
                if min_val is None or stats.min < min_val:
                    min_val = stats.min
                if max_val is None or stats.max > max_val:
                    max_val = stats.max
            else:
                has_min_max = False

        results.append(
            ColumnStatistics(
                name=name,
                physical_type=physical_type,
                null_count=-1 if (any_stats_missing or not matched_any) else null_count,
                distinct_count=distinct_count,
                min_value=stringify_scalar(min_val) if has_min_max else "",
                max_value=stringify_scalar(max_val) if has_min_max else "",
                has_min_max=has_min_max and matched_any,
                compression=compression,
                encodings=sorted(encodings),
                total_compressed_size=total_compressed,
                total_uncompressed_size=total_uncompressed,
            )
        )

    return ReadColumnStatisticsResult(
        columns=results, num_rows=md.num_rows, columns_truncated=truncated
    ), None


def _read_column_statistics_arrow_ipc(data: bytes, requested_columns):
    opened = open_arrow_ipc(data)
    try:
        table = opened.reader.read_all()
    except Exception as e:
        return None, parse_error(f"malformed Arrow IPC file: {e}")
    if table.nbytes > MAX_DECODED_BYTES:
        return None, too_large(
            f"decoded size {table.nbytes} bytes exceeds the {MAX_DECODED_BYTES}-byte cap"
        )

    all_names = table.schema.names
    requested = list(requested_columns) if requested_columns else all_names
    unknown = [c for c in requested if c not in all_names]
    if unknown:
        return None, invalid_argument(f"unknown column(s): {', '.join(unknown)}")

    truncated = len(requested) > MAX_COLUMNS_RETURNED
    requested = requested[:MAX_COLUMNS_RETURNED]

    results = []
    for name in requested:
        column = table.column(name)
        physical_type = str(column.type)
        null_count = column.null_count
        has_min_max = True
        min_val = max_val = None
        try:
            mm = pc.min_max(column)
            min_val = mm["min"].as_py()
            max_val = mm["max"].as_py()
            has_min_max = min_val is not None and max_val is not None
        except (pa.lib.ArrowNotImplementedError, pa.lib.ArrowInvalid):
            has_min_max = False

        results.append(
            ColumnStatistics(
                name=name,
                physical_type=physical_type,
                null_count=null_count,
                distinct_count=-1,
                min_value=stringify_scalar(min_val) if has_min_max else "",
                max_value=stringify_scalar(max_val) if has_min_max else "",
                has_min_max=has_min_max,
                compression="",
                encodings=[],
                total_compressed_size=0,
                total_uncompressed_size=column.nbytes,
            )
        )

    return ReadColumnStatisticsResult(
        columns=results, num_rows=table.num_rows, columns_truncated=truncated
    ), None


def read_column_statistics(ax: AxiomContext, input: ReadColumnStatisticsRequest) -> ReadColumnStatisticsResult:
    """Read per-column statistics for a Parquet or Arrow IPC file: physical
    type, null count, distinct count, stringified min/max, compression
    codec, and encodings, aggregated across the whole file. Parquet reports
    these from its footer's own column-chunk statistics (no full-file scan);
    Arrow IPC has no such footer, so its null_count/min/max are computed by
    one in-memory pass over the decoded data and distinct_count/compression/
    encodings are always absent. Caller can request specific columns (empty
    = all, capped at 500 with a truncation flag). Malformed input or an
    unknown requested column name returns a structured error.
    """
    size_err = check_input_size(input.data)
    if size_err is not None:
        return ReadColumnStatisticsResult(error=size_err)

    if input.format == FileFormat.FILE_FORMAT_PARQUET:
        result, err = _read_column_statistics_parquet(input.data, input.columns)
    elif input.format == FileFormat.FILE_FORMAT_ARROW_IPC:
        result, err = _read_column_statistics_arrow_ipc(input.data, input.columns)
    else:
        return ReadColumnStatisticsResult(
            error=invalid_argument("format must be FILE_FORMAT_PARQUET or FILE_FORMAT_ARROW_IPC")
        )

    if err is not None:
        return ReadColumnStatisticsResult(error=err)
    return result
