import pyarrow as pa
import pyarrow.parquet as pq

from gen.messages_pb2 import (
    FileFormat,
    ReadFileMetadataRequest,
    ReadFileMetadataResult,
    RowGroupSummary,
)
from gen.axiom_context import AxiomContext
from nodes._helpers import (
    MAX_ROW_GROUPS_RETURNED,
    check_input_size,
    invalid_argument,
    open_arrow_ipc,
    parse_error,
)


def read_file_metadata(ax: AxiomContext, input: ReadFileMetadataRequest) -> ReadFileMetadataResult:
    """Read a Parquet or Arrow IPC file's file- and row-group-level
    metadata: total row/column counts, row-group (or Arrow-IPC
    record-batch) count, format version string, Parquet's created_by
    footer string, footer serialized size, and a per-row-group summary
    capped at 1000 entries with a truncation flag. Malformed input returns
    a structured error rather than crashing.
    """
    size_err = check_input_size(input.data)
    if size_err is not None:
        return ReadFileMetadataResult(error=size_err)

    if input.format == FileFormat.FILE_FORMAT_PARQUET:
        try:
            pf = pq.ParquetFile(pa.BufferReader(input.data))
        except Exception as e:
            return ReadFileMetadataResult(error=parse_error(f"malformed Parquet file: {e}"))
        md = pf.metadata
        row_groups = []
        truncated = md.num_row_groups > MAX_ROW_GROUPS_RETURNED
        for i in range(min(md.num_row_groups, MAX_ROW_GROUPS_RETURNED)):
            rg = md.row_group(i)
            total_compressed = sum(rg.column(j).total_compressed_size for j in range(rg.num_columns))
            row_groups.append(
                RowGroupSummary(
                    index=i,
                    num_rows=rg.num_rows,
                    total_byte_size=rg.total_byte_size,
                    total_compressed_size=total_compressed,
                )
            )
        return ReadFileMetadataResult(
            num_rows=md.num_rows,
            num_columns=md.num_columns,
            num_row_groups=md.num_row_groups,
            format_version=md.format_version,
            created_by=md.created_by or "",
            serialized_size=md.serialized_size,
            row_groups=row_groups,
            row_groups_truncated=truncated,
        )

    if input.format == FileFormat.FILE_FORMAT_ARROW_IPC:
        try:
            opened = open_arrow_ipc(input.data)
        except Exception as e:
            return ReadFileMetadataResult(error=parse_error(str(e)))

        if opened.kind == "arrow_ipc_file":
            num_batches = opened.reader.num_record_batches
            truncated = num_batches > MAX_ROW_GROUPS_RETURNED
            row_groups = []
            total_rows = 0
            for i in range(num_batches):
                batch = opened.reader.get_batch(i)
                if i < MAX_ROW_GROUPS_RETURNED:
                    row_groups.append(
                        RowGroupSummary(
                            index=i,
                            num_rows=batch.num_rows,
                            total_byte_size=batch.nbytes,
                            total_compressed_size=0,
                        )
                    )
                total_rows += batch.num_rows
        else:
            # Streaming framing has no random-access batch index; iterate
            # once, one batch at a time (never holding the whole file in
            # memory), just to total the row count.
            num_batches = 0
            total_rows = 0
            row_groups = []
            truncated = False
            for batch in opened.reader:
                total_rows += batch.num_rows

        return ReadFileMetadataResult(
            num_rows=total_rows,
            num_columns=len(opened.reader.schema),
            num_row_groups=num_batches,
            format_version=opened.kind,
            created_by="",
            serialized_size=0,
            row_groups=row_groups,
            row_groups_truncated=truncated,
        )

    return ReadFileMetadataResult(
        error=invalid_argument("format must be FILE_FORMAT_PARQUET or FILE_FORMAT_ARROW_IPC")
    )
