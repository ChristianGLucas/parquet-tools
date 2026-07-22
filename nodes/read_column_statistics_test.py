from gen.messages_pb2 import FileFormat, ReadColumnStatisticsRequest
from nodes.read_column_statistics import read_column_statistics
from nodes._test_fixtures import FakeAxiomContext, arrow_ipc_file_bytes, parquet_bytes

# Independent, hand-computed oracle values derived directly from the fixture
# literals in nodes/_test_fixtures.py (IDS/NAMES/SCORES), not from reading
# the file back through pyarrow.
EXPECTED = {
    "id": {"null_count": 1, "min": "1", "max": "5"},
    "name": {"null_count": 0, "min": "alice", "max": "eve"},
    "score": {"null_count": 1, "min": "10.5", "max": "50.0"},
}


def test_read_column_statistics_parquet_single_row_group_oracle():
    ax = FakeAxiomContext()
    result = read_column_statistics(
        ax,
        ReadColumnStatisticsRequest(data=parquet_bytes(), format=FileFormat.FILE_FORMAT_PARQUET),
    )
    assert result.error.code == ""
    assert result.num_rows == 5
    by_name = {c.name: c for c in result.columns}
    assert set(by_name) == {"id", "name", "score"}
    for name, expected in EXPECTED.items():
        col = by_name[name]
        assert col.null_count == expected["null_count"], name
        assert col.has_min_max is True, name
        assert col.min_value == expected["min"], name
        assert col.max_value == expected["max"], name
        assert col.compression == "SNAPPY", name
        assert col.total_uncompressed_size > 0, name
        assert len(col.encodings) > 0, name
    assert by_name["id"].physical_type == "INT64"
    assert by_name["score"].physical_type == "DOUBLE"


def test_read_column_statistics_parquet_multi_row_group_aggregates():
    ax = FakeAxiomContext()
    # 2 row groups (rows 0-2, 3-4) -- aggregation across groups must still
    # match the same whole-file oracle values as the single-row-group case.
    result = read_column_statistics(
        ax,
        ReadColumnStatisticsRequest(
            data=parquet_bytes(row_group_size=3), format=FileFormat.FILE_FORMAT_PARQUET
        ),
    )
    assert result.error.code == ""
    by_name = {c.name: c for c in result.columns}
    assert by_name["id"].null_count == 1
    assert by_name["id"].min_value == "1"
    assert by_name["id"].max_value == "5"
    assert by_name["score"].null_count == 1
    assert by_name["score"].min_value == "10.5"
    assert by_name["score"].max_value == "50.0"


def test_read_column_statistics_requested_columns_subset():
    ax = FakeAxiomContext()
    result = read_column_statistics(
        ax,
        ReadColumnStatisticsRequest(
            data=parquet_bytes(), format=FileFormat.FILE_FORMAT_PARQUET, columns=["name"]
        ),
    )
    assert result.error.code == ""
    assert [c.name for c in result.columns] == ["name"]


def test_read_column_statistics_unknown_column_is_error():
    ax = FakeAxiomContext()
    result = read_column_statistics(
        ax,
        ReadColumnStatisticsRequest(
            data=parquet_bytes(), format=FileFormat.FILE_FORMAT_PARQUET, columns=["nope"]
        ),
    )
    assert result.error.code == "INVALID_ARGUMENT"


def test_read_column_statistics_arrow_ipc_oracle():
    ax = FakeAxiomContext()
    result = read_column_statistics(
        ax,
        ReadColumnStatisticsRequest(
            data=arrow_ipc_file_bytes(), format=FileFormat.FILE_FORMAT_ARROW_IPC
        ),
    )
    assert result.error.code == ""
    by_name = {c.name: c for c in result.columns}
    assert by_name["id"].null_count == 1
    assert by_name["id"].min_value == "1"
    assert by_name["id"].max_value == "5"
    # Arrow IPC has no footer-level stats; distinct_count/compression are
    # documented as always absent.
    assert by_name["id"].distinct_count == -1
    assert by_name["id"].compression == ""
    assert list(by_name["id"].encodings) == []


def test_read_column_statistics_error_path():
    ax = FakeAxiomContext()
    result = read_column_statistics(
        ax, ReadColumnStatisticsRequest(data=b"garbage", format=FileFormat.FILE_FORMAT_PARQUET)
    )
    assert result.error.code == "PARSE_ERROR"
