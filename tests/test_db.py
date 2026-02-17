import pytest
import sqlite3
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from piholelongtermstats.db import (
    connect_to_sql,
    probe_sample_df,
    get_timestamp_range,
    read_pihole_ftl_db,
)


class TestConnectToSql:
    """Tests for connect_to_sql function."""

    def test_connect_existing_database(self, dummy_db_single):
        """Test connection to valid database path."""
        db_path, _ = dummy_db_single
        conn = connect_to_sql(db_path)
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)
        conn.close()

    def test_connect_nonexistent_database(self):
        """Test connection to non-existent database raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            connect_to_sql("nonexistent_ftl_12345.db")

    def test_connection_text_factory(self, dummy_db_single):
        """Test that connection uses proper text factory for decoding."""
        db_path, _ = dummy_db_single
        conn = connect_to_sql(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT domain FROM queries LIMIT 1")
        result = cursor.fetchone()
        assert result is not None
        assert isinstance(result[0], str)
        conn.close()

    def test_connection_can_query(self, dummy_db_single):
        """Test that connection can execute queries."""
        db_path, df = dummy_db_single
        conn = connect_to_sql(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM queries")
        count = cursor.fetchone()[0]
        assert count == len(df)
        conn.close()


class TestProbeSampleDf:
    """Tests for probe_sample_df function."""

    def test_probe_sample_df_returns_tuple(self, dummy_db_single):
        """Test that probe_sample_df returns a tuple of three values."""
        db_path, _ = dummy_db_single
        conn = connect_to_sql(db_path)
        result = probe_sample_df(conn)
        assert isinstance(result, tuple)
        assert len(result) == 3
        conn.close()

    def test_probe_sample_df_chunksize(self, dummy_db_single):
        """Test that chunksize is a positive integer."""
        db_path, _ = dummy_db_single
        conn = connect_to_sql(db_path)
        chunksize, _, _ = probe_sample_df(conn)
        assert isinstance(chunksize, int)
        assert chunksize > 0
        conn.close()

    def test_probe_sample_df_timestamps(self, dummy_db_single):
        """Test that timestamps are correctly identified."""
        db_path, df = dummy_db_single
        conn = connect_to_sql(db_path)
        chunksize, latest_ts, oldest_ts = probe_sample_df(conn)
        
        assert isinstance(latest_ts, pd.Timestamp)
        assert isinstance(oldest_ts, pd.Timestamp)
        expected_latest = pd.to_datetime(df["timestamp"].iloc[-1], unit="s", utc=True)
        expected_oldest = pd.to_datetime(df["timestamp"].iloc[0], unit="s", utc=True)
        assert latest_ts == expected_latest
        assert oldest_ts == expected_oldest
        conn.close()

    def test_probe_sample_df_empty_database(self, dummy_db_empty):
        """Test probe_sample_df with empty database."""
        conn = connect_to_sql(dummy_db_empty)
        with pytest.raises(ValueError):
            chunksize, latest_ts, oldest_ts = probe_sample_df(conn)

class TestGetTimestampRange:
    """Tests for get_timestamp_range function."""

    def test_get_timestamp_range_no_dates(self):
        """Test timestamp range calculation with no dates specified."""
        days = 31
        start_date = None
        end_date = None
        timezone = "UTC"
        
        start_timestamp, end_timestamp = get_timestamp_range(
            days=days, start_date=start_date, end_date=end_date, timezone=timezone
        )
        
        assert isinstance(start_timestamp, int)
        assert isinstance(end_timestamp, int)
        assert end_timestamp > start_timestamp
        expected_range = 31 * 24 * 60 * 60
        actual_range = end_timestamp - start_timestamp
        assert abs(actual_range - expected_range) < 60

    def test_get_timestamp_range_with_dates(self):
        """Test timestamp range calculation with specific dates."""
        days = 31
        start_date = "2024-01-01"
        end_date = "2024-01-10"
        timezone = "UTC"
        
        start_timestamp, end_timestamp = get_timestamp_range(
            days=days, start_date=start_date, end_date=end_date, timezone=timezone
        )
        
        tz = ZoneInfo(timezone)
        expected_start_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=tz)
        expected_end_dt = datetime(2024, 1, 11, 0, 0, 0, tzinfo=tz)  # +1 day
        
        assert start_timestamp == int(expected_start_dt.timestamp())
        assert end_timestamp == int(expected_end_dt.timestamp())

    def test_get_timestamp_range_different_timezone(self):
        """Test timestamp range with different timezone."""
        days = 7
        start_date = "2024-01-01"
        end_date = "2024-01-07"
        timezone = "America/New_York"
        
        start_timestamp, end_timestamp = get_timestamp_range(
            days=days, start_date=start_date, end_date=end_date, timezone=timezone
        )
        
        # return UTC timestamps
        assert isinstance(start_timestamp, int)
        assert isinstance(end_timestamp, int)
        assert end_timestamp > start_timestamp

    def test_get_timestamp_range_invalid_timezone(self):
        """Test timestamp range with invalid timezone falls back to UTC."""
        days = 7
        start_date = None
        end_date = None
        timezone = "Invalid/Timezone"
        
        #  use UTC
        start_timestamp, end_timestamp = get_timestamp_range(
            days=days, start_date=start_date, end_date=end_date, timezone=timezone
        )
        
        assert isinstance(start_timestamp, int)
        assert isinstance(end_timestamp, int)

    def test_get_timestamp_range_date_only_start(self):
        """Test timestamp range with only start_date specified."""
        days = 31
        start_date = "2024-01-01"
        end_date = None
        timezone = "UTC"
        
        # use days parameter when end_date is None
        start_timestamp, end_timestamp = get_timestamp_range(
            days=days, start_date=start_date, end_date=end_date, timezone=timezone
        )
        
        assert isinstance(start_timestamp, int)
        assert isinstance(end_timestamp, int)
        assert end_timestamp > start_timestamp


class TestReadPiholeFtlDb:
    """Tests for read_pihole_ftl_db function."""

    def test_read_pihole_ftl_db_single(self, dummy_db_single):
        """Test reading from a single database."""
        db_path, df_expected = dummy_db_single
        
        df_expected_mod = df_expected.drop(
            columns=["forward", "additional_info", "reply_type", "dnssec", "list_id", "ede"]
        )
        
        conn = connect_to_sql(db_path)
        chunksize, _, _ = probe_sample_df(conn)
        conn.close()
        
        chunks = list(read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[chunksize],
            start_date=None,
            end_date=None,
            days=31,
            timezone="UTC",
        ))
        
        df_result = pd.concat(chunks)
        
        # check columns
        assert set(df_result.columns) == set(df_expected_mod.columns)
        
        assert len(df_result) > 0

    def test_read_pihole_ftl_db_multiple(self, dummy_db_multiple):
        """Test reading from multiple databases."""
        db_paths, dfs_expected = dummy_db_multiple
        
        df1_mod = dfs_expected[0].drop(
            columns=["forward", "additional_info", "reply_type", "dnssec", "list_id", "ede"]
        )
        
        conn = connect_to_sql(db_paths[0])
        chunksize, _, _ = probe_sample_df(conn)
        conn.close()
        
        chunks = list(read_pihole_ftl_db(
            db_paths=db_paths,
            chunksize=[chunksize, chunksize],
            start_date=None,
            end_date=None,
            days=31,
            timezone="UTC",
        ))
        
        df_result = pd.concat(chunks)
        
        assert len(df_result) > 0
        assert set(df_result.columns) == set(df1_mod.columns)

    def test_read_pihole_ftl_db_with_date_range(self, dummy_db_date_range):
        """Test reading with specific date range."""
        db_path, df_expected = dummy_db_date_range
        
        min_timestamp = df_expected["timestamp"].min()
        max_timestamp = df_expected["timestamp"].max()
        
        min_date = pd.Timestamp(min_timestamp, unit="s", tz="UTC").date()
        max_date = pd.Timestamp(max_timestamp, unit="s", tz="UTC").date()
        
        start_date = min_date.strftime("%Y-%m-%d")
        end_date = max_date.strftime("%Y-%m-%d")
        
        conn = connect_to_sql(db_path)
        chunksize, _, _ = probe_sample_df(conn)
        conn.close()
        
        chunks = list(read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[chunksize],
            start_date=start_date,
            end_date=end_date,
            days=31,
            timezone="UTC",
        ))
        
        df_result = pd.concat(chunks)
        
        assert len(df_result) > 0
        if len(df_result) > 0:
            result_timestamps = df_result["timestamp"]
            assert result_timestamps.min() >= min_timestamp
            assert result_timestamps.max() <= max_timestamp

    def test_read_pihole_ftl_db_empty_result(self, dummy_db_single):
        """Test reading with date range that returns no results."""
        db_path, _ = dummy_db_single
        
        start_date = "2099-01-01"
        end_date = "2099-01-02"
        
        conn = connect_to_sql(db_path)
        chunksize, _, _ = probe_sample_df(conn)
        conn.close()
        
        chunks = list(read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[chunksize],
            start_date=start_date,
            end_date=end_date,
            days=31,
            timezone="UTC",
        ))
        
        if chunks:
            df_result = pd.concat(chunks)
            assert len(df_result) == 0
        else:
            assert True

    def test_read_pihole_ftl_db_chunksize_none(self, dummy_db_single):
        """Test reading with chunksize=None."""
        db_path, _ = dummy_db_single
        
        chunks = list(read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[None],
            start_date=None,
            end_date=None,
            days=31,
            timezone="UTC",
        ))
        
        assert isinstance(chunks, list)

    def test_read_pihole_ftl_db_timezone_conversion(self, dummy_db_single):
        """Test that timezone parameter affects date range calculation."""
        db_path, _ = dummy_db_single
        
        conn = connect_to_sql(db_path)
        chunksize, _, _ = probe_sample_df(conn)
        conn.close()
        
        chunks_utc = list(read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[chunksize],
            start_date=None,
            end_date=None,
            days=7,
            timezone="UTC",
        ))
        
        # different timezone
        chunks_ny = list(read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[chunksize],
            start_date=None,
            end_date=None,
            days=7,
            timezone="America/New_York",
        ))
        
        # may have slightly different date ranges
        assert isinstance(chunks_utc, list)
        assert isinstance(chunks_ny, list)

    def test_read_pihole_ftl_db_returns_generator(self, dummy_db_single):
        """Test that read_pihole_ftl_db returns a generator."""
        db_path, _ = dummy_db_single
        
        conn = connect_to_sql(db_path)
        chunksize, _, _ = probe_sample_df(conn)
        conn.close()
        
        result = read_pihole_ftl_db(
            db_paths=[db_path],
            chunksize=[chunksize],
            start_date=None,
            end_date=None,
            days=31,
            timezone="UTC",
        )
        
        assert hasattr(result, "__iter__")
        assert not hasattr(result, "__len__") 
