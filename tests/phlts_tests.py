import pytest
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import tempfile
import shutil
import pandas.testing as pdt

from app import (
    connect_to_sql,
    probe_sample_df,
    get_timestamp_range,
    read_pihole_ftl_db,
    _is_valid_regex,
    regex_ignore_domains,
)


@pytest.fixture(scope="session")
def dummy_df():
    # Define columns
    # cols = ['id', 'timestamp', 'type', 'status', 'domain', 'client', 'forward',
    #     'additional_info', 'reply_type', 'reply_time', 'dnssec', 'list_id',
    #     'ede']

    def make_dummy_df(seed):
        rng = np.random.default_rng(seed)
        n = 44641
        # define start/end
        today = pd.Timestamp.today(tz="UTC").normalize()
        previous_month = today - pd.DateOffset(month=today.month - 1)
        unix_today = int(today.timestamp())
        unix_previous_month = int(previous_month.timestamp())
        timestamps_unix = np.linspace(unix_previous_month, unix_today, n)
        timestamps_unix = np.sort(timestamps_unix)
        allowed_statuses = [2, 3, 12, 13, 14, 17]
        blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
        status = rng.choice(allowed_statuses + blocked_statuses, n)
        domain_indices = rng.integers(1, 6, n)  # i in 1..5
        domain = [
            f"www.{'alloweddomain' if s in allowed_statuses else 'blockeddomain'}{seed}_{i}.com"
            for s, i in zip(status, domain_indices)
        ]

        df = pd.DataFrame(
            {
                "id": range(1, n + 1),
                "timestamp": timestamps_unix,
                "type": rng.choice(
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16], n
                ),
                "status": status,
                "domain": domain,
                "client": rng.choice(
                    [
                        "192.168.1.2",
                        "192.168.1.3",
                        "192.168.1.4",
                        "192.168.1.5",
                        "192.168.1.6",
                    ],
                    n,
                ),
                "forward": rng.choice(
                    ["8.8.8.8", "1.1.1.1", "None", "127.0.0.1#5335"], n
                ),
                "additional_info": rng.choice(["53", "-20", "-10", "None"], n),
                "reply_type": rng.choice(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], n
                ),
                "reply_time": rng.random(n) * 50,
                "dnssec": rng.choice([0, 1, 2, 3, 4, 5], n),
                "list_id": rng.choice(["53", "-20", "-10", "None"], n),
                "ede": rng.choice([0, 5, 15], n),
            }
        )
        return df

    df1 = make_dummy_df(1)
    df2 = make_dummy_df(2)

    temp_dir = tempfile.mkdtemp()
    out1 = Path(temp_dir) / "test_ftl1.db"
    out2 = Path(temp_dir) / "test_ftl2.db"

    for df, path in [(df1, out1), (df2, out2)]:
        conn = sqlite3.connect(path)
        df.to_sql("queries", conn, index=False)
        conn.close()

    yield str(out1), str(out2), df1, df2

    shutil.rmtree(temp_dir)


def test_connect_existing_database(dummy_df):
    """Test connection to valid db path"""
    db1, _, _, _ = dummy_df
    conn = connect_to_sql(db1)
    assert conn is not None
    conn.close()


def test_connect_to_nonexistent_db():
    """Test connection to valid db path"""
    with pytest.raises(FileNotFoundError):
        connect_to_sql("123non_existent_ftl123.db")


def test_probe_sample_df(dummy_df):
    db1, _, df1, _ = dummy_df
    conn = connect_to_sql(db1)
    chunksize, latest_ts, oldest_ts = probe_sample_df(conn)
    assert isinstance(chunksize, int)
    assert chunksize > 0
    assert str(latest_ts) == str(
        pd.to_datetime(df1["timestamp"].iloc[-1], unit="s", utc=True)
    )
    assert str(oldest_ts) == str(
        pd.to_datetime(df1["timestamp"].iloc[0], unit="s", utc=True)
    )
    conn.close()


def test_no_data_range():
    days = 31
    start_date = None
    end_date = None
    timezone = "UTC"
    tz = ZoneInfo(timezone)
    expected_end_dt = datetime.now(tz)
    expected_start_dt = expected_end_dt - timedelta(days=days)

    start_timestamp, end_timestamp = get_timestamp_range(
        days=days, start_date=start_date, end_date=end_date, timezone=timezone
    )

    assert end_timestamp == int(expected_end_dt.astimezone(ZoneInfo("UTC")).timestamp())
    assert start_timestamp == int(
        expected_start_dt.astimezone(ZoneInfo("UTC")).timestamp()
    )


def test_with_date_range():
    days = 31
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    timezone = "UTC"

    start_timestamp, end_timestamp = get_timestamp_range(
        days=days, start_date=start_date, end_date=end_date, timezone=timezone
    )

    tz = ZoneInfo(timezone)
    expected_start_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=tz)
    expected_end_dt = datetime(2024, 1, 11, 0, 0, 0, tzinfo=tz)

    assert start_timestamp == int(expected_start_dt.timestamp())
    assert end_timestamp == int(expected_end_dt.timestamp())


def test_read_pihole_ftl_db_single_df(dummy_df):
    db1, _, df1, _ = dummy_df
    df1_mod = df1.drop(
        columns=["forward", "additional_info", "reply_type", "dnssec", "list_id", "ede"]
    )
    conn = connect_to_sql(db1)
    chunksize, _, _ = probe_sample_df(conn)
    df = pd.concat(
        read_pihole_ftl_db(
            db_paths=[db1],
            chunksize=[chunksize],
            start_date=None,
            end_date=None,
            days=31,
            timezone="UTC",
        )
    )
    conn.close()
    pdt.assert_frame_equal(df, df1_mod, check_dtype=False, check_like=True)


def test_read_pihole_ftl_db_multiple_df(dummy_df):
    db1, db2, df1, df2 = dummy_df
    df1_mod = df1.drop(
        columns=["forward", "additional_info", "reply_type", "dnssec", "list_id", "ede"]
    )
    df2_mod = df2.drop(
        columns=["forward", "additional_info", "reply_type", "dnssec", "list_id", "ede"]
    )
    db_paths = [db1, db2]
    conn = connect_to_sql(db1)
    chunksize, _, _ = probe_sample_df(conn)
    conn.close()
    df = pd.concat(
        read_pihole_ftl_db(
            db_paths=db_paths,
            chunksize=[chunksize, chunksize],
            start_date=None,
            end_date=None,
            days=31,
            timezone="UTC",
        )
    )
    pdt.assert_frame_equal(
        df, pd.concat((df1_mod, df2_mod)), check_dtype=False, check_like=True
    )


def test_read_pihole_ftl_db_with_date_range(dummy_df):
    db1, _, df1, _ = dummy_df
    df1_mod = df1.drop(
        columns=["forward", "additional_info", "reply_type", "dnssec", "list_id", "ede"]
    )

    min_timestamp = df1["timestamp"].min()
    max_timestamp = df1["timestamp"].max()

    min_date = pd.Timestamp(min_timestamp, unit="s", tz="UTC").date()
    max_date = pd.Timestamp(max_timestamp, unit="s", tz="UTC").date()

    start_date = min_date.strftime("%Y-%m-%d")
    end_date = max_date.strftime("%Y-%m-%d")

    conn = connect_to_sql(db1)
    chunksize, _, _ = probe_sample_df(conn)
    conn.close()

    df = pd.concat(
        read_pihole_ftl_db(
            db_paths=[db1],
            chunksize=[chunksize],
            start_date=start_date,
            end_date=end_date,
            days=31,
            timezone="UTC",
        )
    )

    pdt.assert_frame_equal(df, df1_mod, check_dtype=False, check_like=True)


def test_is_valid_regex():
    assert _is_valid_regex("*test") is False
    assert _is_valid_regex(".*\.local") is True


def test_regex_ignore_domains(dummy_df):
    _, _, df1, _ = dummy_df

    df_test1 = regex_ignore_domains(df1, "*test")  # should return the df unchanged
    df_test2 = regex_ignore_domains(df1, ".*blocked.*")
    mask = df1["domain"].str.contains(".*blocked.*", regex=True, na=False)
    df1_expected = df1[~mask].reset_index(drop=True)

    pdt.assert_frame_equal(df1, df_test1, check_dtype=False, check_like=True)
    pdt.assert_frame_equal(df1_expected, df_test2, check_dtype=False, check_like=True)
