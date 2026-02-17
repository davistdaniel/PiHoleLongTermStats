import pytest
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import shutil
from piholelongtermstats.process import preprocess_df

@pytest.fixture(scope="session")
def temp_dir():
    """Create a temporary directory for test databases."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


def create_dummy_dataframe(seed=1, n_rows=1000, start_date=None, end_date=None):
    """
    Create a dummy DataFrame with PiHole FTL database structure.
    
    Args:
        seed: Random seed for reproducibility
        n_rows: Number of rows to generate
        start_date: Start date for timestamps (defaults to 30 days ago)
        end_date: End date for timestamps (defaults to today)
    
    Returns:
        DataFrame with columns matching PiHole FTL queries table
    """
    rng = np.random.default_rng(seed)
    
    # Define date range
    if end_date is None:
        end_date = pd.Timestamp.today(tz="UTC").normalize()
    if start_date is None:
        start_date = end_date - pd.DateOffset(days=30)
    
    unix_start = int(start_date.timestamp())
    unix_end = int(end_date.timestamp())
    

    timestamps_unix = np.linspace(unix_start, unix_end, n_rows)
    timestamps_unix = np.sort(timestamps_unix)
    
    allowed_statuses = [2, 3, 12, 13, 14, 17]
    blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
    status = rng.choice(allowed_statuses + blocked_statuses, n_rows)
    
    domain_indices = rng.integers(1, 11, n_rows)
    domain = [
        f"www.{'alloweddomain' if s in allowed_statuses else 'blockeddomain'}{seed}_{i}.com"
        for s, i in zip(status, domain_indices)
    ]
    
    df = pd.DataFrame(
        {
            "id": range(1, n_rows + 1),
            "timestamp": timestamps_unix,
            "type": rng.choice(
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16], n_rows
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
                n_rows,
            ),
            "forward": rng.choice(
                ["8.8.8.8", "1.1.1.1", "None", "127.0.0.1#5335"], n_rows
            ),
            "additional_info": rng.choice(["53", "-20", "-10", "None"], n_rows),
            "reply_type": rng.choice(
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], n_rows
            ),
            "reply_time": rng.random(n_rows) * 50,
            "dnssec": rng.choice([0, 1, 2, 3, 4, 5], n_rows),
            "list_id": rng.choice(["53", "-20", "-10", "None"], n_rows),
            "ede": rng.choice([0, 5, 15], n_rows),
        }
    )
    return df


def create_test_database(df, db_path):
    """Create a SQLite database from a DataFrame."""
    conn = sqlite3.connect(db_path)
    df.to_sql("queries", conn, index=False, if_exists="replace")
    conn.close()
    return db_path


@pytest.fixture(scope="session")
def dummy_db_single(temp_dir):
    """Create a single test database."""
    df = create_dummy_dataframe(seed=1, n_rows=5000)
    db_path = Path(temp_dir) / "test_ftl_single.db"
    create_test_database(df, str(db_path))
    yield str(db_path), df


@pytest.fixture(scope="session")
def dummy_db_multiple(temp_dir):
    """Create multiple test databases."""
    df1 = create_dummy_dataframe(seed=1, n_rows=3000)
    df2 = create_dummy_dataframe(seed=2, n_rows=2000)
    
    db_path1 = Path(temp_dir) / "test_ftl1.db"
    db_path2 = Path(temp_dir) / "test_ftl2.db"
    
    create_test_database(df1, str(db_path1))
    create_test_database(df2, str(db_path2))
    
    yield [str(db_path1), str(db_path2)], [df1, df2]


@pytest.fixture(scope="session")
def dummy_db_empty(temp_dir):
    """Create an empty test database."""
    db_path = Path(temp_dir) / "test_ftl_empty.db"
    conn = sqlite3.connect(str(db_path))
    # Create table structure but no data
    conn.execute("""
        CREATE TABLE queries (
            id INTEGER,
            timestamp INTEGER,
            type INTEGER,
            status INTEGER,
            domain TEXT,
            client TEXT,
            forward TEXT,
            additional_info TEXT,
            reply_type INTEGER,
            reply_time REAL,
            dnssec INTEGER,
            list_id TEXT,
            ede INTEGER
        )
    """)
    conn.commit()
    conn.close()
    yield str(db_path)


@pytest.fixture(scope="session")
def dummy_db_date_range(temp_dir):
    """Create a database with specific date range."""
    start_date = pd.Timestamp("2024-01-01", tz="UTC")
    end_date = pd.Timestamp("2024-01-31", tz="UTC")
    df = create_dummy_dataframe(seed=3, n_rows=2000, start_date=start_date, end_date=end_date)
    db_path = Path(temp_dir) / "test_ftl_date_range.db"
    create_test_database(df, str(db_path))
    yield str(db_path), df


@pytest.fixture
def sample_dataframe():
    """Create a small sample DataFrame for quick tests."""
    return create_dummy_dataframe(seed=42, n_rows=100)


@pytest.fixture
def preprocessed_dataframe(sample_dataframe):
    """Create a preprocessed DataFrame for testing stats and plots."""
    from piholelongtermstats.process import preprocess_df
    return preprocess_df(sample_dataframe.copy(), timezone="UTC")

@pytest.fixture
def plot_dataframe():
    """Create a dataframe suitable for plot generation."""
    rng = np.random.default_rng(123)
    n_rows = 1000

    start = pd.Timestamp("2024-01-01", tz="UTC")
    timestamps = pd.date_range(start, periods=n_rows, freq="10min")

    allowed_statuses = [2, 3, 12, 13, 14, 17]
    blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
    status = rng.choice(allowed_statuses + blocked_statuses, n_rows)

    domains = [f"domain{i % 20}.com" for i in range(n_rows)]
    
    clients = [f"192.168.1.{i % 5 + 1}" for i in range(n_rows)]
    
    df = pd.DataFrame({
        "id": range(1, n_rows + 1),
        "timestamp": [int(ts.timestamp()) for ts in timestamps],
        "type": rng.choice([1, 2, 3, 4], n_rows),
        "status": status,
        "domain": domains,
        "client": clients,
        "reply_time": rng.random(n_rows) * 50,
    })
    
    return preprocess_df(df, timezone="UTC")