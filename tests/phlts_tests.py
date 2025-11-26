import pytest
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
# from datetime import datetime, timedelta
# from zoneinfo import ZoneInfo
import tempfile
import shutil

from app import connect_to_sql

@pytest.fixture
def dummy_df():
    # Define columns
    cols = ['id', 'timestamp', 'type', 'status', 'domain', 'client', 'forward',
        'additional_info', 'reply_type', 'reply_time', 'dnssec', 'list_id',
        'ede']


    def make_dummy_df(seed):
        rng = np.random.default_rng(seed)
        n = 44641
        end = pd.Timestamp.today(tz="UTC")            # today in UTC
        start = end - pd.DateOffset(months=1)         # one month before today
        all_ts = pd.date_range(start, end, freq="60s")
        timestamps = rng.choice(all_ts, n)
        timestamps_unix = pd.to_datetime(timestamps).astype('int64') // 1_000_000_000
        allowed_statuses = [2, 3, 12, 13, 14, 17]
        blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
        status = rng.choice(allowed_statuses + blocked_statuses, n)
        domain_indices = rng.integers(1, 6, n)  # i in 1..5
        domain = [
            f"www.{'alloweddomain' if s in allowed_statuses else 'blockeddomain'}{seed}_{i}.com"
            for s, i in zip(status, domain_indices)
        ]

        df = pd.DataFrame({
            'id': range(1, n+1),
            'timestamp': timestamps_unix,
            'type': rng.choice([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16], n),
            'status': status,
            'domain': domain,
            'client': rng.choice(['192.168.1.2','192.168.1.3','192.168.1.4','192.168.1.5','192.168.1.6'], n),
            'forward': rng.choice(['8.8.8.8','1.1.1.1','None','127.0.0.1#5335'], n),
            'additional_info': rng.choice(['53','-20','-10','None'], n),
            'reply_type': rng.choice([0,1,2,3,4,5,6,7,8,9,10,11,12,13], n),
            'reply_time': rng.random(n)*50,
            'dnssec': rng.choice([0,1,2,3,4,5], n),
            'list_id': rng.choice(['53','-20','-10','None'], n),
            'ede': rng.choice([0,5,15], n),
        })
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
    
    yield str(out1), str(out2)
    

    shutil.rmtree(temp_dir)

class TestDatabaseConnection:
    def test_connect_existing_database(self,dummy_df):
        """Test connection to valid db path"""
        db1,_ = dummy_df
        conn = connect_to_sql(db1)
        assert conn is not None
        conn.close()
    
    def test_connect_to_nonexistent_db(self):
        """Test connection to valid db path"""
        with pytest.raises(FileNotFoundError):
            connect_to_sql("123non_existent_ftl123.db")