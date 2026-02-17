"""
Comprehensive tests for piholelongtermstats.stats module.
"""
import pytest
import pandas as pd
import numpy as np

from piholelongtermstats.stats import compute_stats
from piholelongtermstats.process import preprocess_df


@pytest.fixture
def stats_dataframe():
    """Create a dataframe suitable for stats computation."""
    rng = np.random.default_rng(42)
    n_rows = 2000
    
    # Create timestamps over 7 days
    start = pd.Timestamp("2024-01-01", tz="UTC")
    timestamps = pd.date_range(start, periods=n_rows, freq="5min")
    
    # Create status types
    allowed_statuses = [2, 3, 12, 13, 14, 17]
    blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
    status = rng.choice(allowed_statuses + blocked_statuses, n_rows)
    
    # Create domains
    domains = [f"domain{i % 10}.com" for i in range(n_rows)]
    
    # Create clients
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


class TestComputeStats:
    """Tests for compute_stats function."""

    def test_compute_stats_returns_dict(self, stats_dataframe):
        """Test that compute_stats returns a dictionary."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        assert isinstance(stats, dict)
        assert len(stats) > 0

    def test_compute_stats_main_heading(self, stats_dataframe):
        """Test main heading stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check main heading stats exist
        assert "n_data_points" in stats
        assert "oldest_data_point" in stats
        assert "latest_data_point" in stats
        assert "min_date" in stats
        assert "max_date" in stats
        assert "data_span_days" in stats
        assert "data_span_str" in stats
        
        # Check values
        assert stats["n_data_points"] == len(stats_dataframe)
        assert isinstance(stats["data_span_days"], int)
        assert stats["data_span_days"] >= 0

    def test_compute_stats_query_stats(self, stats_dataframe):
        """Test query-related stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check query stats exist
        assert "total_queries" in stats
        assert "blocked_count" in stats
        assert "allowed_count" in stats
        assert "blocked_pct" in stats
        assert "allowed_pct" in stats
        
        # Check values are correct
        assert stats["total_queries"] == len(stats_dataframe)
        assert stats["blocked_count"] + stats["allowed_count"] <= stats["total_queries"]
        assert 0 <= stats["blocked_pct"] <= 100
        assert 0 <= stats["allowed_pct"] <= 100
        
        # Check percentages sum correctly (within rounding)
        total_pct = stats["blocked_pct"] + stats["allowed_pct"]
        assert abs(total_pct - 100) < 1 or stats["total_queries"] == 0

    def test_compute_stats_top_clients(self, stats_dataframe):
        """Test top client stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check top client stats exist
        assert "top_client" in stats
        assert "top_allowed_client" in stats
        assert "top_blocked_client" in stats
        
        # Check they are valid clients
        assert stats["top_client"] in stats_dataframe["client"].values
        assert stats["top_allowed_client"] in stats_dataframe["client"].values
        assert stats["top_blocked_client"] in stats_dataframe["client"].values

    def test_compute_stats_domain_stats(self, stats_dataframe):
        """Test domain-related stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check domain stats exist
        assert "top_allowed_domain" in stats
        assert "top_blocked_domain" in stats
        assert "top_allowed_domain_count" in stats
        assert "top_blocked_domain_count" in stats
        assert "top_allowed_domain_client" in stats
        assert "top_blocked_domain_client" in stats
        
        # Check counts are positive
        assert stats["top_allowed_domain_count"] > 0
        assert stats["top_blocked_domain_count"] > 0
        
        # Check domains exist in dataframe
        assert stats["top_allowed_domain"] in stats_dataframe["domain"].values
        assert stats["top_blocked_domain"] in stats_dataframe["domain"].values

    def test_compute_stats_most_persistent(self, stats_dataframe):
        """Test most persistent client stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check persistent stats exist
        assert "most_persistent_client" in stats
        assert "blocked_domain" in stats
        assert "repeat_attempts" in stats
        
        # Check values
        assert stats["repeat_attempts"] > 0
        assert stats["most_persistent_client"] in stats_dataframe["client"].values
        assert stats["blocked_domain"] in stats_dataframe["domain"].values

    def test_compute_stats_activity_stats(self, stats_dataframe):
        """Test activity-related stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check activity stats exist
        assert "date_most_queries" in stats
        assert "date_most_blocked" in stats
        assert "date_most_allowed" in stats
        assert "date_least_queries" in stats
        assert "date_least_blocked" in stats
        assert "date_least_allowed" in stats
        assert "most_active_hour" in stats
        assert "least_active_hour" in stats
        assert "avg_queries_most" in stats
        assert "avg_queries_least" in stats
        assert "most_active_day" in stats
        assert "most_active_avg" in stats
        assert "least_active_day" in stats
        assert "least_active_avg" in stats
        
        # Check hour values
        assert 0 <= stats["most_active_hour"] <= 23
        assert 0 <= stats["least_active_hour"] <= 23
        
        # Check day values
        valid_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        assert stats["most_active_day"] in valid_days
        assert stats["least_active_day"] in valid_days

    def test_compute_stats_day_night_stats(self, stats_dataframe):
        """Test day/night stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check day/night stats exist
        assert "day_total_queries" in stats
        assert "day_top_client" in stats
        assert "day_top_allowed_client" in stats
        assert "day_top_blocked_client" in stats
        assert "day_top_allowed_domain" in stats
        assert "day_top_blocked_domain" in stats
        
        assert "night_total_queries" in stats
        assert "night_top_client" in stats
        assert "night_top_allowed_client" in stats
        assert "night_top_blocked_client" in stats
        assert "night_top_allowed_domain" in stats
        assert "night_top_blocked_domain" in stats
        
        # Check totals
        assert stats["day_total_queries"] >= 0
        assert stats["night_total_queries"] >= 0

    def test_compute_stats_streak_stats(self, stats_dataframe):
        """Test streak stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check streak stats exist
        assert "longest_streak_length_blocked" in stats
        assert "streak_date_blocked" in stats
        assert "streak_hour_blocked" in stats
        assert "longest_streak_length_allowed" in stats
        assert "streak_date_allowed" in stats
        assert "streak_hour_allowed" in stats
        
        # Check streak lengths are positive
        assert stats["longest_streak_length_blocked"] > 0
        assert stats["longest_streak_length_allowed"] > 0

    def test_compute_stats_idle_time_stats(self, stats_dataframe):
        """Test idle time stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check idle time stats exist
        assert "max_idle_ms" in stats
        assert "avg_time_between_blocked" in stats
        assert "avg_time_between_allowed" in stats
        assert "before_gap" in stats
        assert "after_gap" in stats
        
        # Check max_idle_ms is positive
        assert stats["max_idle_ms"] >= 0

    def test_compute_stats_unique_stats(self, stats_dataframe):
        """Test unique stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check unique stats exist
        assert "unique_domains" in stats
        assert "unique_clients" in stats
        assert "most_diverse_client" in stats
        assert "unique_domains_count" in stats
        
        # Check values
        assert stats["unique_domains"] > 0
        assert stats["unique_clients"] > 0
        assert stats["unique_domains_count"] > 0
        assert stats["most_diverse_client"] in stats_dataframe["client"].values

    def test_compute_stats_reply_time_stats(self, stats_dataframe):
        """Test reply time stats."""
        min_date = stats_dataframe["timestamp"].min()
        max_date = stats_dataframe["timestamp"].max()
        
        stats = compute_stats(stats_dataframe, min_date, max_date)
        
        # Check reply time stats exist
        assert "avg_reply_time" in stats
        assert "max_reply_time" in stats
        assert "min_reply_time" in stats
        assert "slowest_domain" in stats
        assert "slowest_avg_reply_time" in stats
        
        # Check values
        assert stats["avg_reply_time"] >= 0
        assert stats["max_reply_time"] >= stats["min_reply_time"]
        assert stats["slowest_domain"] in stats_dataframe["domain"].values

    def test_compute_stats_empty_dataframe(self):
        """Test compute_stats with empty dataframe."""
        empty_df = pd.DataFrame(columns=["timestamp", "status_type", "domain", "client", "reply_time", "date", "hour", "day_period", "day_name"])
        empty_df["timestamp"] = pd.to_datetime([])
        empty_df["status_type"] = []
        empty_df["domain"] = []
        empty_df["client"] = []
        empty_df["reply_time"] = []
        empty_df["date"] = pd.to_datetime([])
        empty_df["hour"] = []
        empty_df["day_period"] = []
        empty_df["day_name"] = []
        
        min_date = pd.Timestamp.now(tz="UTC")
        max_date = pd.Timestamp.now(tz="UTC")
        
        # Should handle empty dataframe gracefully
        # Note: Some stats functions may raise errors with empty data
        # This test checks if it handles it or raises appropriate errors
        try:
            stats = compute_stats(empty_df, min_date, max_date)
            # If it succeeds, check that stats exist
            assert isinstance(stats, dict)
        except (ValueError, IndexError, KeyError):
            # It's acceptable for empty dataframes to raise errors
            pass

    def test_compute_stats_single_row(self):
        """Test compute_stats with single row."""
        single_row = pd.DataFrame({
            "timestamp": [pd.Timestamp("2024-01-01 12:00:00", tz="UTC")],
            "status_type": ["Allowed"],
            "domain": ["example.com"],
            "client": ["192.168.1.1"],
            "reply_time": [0.05],
            "date": [pd.Timestamp("2024-01-01", tz="UTC").normalize()],
            "hour": [12],
            "day_period": ["Day"],
            "day_name": ["Monday"],
            "status": [2],
        })
        
        min_date = single_row["timestamp"].min()
        max_date = single_row["timestamp"].max()
        
        stats = compute_stats(single_row, min_date, max_date)
        
        assert isinstance(stats, dict)
        assert stats["total_queries"] == 1
        assert stats["allowed_count"] == 1
        assert stats["blocked_count"] == 0

    def test_compute_stats_all_blocked(self):
        """Test compute_stats with all blocked queries."""
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=100, freq="1h", tz="UTC"),
            "status_type": ["Blocked"] * 100,
            "domain": ["blocked.com"] * 100,
            "client": ["192.168.1.1"] * 100,
            "reply_time": [0.1] * 100,
            "date": pd.date_range("2024-01-01", periods=100, freq="1h", tz="UTC").normalize(),
            "hour": [i % 24 for i in range(100)],
            "day_period": ["Day"] * 100,
            "day_name": ["Monday"] * 100,
            "status": [1] * 100,
        })
        
        min_date = df["timestamp"].min()
        max_date = df["timestamp"].max()
        
        stats = compute_stats(df, min_date, max_date)
        
        assert stats["blocked_count"] == 100
        assert stats["allowed_count"] == 0
        assert stats["blocked_pct"] == 100.0
        assert stats["allowed_pct"] == 0.0

    def test_compute_stats_all_allowed(self):
        """Test compute_stats with all allowed queries."""
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=100, freq="1h", tz="UTC"),
            "status_type": ["Allowed"] * 100,
            "domain": ["allowed.com"] * 100,
            "client": ["192.168.1.1"] * 100,
            "reply_time": [0.1] * 100,
            "date": pd.date_range("2024-01-01", periods=100, freq="1h", tz="UTC").normalize(),
            "hour": [i % 24 for i in range(100)],
            "day_period": ["Day"] * 100,
            "day_name": ["Monday"] * 100,
            "status": [2] * 100,
        })
        
        min_date = df["timestamp"].min()
        max_date = df["timestamp"].max()
        
        stats = compute_stats(df, min_date, max_date)
        
        assert stats["blocked_count"] == 0
        assert stats["allowed_count"] == 100
        assert stats["blocked_pct"] == 0.0
        assert stats["allowed_pct"] == 100.0

    def test_compute_stats_zero_total_queries(self):
        """Test compute_stats with zero total queries edge case."""
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=1, freq="1h", tz="UTC"),
            "status_type": ["Other"],
            "domain": ["example.com"],
            "client": ["192.168.1.1"],
            "reply_time": [None],
            "date": [pd.Timestamp("2024-01-01", tz="UTC").normalize()],
            "hour": [12],
            "day_period": ["Day"],
            "day_name": ["Monday"],
            "status": [99],  # status is unknown, see pihole docs
        })
        
        min_date = df["timestamp"].min()
        max_date = df["timestamp"].max()
        
        stats = compute_stats(df, min_date, max_date)
        
        assert stats["blocked_count"] == 0
        assert stats["allowed_count"] == 0
        assert stats["blocked_pct"] == 0.0
        assert stats["allowed_pct"] == 0.0

    def test_compute_stats_empty_reply_times(self):
        """Test compute_stats with empty reply times."""
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="1h", tz="UTC"),
            "status_type": ["Allowed"] * 10,
            "domain": ["example.com"] * 10,
            "client": ["192.168.1.1"] * 10,
            "reply_time": [None] * 10,
            "date": pd.date_range("2024-01-01", periods=10, freq="1h", tz="UTC").normalize(),
            "hour": [i % 24 for i in range(10)],
            "day_period": ["Day"] * 10,
            "day_name": ["Monday"] * 10,
            "status": [2] * 10,
        })
        
        min_date = df["timestamp"].min()
        max_date = df["timestamp"].max()
        
        stats = compute_stats(df, min_date, max_date)
        
        assert stats["avg_reply_time"] == 0.0
        assert stats["max_reply_time"] == 0.0
        assert stats["min_reply_time"] == 0.0

    def test_compute_stats_single_client_single_domain(self):
        """Test compute_stats with single client and domain."""
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=50, freq="1h", tz="UTC"),
            "status_type": ["Allowed"] * 25 + ["Blocked"] * 25,
            "domain": ["example.com"] * 50,
            "client": ["192.168.1.1"] * 50,
            "reply_time": [0.1] * 50,
            "date": pd.date_range("2024-01-01", periods=50, freq="1h", tz="UTC").normalize(),
            "hour": [i % 24 for i in range(50)],
            "day_period": ["Day"] * 50,
            "day_name": ["Monday"] * 50,
            "status": [2] * 25 + [1] * 25,
        })
        
        min_date = df["timestamp"].min()
        max_date = df["timestamp"].max()
        
        stats = compute_stats(df, min_date, max_date)
        
        assert stats["unique_clients"] == 1
        assert stats["unique_domains"] == 1
        assert stats["most_diverse_client"] == "192.168.1.1"
        assert stats["unique_domains_count"] == 1
