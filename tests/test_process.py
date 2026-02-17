import pandas as pd

from piholelongtermstats.process import (
    _is_valid_regex,
    regex_ignore_domains,
    preprocess_df,
    prepare_hourly_aggregated_data,
)


class TestIsValidRegex:
    """Tests for _is_valid_regex function."""

    def test_valid_regex_patterns(self):
        """Test that valid regex patterns return True."""
        valid_patterns = [
            r".*\.local",
            r"^www\.",
            r"example\.com$",
            r"[0-9]+",
            r"\d+",
            r"test.*domain",
            r"(allowed|blocked)",
        ]
        
        for pattern in valid_patterns:
            assert _is_valid_regex(pattern) is True, f"Pattern '{pattern}' should be valid"

    def test_invalid_regex_patterns(self):
        """Test that invalid regex patterns return False."""
        invalid_patterns = [
            "*test",
            "[unclosed",
            "(unclosed",  
            "\\",  # incomplete escape
            "test[",
        ]
        
        for pattern in invalid_patterns:
            assert _is_valid_regex(pattern) is False, f"Pattern '{pattern}' should be invalid"

    def test_empty_string(self):
        """Test that empty string is a valid regex."""
        assert _is_valid_regex("") is True

    def test_simple_string(self):
        """Test that simple string without special chars is valid."""
        assert _is_valid_regex("test") is True
        assert _is_valid_regex("example.com") is True


class TestRegexIgnoreDomains:
    """Tests for regex_ignore_domains function."""

    def test_regex_ignore_domains_valid_pattern(self, sample_dataframe):
        """Test filtering with valid regex pattern."""
        df = sample_dataframe.copy()
        
        result = regex_ignore_domains(df, r".*blocked.*")
        
        assert len(result) <= len(df)
        
        if len(result) > 0:
            assert not result["domain"].str.contains("blocked", regex=True, na=False).any()

    def test_regex_ignore_domains_invalid_pattern(self, sample_dataframe):
        """Test that invalid regex pattern returns original dataframe."""
        df = sample_dataframe.copy()
        original_len = len(df)
        
        result = regex_ignore_domains(df, "*test")
        
        assert len(result) == original_len
        pd.testing.assert_frame_equal(result, df)

    def test_regex_ignore_domains_no_matches(self, sample_dataframe):
        """Test regex that matches nothing."""
        df = sample_dataframe.copy()
        original_len = len(df)

        result = regex_ignore_domains(df, r"nonexistent_pattern_12345")
        
        assert len(result) == original_len
        pd.testing.assert_frame_equal(result, df)

    def test_regex_ignore_domains_all_matches(self, sample_dataframe):
        """Test regex that matches everything."""
        df = sample_dataframe.copy()
        
        result = regex_ignore_domains(df, r".*")
        
        assert len(result) == 0
        assert list(result.columns) == list(df.columns)

    def test_regex_ignore_domains_resets_index(self, sample_dataframe):
        """Test that index is reset after filtering."""
        df = sample_dataframe.copy()
        df.index = range(100, 100 + len(df))
        
        result = regex_ignore_domains(df, r".*blocked.*")
        
        assert result.index.tolist() == list(range(len(result)))

    def test_regex_ignore_domains_preserves_columns(self, sample_dataframe):
        """Test that all columns are preserved."""
        df = sample_dataframe.copy()
        
        result = regex_ignore_domains(df, r".*blocked.*")
        
        assert set(result.columns) == set(df.columns)


class TestPreprocessDf:
    """Tests for preprocess_df function."""

    def test_preprocess_df_basic(self, sample_dataframe):
        """Test basic preprocessing."""
        df = sample_dataframe.copy()
        result = preprocess_df(df, timezone="UTC")
        
        assert "timestamp" in result.columns
        assert "date" in result.columns
        assert "hour" in result.columns
        assert "day_period" in result.columns
        assert "status_type" in result.columns
        assert "day_name" in result.columns
        
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_preprocess_df_timezone_conversion(self, sample_dataframe):
        """Test timezone conversion."""
        df = sample_dataframe.copy()
        
        result_utc = preprocess_df(df, timezone="UTC")
        result_ny = preprocess_df(df, timezone="America/New_York")
        
        assert result_utc["timestamp"].iloc[0].tz != result_ny["timestamp"].iloc[0].tz
        

    def test_preprocess_df_status_type(self, sample_dataframe):
        """Test status_type classification."""
        df = sample_dataframe.copy()
        result = preprocess_df(df, timezone="UTC")
        
        assert set(result["status_type"].unique()).issubset({"Allowed", "Blocked", "Other"})
        
        allowed_statuses = [2, 3, 12, 13, 14, 17]
        blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
        
        allowed_mask = result["status"].isin(allowed_statuses)
        blocked_mask = result["status"].isin(blocked_statuses)
        
        assert (result.loc[allowed_mask, "status_type"] == "Allowed").all()
        assert (result.loc[blocked_mask, "status_type"] == "Blocked").all()

    def test_preprocess_df_day_period(self, sample_dataframe):
        """Test day_period classification."""
        df = sample_dataframe.copy()
        result = preprocess_df(df, timezone="UTC")
        
        assert set(result["day_period"].unique()).issubset({"Day", "Night"})
        
        # Day: 6 <= hour < 24, Night: otherwise
        day_mask = (result["hour"] >= 6) & (result["hour"] < 24)
        night_mask = ~day_mask
        
        assert (result.loc[day_mask, "day_period"] == "Day").all()
        assert (result.loc[night_mask, "day_period"] == "Night").all()

    def test_preprocess_df_sorted(self, sample_dataframe):
        """Test that dataframe is sorted by timestamp."""
        df = sample_dataframe.copy()

        df = df.sample(frac=1).reset_index(drop=True)
        
        result = preprocess_df(df, timezone="UTC")

        assert result["timestamp"].is_monotonic_increasing

    def test_preprocess_df_reply_time_numeric(self, sample_dataframe):
        """Test that reply_time is converted to numeric."""
        df = sample_dataframe.copy()
        result = preprocess_df(df, timezone="UTC")
        
        assert pd.api.types.is_numeric_dtype(result["reply_time"])

    def test_preprocess_df_invalid_timezone(self, sample_dataframe):
        """Test that invalid timezone falls back to UTC."""
        df = sample_dataframe.copy()
        

        result = preprocess_df(df, timezone="Invalid/Timezone")
        
        assert "timestamp" in result.columns

    def test_preprocess_df_date_normalized(self, sample_dataframe):
        """Test that date column is normalized (time set to 00:00:00)."""
        df = sample_dataframe.copy()
        result = preprocess_df(df, timezone="UTC")
        
        assert (result["date"].dt.hour == 0).all()
        assert (result["date"].dt.minute == 0).all()
        assert (result["date"].dt.second == 0).all()

    def test_preprocess_df_day_name(self, sample_dataframe):
        """Test day_name column."""
        df = sample_dataframe.copy()
        result = preprocess_df(df, timezone="UTC")
        

        valid_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        assert set(result["day_name"].unique()).issubset(set(valid_days))


class TestPrepareHourlyAggregatedData:
    """Tests for prepare_hourly_aggregated_data function."""

    def test_prepare_hourly_aggregated_data_basic(self, preprocessed_dataframe):
        """Test basic hourly aggregation."""
        df = preprocessed_dataframe.copy()
        n_clients = 5
        
        result = prepare_hourly_aggregated_data(df, n_clients)
        
        assert isinstance(result, dict)
        assert "hourly_agg" in result
        assert "top_clients" in result
        
        hourly_agg = result["hourly_agg"]
        assert isinstance(hourly_agg, pd.DataFrame)
        assert "timestamp" in hourly_agg.columns
        assert "status_type" in hourly_agg.columns
        assert "client" in hourly_agg.columns
        assert "count" in hourly_agg.columns

    def test_prepare_hourly_aggregated_data_top_clients(self, preprocessed_dataframe):
        """Test top clients selection."""
        df = preprocessed_dataframe.copy()
        n_clients = 3
        
        result = prepare_hourly_aggregated_data(df, n_clients)
        
        assert len(result["top_clients"]) <= n_clients        
        assert isinstance(result["top_clients"], list)
        assert all(client in df["client"].values for client in result["top_clients"])

    def test_prepare_hourly_aggregated_data_aggregation(self, preprocessed_dataframe):
        """Test that aggregation is correct."""
        df = preprocessed_dataframe.copy()
        n_clients = 5

        result = prepare_hourly_aggregated_data(df, n_clients)
        hourly_agg = result["hourly_agg"]

        assert (hourly_agg["count"] > 0).all()

        assert (hourly_agg["timestamp"].dt.minute == 0).all()
        assert (hourly_agg["timestamp"].dt.second == 0).all()
        assert (hourly_agg["timestamp"].dt.microsecond == 0).all()

        top_clients = result["top_clients"]
        assert len(top_clients) <= n_clients
        for client in top_clients:
            assert client in df["client"].unique()


    def test_prepare_hourly_aggregated_data_n_clients_larger_than_data(self, preprocessed_dataframe):
        """Test when n_clients is larger than unique clients."""
        df = preprocessed_dataframe.copy()
        unique_clients = df["client"].nunique()
        n_clients = unique_clients + 10
        
        result = prepare_hourly_aggregated_data(df, n_clients)
        
        assert len(result["top_clients"]) <= unique_clients

    def test_prepare_hourly_aggregated_data_empty_dataframe(self):
        """Test with empty dataframe."""
        df = pd.DataFrame(columns=["timestamp", "status_type", "client"])
        df["timestamp"] = pd.to_datetime([])
        df["status_type"] = []
        df["client"] = []
        
        result = prepare_hourly_aggregated_data(df, n_clients=5)
        
        assert isinstance(result, dict)
        assert len(result["hourly_agg"]) == 0
        assert len(result["top_clients"]) == 0

    def test_prepare_hourly_aggregated_data_grouping(self, preprocessed_dataframe):
        """Test that grouping by hour, status_type, and client works."""
        df = preprocessed_dataframe.copy()
        n_clients = 5
        
        result = prepare_hourly_aggregated_data(df, n_clients)
        hourly_agg = result["hourly_agg"]
        
        if len(hourly_agg) > 0:
            grouped = hourly_agg.groupby(["timestamp", "status_type", "client"]).size()
            assert (grouped == 1).all() 
