import pandas as pd
import plotly.graph_objects as go

from piholelongtermstats.plot import (
    generate_plot_data,
    generate_queries_over_time,
    generate_client_activity_over_time,
)
from piholelongtermstats.process import prepare_hourly_aggregated_data

class TestGeneratePlotData:
    """Tests for generate_plot_data function."""

    def test_generate_plot_data_returns_dict(self, plot_dataframe):
        """Test that generate_plot_data returns a dictionary."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_generate_plot_data_structure(self, plot_dataframe):
        """Test structure of returned plot data."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        
        expected_keys = [
            "top_clients_stacked",
            "blocked_df",
            "allowed_df",
            "reply_time_df",
            "client_list",
            "data_span_days",
            "client_domain_scatter_df",
            "day_hour_heatmap",
            "blocked_day_hour_heatmap",
            "allowed_day_hour_heatmap",
        ]
        
        for key in expected_keys:
            assert key in result, f"Missing key: {key}"

    def test_generate_plot_data_top_clients_stacked(self, plot_dataframe):
        """Test top_clients_stacked data."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        top_clients_stacked = result["top_clients_stacked"]
        
        assert isinstance(top_clients_stacked, pd.DataFrame)
        assert "client" in top_clients_stacked.columns
        assert "status_type" in top_clients_stacked.columns
        assert "count" in top_clients_stacked.columns
        
        unique_clients = top_clients_stacked["client"].nunique()
        assert unique_clients <= n_clients

    def test_generate_plot_data_blocked_allowed_domains(self, plot_dataframe):
        """Test blocked and allowed domain dataframes."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        
        blocked_df = result["blocked_df"]
        allowed_df = result["allowed_df"]
        
        assert isinstance(blocked_df, pd.DataFrame)
        assert isinstance(allowed_df, pd.DataFrame)
        
        assert "Domain" in blocked_df.columns
        assert "count" in blocked_df.columns
        assert "Domain" in allowed_df.columns
        assert "count" in allowed_df.columns
        
        assert len(blocked_df) <= n_domains
        assert len(allowed_df) <= n_domains

    def test_generate_plot_data_reply_time_df(self, plot_dataframe):
        """Test reply time dataframe."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        reply_time_df = result["reply_time_df"]
        
        assert isinstance(reply_time_df, pd.DataFrame)
        assert "date" in reply_time_df.columns
        assert "reply_time_ms" in reply_time_df.columns
        
        assert (reply_time_df["reply_time_ms"] >= 0).all()

    def test_generate_plot_data_client_list(self, plot_dataframe):
        """Test client list."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        client_list = result["client_list"]
        
        assert isinstance(client_list, list)
        assert len(client_list) > 0
        assert all(client in plot_dataframe["client"].values for client in client_list)

    def test_generate_plot_data_heatmaps(self, plot_dataframe):
        """Test heatmap dataframes."""
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(plot_dataframe, n_clients, n_domains)
        
        day_hour_heatmap = result["day_hour_heatmap"]
        blocked_day_hour_heatmap = result["blocked_day_hour_heatmap"]
        allowed_day_hour_heatmap = result["allowed_day_hour_heatmap"]
        
        assert isinstance(day_hour_heatmap, pd.DataFrame)
        assert isinstance(blocked_day_hour_heatmap, pd.DataFrame)
        assert isinstance(allowed_day_hour_heatmap, pd.DataFrame)
        
        assert len(day_hour_heatmap) <= 7  # 7 days of week
        assert len(day_hour_heatmap.columns) <= 24  # 24 hours

    def test_generate_plot_data_domain_shortening(self, plot_dataframe):
        """Test that long domain names are shortened."""
        df = plot_dataframe.copy()
        df.loc[0, "domain"] = "a" * 100 + ".com"
        
        n_clients = 5
        n_domains = 10
        
        result = generate_plot_data(df, n_clients, n_domains)
        
        blocked_df = result["blocked_df"]
        if len(blocked_df) > 0:
            max_length = blocked_df["Domain"].str.len().max()
            assert max_length <= 45 or "..." in blocked_df["Domain"].iloc[0]


class TestGenerateQueriesOverTime:
    """Tests for generate_queries_over_time function."""

    def test_generate_queries_over_time_returns_figure(self, plot_dataframe):
        """Test that generate_queries_over_time returns a plotly figure."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        fig = generate_queries_over_time(callback_data)
        
        assert isinstance(fig, go.Figure)

    def test_generate_queries_over_time_all_clients(self, plot_dataframe):
        """Test generating plot for all clients."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        fig = generate_queries_over_time(callback_data, client=None)
        
        assert isinstance(fig, go.Figure)
        assert fig.layout.title.text == "DNS Queries Over Time for All Clients"

    def test_generate_queries_over_time_specific_client(self, plot_dataframe):
        """Test generating plot for specific client."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        client = plot_dataframe["client"].iloc[0]
        
        fig = generate_queries_over_time(callback_data, client=client)
        
        assert isinstance(fig, go.Figure)
        assert client in fig.layout.title.text

    def test_generate_queries_over_time_empty_data(self):
        """Test generating plot with empty data."""
        empty_hourly_agg = pd.DataFrame(columns=["timestamp", "status_type", "client", "count"])
        callback_data = {
            "hourly_agg": empty_hourly_agg,
            "top_clients": [],
        }
        
        fig = generate_queries_over_time(callback_data)
        
        assert isinstance(fig, go.Figure)

    def test_generate_queries_over_time_nonexistent_client(self, plot_dataframe):
        """Test generating plot for non-existent client."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        fig = generate_queries_over_time(callback_data, client="999.999.999.999")
        
        assert isinstance(fig, go.Figure)


class TestGenerateClientActivityOverTime:
    """Tests for generate_client_activity_over_time function."""

    def test_generate_client_activity_over_time_returns_figure(self, plot_dataframe):
        """Test that generate_client_activity_over_time returns a plotly figure."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        fig = generate_client_activity_over_time(callback_data, n_clients=5)
        
        assert isinstance(fig, go.Figure)

    def test_generate_client_activity_over_time_all_clients(self, plot_dataframe):
        """Test generating plot for all top clients."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        fig = generate_client_activity_over_time(callback_data, n_clients=5, client=None)
        
        assert isinstance(fig, go.Figure)
        assert "top" in fig.layout.title.text.lower() or "activity" in fig.layout.title.text.lower()

    def test_generate_client_activity_over_time_specific_client(self, plot_dataframe):
        """Test generating plot for specific client."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        client = plot_dataframe["client"].iloc[0]
        
        fig = generate_client_activity_over_time(callback_data, n_clients=5, client=client)
        
        assert isinstance(fig, go.Figure)
        assert client in fig.layout.title.text

    def test_generate_client_activity_over_time_empty_data(self):
        """Test generating plot with empty data."""
        empty_hourly_agg = pd.DataFrame(columns=["timestamp", "status_type", "client", "count"])
        callback_data = {
            "hourly_agg": empty_hourly_agg,
            "top_clients": [],
        }
        
        fig = generate_client_activity_over_time(callback_data, n_clients=5)
        
        assert isinstance(fig, go.Figure)


    def test_generate_client_activity_over_time_n_clients_larger(self, plot_dataframe):
        """Test with n_clients larger than available."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=100)
        
        fig = generate_client_activity_over_time(callback_data, n_clients=100)
        
        assert isinstance(fig, go.Figure)

    def test_generate_client_activity_over_time_nonexistent_client(self, plot_dataframe):
        """Test generating plot for non-existent client."""
        callback_data = prepare_hourly_aggregated_data(plot_dataframe, n_clients=5)
        
        fig = generate_client_activity_over_time(
            callback_data, n_clients=5, client="999.999.999.999"
        )
        
        assert isinstance(fig, go.Figure)

    def test_generate_plot_data_empty_dataframe(self):
        """Test generate_plot_data with empty dataframe."""
        empty_df = pd.DataFrame(columns=["timestamp", "status_type", "domain", "client", "reply_time", "date", "hour", "day_name"])
        empty_df["timestamp"] = pd.to_datetime([])
        empty_df["status_type"] = []
        empty_df["domain"] = []
        empty_df["client"] = []
        empty_df["reply_time"] = []
        empty_df["date"] = pd.to_datetime([])
        empty_df["hour"] = []
        empty_df["day_name"] = []
        
        result = generate_plot_data(empty_df, n_clients=5, n_domains=10)
        
        assert isinstance(result, dict)
        assert result["data_span_days"] == 0
        assert len(result["client_list"]) == 0
