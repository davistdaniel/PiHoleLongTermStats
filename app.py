import pandas as pd
import sqlite3
import argparse
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from datetime import datetime, timedelta

# initialize the parser for cli arguments
parser = argparse.ArgumentParser(
    description="Generate an interactive dashboard for Pi-hole query statistics."
)
parser.add_argument(
    "--days", type=int, default=365, help="Number of days of data to analyze."
)
parser.add_argument(
    "--db_path",
    type=str,
    default="pihole-FTL.db",
    help="Path to a copy of the PiHole FTL database.",
)
parser.add_argument(
    "--port", type=int, default=9292, help="Port to serve the dash app at."
)
args = parser.parse_args()


def read_pihole_ftl_db(db_path="pihole-FTL.db", days=365):
    """
    Reads in the pihole-FTL.db into a pandas dataframe.
    """

    # databased connection
    conn = sqlite3.connect(db_path)

    # get user-requested number of days from today
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    start_timestamp = int(start_date.timestamp())

    # select all queries for the time frame which is requested
    query = f"""
    SELECT * 
    FROM queries
    WHERE timestamp >= {start_timestamp};
    """

    # load data into a pandas dataframe
    df = pd.read_sql_query(query, conn)

    # close database connection
    conn.close()

    return df


# read pihole ftl db
df = read_pihole_ftl_db(db_path=args.db_path, days=args.days)

# basic time processing
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour

# status ids for pihole ftl db, see pi-hole FTL docs
allowed_statuses = [2, 3, 12, 13, 14, 17]
blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
df["status_type"] = df["status"].apply(
    lambda x: "Allowed"
    if x in allowed_statuses
    else ("Blocked" if x in blocked_statuses else "Other")
)

# calculate query related plot data
total_queries = len(df)
blocked_count = len(df[df["status_type"] == "Blocked"])
allowed_count = len(df[df["status_type"] == "Allowed"])
blocked_pct = (blocked_count / total_queries) * 100
allowed_pct = (allowed_count / total_queries) * 100
top_client = df["client"].value_counts().idxmax()
top_allowed_domain = (
    df[df["status_type"] == "Allowed"]["domain"].value_counts().idxmax()
)
top_blocked_domain = (
    df[df["status_type"] == "Blocked"]["domain"].value_counts().idxmax()
)
top_clients_stacked = (
    df[df["client"].notna()]
    .groupby(["client", "status_type"])
    .size()
    .reset_index(name="count")
)
top_domains = df["domain"].value_counts().nlargest(10)
top_clients = df["client"].value_counts().nlargest(10)

# init app
app = Dash(__name__)
app.title = "PiHole Long Term Statistics"
app.layout = html.Div(
    [
        html.H1("PiHole Long Term Statistics", style={"textAlign": "center"}),
        # info cards
        html.Div(
            [
                html.Div(
                    [
                        html.H3("Allowed Queries"),
                        html.P(f"{allowed_count:,} ({allowed_pct:.1f}%)"),
                        html.P(
                            f"Top allowed client was '{df[df['status_type'] == 'Allowed']['client'].value_counts().idxmax()}'",
                            style={"fontSize": "14px", "color": "#777"},
                        ),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Blocked Queries"),
                        html.P(f"{blocked_count:,} ({blocked_pct:.1f}%)"),
                        html.P(
                            f"Top blocked client was '{df[df['status_type'] == 'Blocked']['client'].value_counts().idxmax()}'",
                            style={"fontSize": "14px", "color": "#777"},
                        ),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Top Allowed Domain"),
                        html.P(top_allowed_domain),
                        html.P(
                            f"was allowed {df[df['domain'] == top_allowed_domain].shape[0]:,} times",
                            style={"fontSize": "14px", "color": "#777"},
                        ),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Top Blocked Domain"),
                        html.P(top_blocked_domain),
                        html.P(
                            f"was blocked {df[df['domain'] == top_blocked_domain].shape[0]:,} times",
                            style={"fontSize": "14px", "color": "#777"},
                        ),
                    ],
                    className="card",
                ),
                html.Div(
                    [
                        html.H3("Total Queries"),
                        html.P(f"{total_queries:,}"),
                        html.P(
                            f"Out of which {df['domain'].nunique():,} were unique, most queries came from '{top_client}'",
                            style={"fontSize": "14px", "color": "#777"},
                        ),
                    ],
                    className="card card-wide",
                ),
            ],
            className="kpi-container",
        ),
        html.Br(),
        # time series
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Queries Over Time"),
                        dcc.Graph(
                            id="time-series",
                            figure=px.histogram(
                                df,
                                x="timestamp",
                                color="status_type",
                                barmode="stack",
                                nbins=100,
                                title="Queries Over Time",
                                color_discrete_map={
                                    "Allowed": "#10b981",
                                    "Blocked": "#ef4444",
                                },
                                template="simple_white",
                            ),
                        ),
                    ],
                    className="cardplot",
                )
            ]
        ),
        html.Br(),
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Top Blocked Domains"),
                        dcc.Graph(
                            id="top-blocked-domains",
                            figure=px.bar(
                                df[df["status_type"] == "Blocked"]["domain"]
                                .value_counts()
                                .nlargest(10),
                                labels={"x": "Domain", "y": "Count"},
                                title="Top Blocked Domains",
                                template="simple_white",
                                color_discrete_sequence=["#ef4444"],
                            ),
                        ),
                    ],
                    className="cardplot",
                ),
                html.Div(
                    [
                        html.H2("Top Allowed Domains"),
                        dcc.Graph(
                            id="top-allowed-domains",
                            figure=px.bar(
                                df[df["status_type"] == "Allowed"]["domain"]
                                .value_counts()
                                .nlargest(10),
                                labels={"x": "Domain", "y": "Count"},
                                title="Top Allowed Domains",
                                template="simple_white",
                                color_discrete_sequence=["#10b981"],
                            ),
                        ),
                    ],
                    className="cardplot",
                ),
            ],
            className="row",
        ),
        html.Br(),
        html.Div(
            [
                dcc.Graph(
                    id="top-clients",
                    figure=px.bar(
                        top_clients_stacked,
                        x="client",
                        y="count",
                        color="status_type",
                        barmode="stack",
                        title="Top Clients by Query Type",
                        color_discrete_map={"Allowed": "#10b981", "Blocked": "#ef4444"},
                        template="simple_white",
                    ),
                ),
            ],
            className="cardplot",
        ),
        html.Br(),
        html.H2("Filtered Query Viewer"),
        html.Div(
            [
                dcc.Dropdown(
                    options=[{"label": c, "value": c} for c in df["client"].unique()],
                    id="client-filter",
                    placeholder="Select a Client",
                ),
                dcc.Graph(id="filtered-view"),
            ],
            className="cardplot",
        ),
    ],
    className="container",
)


# callback
@app.callback(Output("filtered-view", "figure"), Input("client-filter", "value"))
def update_filtered_view(client):
    if not client:
        dff = df
    else:
        dff = df[df["client"] == client]

    dff_grouped = (
        dff.groupby([pd.Grouper(key="timestamp", freq="h"), "status_type"])
        .size()
        .reset_index(name="count")
    )

    fig = px.bar(
        dff_grouped,
        x="timestamp",
        y="count",
        color="status_type",
        barmode="stack",
        title=f"DNS Queries Over Time for {client or 'All Clients'}",
        color_discrete_map={"Allowed": "#10b981", "Blocked": "#ef4444"},
        template="simple_white",
    )
    return fig


# serve the app on requested port
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=args.port, debug=True)
