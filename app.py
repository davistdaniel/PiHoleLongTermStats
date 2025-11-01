## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.1.1
## License :  MIT

import sqlite3
import argparse
import itertools
import os
import logging
import psutil
import plotly.express as px
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State
from datetime import datetime, timedelta
from pathlib import Path

# logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

####### command line options #######

# initialize parser
parser = argparse.ArgumentParser(
    description="Generate an interactive dashboard for Pi-hole query statistics."
)
parser.add_argument(
    "--days",
    type=int,
    default=int(os.getenv("PIHOLE_LT_STATS_DAYS", 31)),
    help="Number of days of data to analyze. Env: PIHOLE_LT_STATS_DAYS",
)
parser.add_argument(
    "--db_path",
    type=str,
    default=os.getenv("PIHOLE_LT_STATS_DB_PATH", "pihole-FTL.db"),
    help="Path to a copy of the PiHole FTL database. Env: PIHOLE_LT_STATS_DB_PATH",
)
parser.add_argument(
    "--port",
    type=int,
    default=int(os.getenv("PIHOLE_LT_STATS_PORT", 9292)),
    help="Port to serve the dash app at. Env: PIHOLE_LT_STATS_PORT",
)

args = parser.parse_args()


def connect_to_sql(db_path):
    """Connect to an SQL database"""

    if Path(db_path).is_file():
        conn = sqlite3.connect(db_path)
        return conn
    else:
        raise FileNotFoundError(
            f"Database file {db_path} not found. Please provide a valid path."
        )


def calculate_chunk_size(conn):
    """calculate chink size for reading the database based on available memory"""

    # calculate safe chunksize to not overlaod system memory
    sample_query = """SELECT id, timestamp, type, status, domain, client, reply_time
    FROM queries LIMIT 50"""
    sample_df = pd.read_sql_query(sample_query, conn)
    sample_df["timestamp"] = pd.to_datetime(sample_df["timestamp"], unit="s")
    available_memory = psutil.virtual_memory().available
    memory_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    safe_memory = available_memory * 0.5
    chunksize = int(safe_memory / memory_per_row)
    logging.info(f"Calculated chunksize = {chunksize} based on available memory.")

    return sample_df, chunksize


####### reading the database #######


def read_pihole_ftl_db(conn, days=31, start_date=None, end_date=None, chunksize=None):
    """Read the PiHole FTL database lazily"""

    if start_date is not None and end_date is not None:
        # if dates are selected, use them
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(
            days=1
        )  
    else:
        # otherwise use default day given by days (or args.days)
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)

    start_timestamp = int(start_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())

    query = f"""
    SELECT id, timestamp, type, status, domain, client, reply_time	 
    FROM queries
    WHERE timestamp >= {start_timestamp} AND timestamp < {end_timestamp};
    """

    chunk_num = 0
    for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
        chunk_num += 1
        logging.info(f"Processing database chunk {chunk_num}")
        yield chunk

    conn.close()


####### data collection for cards and plots #######


# basic time processing
def process_timestamps(df):
    """Convert timestamps in pandas dataframe of FTL database to date time."""

    logging.info(f"Processing timestamps for the past {args.days} days")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["day_period"] = df["hour"].apply(lambda h: "Day" if 6 <= h < 24 else "Night")

    # status ids for pihole ftl db, see pi-hole FTL docs
    logging.info("Processing allowed and blocked status codes")
    allowed_statuses = [2, 3, 12, 13, 14, 17]
    blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
    df["status_type"] = df["status"].apply(
        lambda x: "Allowed"
        if x in allowed_statuses
        else ("Blocked" if x in blocked_statuses else "Other")
    )

    df["day_name"] = df["timestamp"].dt.day_name()
    df["reply_time"] = pd.to_numeric(df["reply_time"], errors="coerce")

    return df


def generate_headings_data(sample_df, df):
    """Compute data and heading to display in the first info card"""

    oldest_data_point = (
        f"{sample_df['timestamp'].iloc[0].strftime('%-d-%-m-%Y (%H:%M)')}"
    )
    # latest_data_point = f"{df['timestamp'].iloc[-1].strftime('%-d-%-m-%Y (%H:%M)')}"
    min_date = df["timestamp"].min().strftime("%-d-%-m-%Y (%H:%M)")
    max_date = df["timestamp"].max().strftime("%-d-%-m-%Y (%H:%M)")
    date_diff = df["timestamp"].max() - df["timestamp"].min()

    data_span_days = date_diff.days
    hours = (date_diff.seconds // 3600) % 24
    minutes = (date_diff.seconds // 60) % 60
    data_span_str = f"{data_span_days}d,{hours}h and {minutes}min"
    logging.info(
        f"Data loaded with {len(df)} rows, spanning from {min_date} to {max_date}"
    )

    return (
        oldest_data_point,
        min_date,
        max_date,
        data_span_days,
        data_span_str,
    )


def generate_top_clients_data(df):
    """Compute data related to top clients"""

    # plot data for top clients plot
    logging.info("Processing top clients data")
    top_clients = df["client"].value_counts().nlargest(10).index
    top_client = df["client"].value_counts().idxmax()

    top_clients_stacked = (
        df[df["client"].isin(top_clients)]
        .groupby(["client", "status_type"])
        .size()
        .reset_index(name="count")
    )
    top_clients_stacked["client"] = pd.Categorical(
        top_clients_stacked["client"],
        categories=top_clients_stacked.groupby("client")["count"]
        .sum()
        .sort_values(ascending=False)
        .index,
        ordered=True,
    )
    top_clients_stacked = top_clients_stacked.sort_values(
        ["client", "count"], ascending=[True, False]
    )

    return top_client, top_clients, top_clients_stacked


def generate_allowed_blocked_df(df):
    # plot data for top allowed and blocked domains plot
    logging.info("Processing top allowed and blocked domains data")
    blocked_df = (
        df[df["status_type"] == "Blocked"]["domain"]
        .value_counts()
        .nlargest(10)
        .reset_index()
        .rename(columns={"index": "Count", "domain": "Domain"})
    )

    allowed_df = (
        df[df["status_type"] == "Allowed"]["domain"]
        .value_counts()
        .nlargest(10)
        .reset_index()
        .rename(columns={"index": "Count", "domain": "Domain"})
    )

    return allowed_df, blocked_df


def generate_query_stats(df):
    # data related for info cards
    logging.info("Generating data for info cards")
    total_queries = len(df)
    blocked_count = len(df[df["status_type"] == "Blocked"])
    allowed_count = len(df[df["status_type"] == "Allowed"])
    blocked_pct = (blocked_count / total_queries) * 100
    allowed_pct = (allowed_count / total_queries) * 100

    return total_queries, blocked_count, allowed_count, blocked_pct, allowed_pct


def generate_domain_stats(df):
    ## top allowed and blocked domain card
    logging.info("Finding top allowed and blocked domains")

    top_allowed_domain = (
        df[df["status_type"] == "Allowed"]["domain"].value_counts().idxmax()
    )
    top_blocked_domain = (
        df[df["status_type"] == "Blocked"]["domain"].value_counts().idxmax()
    )

    return top_allowed_domain, top_blocked_domain


def generate_persistent_client_stats(df):
    # data for most persistent client card
    logging.info("Finding most persistent client")
    blocked_df_c = df[df["status_type"] == "Blocked"]
    persistence = (
        blocked_df_c.groupby(["client", "domain"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    most_persistent_row = persistence.iloc[0]
    most_persistent_client = most_persistent_row["client"]
    blocked_domain = most_persistent_row["domain"]
    repeat_attempts = most_persistent_row["count"]

    return most_persistent_row, most_persistent_client, blocked_domain, repeat_attempts


def get_day_night_top_stats(period_df):
    # data for day and night stats cards
    logging.info("Processing day and night stats")
    return {
        "top_client": period_df["client"].value_counts().idxmax(),
        "total_queries": len(period_df),
        "top_allowed_client": period_df[period_df["status_type"] == "Allowed"]["client"]
        .value_counts()
        .idxmax(),
        "top_allowed_domain": period_df[period_df["status_type"] == "Allowed"]["domain"]
        .value_counts()
        .idxmax(),
        "top_blocked_client": period_df[period_df["status_type"] == "Blocked"]["client"]
        .value_counts()
        .idxmax(),
        "top_blocked_domain": period_df[period_df["status_type"] == "Blocked"]["domain"]
        .value_counts()
        .idxmax(),
    }


def generate_day_night_stats(df):
    day_df = df[df["day_period"] == "Day"]
    night_df = df[df["day_period"] == "Night"]
    day_stats = get_day_night_top_stats(day_df)
    night_stats = get_day_night_top_stats(night_df)

    return day_df, night_df, day_stats, night_stats


def generate_activity_stats(df):
    # data for date with most activity and least acitivity
    logging.info("Finding date with most and least activity")
    query_date_counts = df.groupby("date")["domain"].count()
    blocked_date_counts = (
        df[df["status_type"] == "Blocked"].groupby("date")["domain"].count()
    )
    allowed_date_counts = (
        df[df["status_type"] == "Allowed"].groupby("date")["domain"].count()
    )
    date_most_queries = query_date_counts.idxmax().strftime("%d %B %Y")
    date_most_blocked = blocked_date_counts.idxmax().strftime("%d %B %Y")
    date_most_allowed = allowed_date_counts.idxmax().strftime("%d %B %Y")

    date_least_queries = query_date_counts.idxmin().strftime("%d %B %Y")
    date_least_blocked = blocked_date_counts.idxmin().strftime("%d %B %Y")
    date_least_allowed = allowed_date_counts.idxmin().strftime("%d %B %Y")

    return (
        date_most_queries,
        date_most_blocked,
        date_most_allowed,
        date_least_queries,
        date_least_blocked,
        date_least_allowed,
    )


def generate_hourly_actvity_stats(df):
    # data for most active hour and least active hour
    logging.info("Finding most and least active hour")
    # hourly_avg = df.groupby("hour").size().mean()
    hourly_counts = df.groupby("hour").size()
    most_active_hour = hourly_counts.idxmax()
    least_active_hour = hourly_counts.idxmin()
    avg_queries_most = hourly_counts.max()
    avg_queries_least = hourly_counts.min()

    return most_active_hour, least_active_hour, avg_queries_most, avg_queries_least


def generate_daily_activity_stats(df):
    # data for most active day and least active day
    logging.info("Finding most and least active day of the week")
    daily_counts = (
        df.groupby(["date", "day_name"]).size().reset_index(name="query_count")
    )
    avg = (
        daily_counts.groupby("day_name")["query_count"]
        .mean()
        .sort_values(ascending=False)
    )
    most_active_day = avg.idxmax()
    most_active_avg = int(avg.max())
    least_active_day = avg.idxmin()
    least_active_avg = int(avg.min())

    return most_active_day, most_active_avg, least_active_day, least_active_avg


def generate_sorted_df(df):
    df_sorted = df.sort_values("timestamp").copy()
    df_sorted["is_blocked"] = df_sorted["status_type"] == "Blocked"
    df_sorted["is_allowed"] = df_sorted["status_type"] == "Allowed"
    df_sorted["blocked_group"] = (
        df_sorted["is_blocked"] != df_sorted["is_blocked"].shift()
    ).cumsum()
    df_sorted["allowed_group"] = (
        df_sorted["is_allowed"] != df_sorted["is_allowed"].shift()
    ).cumsum()
    df_sorted["idle_gap"] = df_sorted["timestamp"].diff().dt.total_seconds()

    return df_sorted


def generate_streak_stats(df_sorted):
    logging.info("Finding longest blocking and allowing streak")
    blocked_groups = df_sorted[df_sorted["is_blocked"]].groupby("blocked_group")
    allowed_groups = df_sorted[df_sorted["is_allowed"]].groupby("allowed_group")
    streaks_blocked = blocked_groups.agg(
        streak_length=("is_blocked", "size"), start_time=("timestamp", "first")
    )
    streaks_allowed = allowed_groups.agg(
        streak_length=("is_allowed", "size"), start_time=("timestamp", "first")
    )
    longest_streak_blocked = streaks_blocked.loc[
        streaks_blocked["streak_length"].idxmax()
    ]
    longest_streak_length_blocked = longest_streak_blocked["streak_length"]
    streak_start_time_blocked = longest_streak_blocked["start_time"]
    streak_date_blocked = streak_start_time_blocked.strftime("%d %B %Y")
    streak_hour_blocked = streak_start_time_blocked.strftime("%H:%M")

    longest_streak_allowed = streaks_allowed.loc[
        streaks_allowed["streak_length"].idxmax()
    ]
    longest_streak_length_allowed = longest_streak_allowed["streak_length"]
    streak_start_time_allowed = longest_streak_allowed["start_time"]
    streak_date_allowed = streak_start_time_allowed.strftime("%d %B %Y")
    streak_hour_allowed = streak_start_time_allowed.strftime("%H:%M")

    return (
        longest_streak_length_blocked,
        streak_date_blocked,
        streak_hour_blocked,
        longest_streak_length_allowed,
        streak_date_allowed,
        streak_hour_allowed,
    )


def generate_time_stats(df_sorted):
    # data for longest idle gap
    logging.info("Finding longest idle gap")
    max_idle_ms = df_sorted["idle_gap"].max()
    max_idle_idx = df_sorted["idle_gap"].idxmax()

    # data for average time between blocked and allowed queries
    logging.info("Finding average time between blocked and allowed queries")
    blocked = df_sorted[df_sorted["status_type"] == "Blocked"]
    blocked_times = blocked["timestamp"].diff().dt.total_seconds().dropna()
    avg_time_between_blocked = blocked_times.mean() if not blocked_times.empty else None

    allowed = df_sorted[df_sorted["status_type"] == "Allowed"]
    allowed_times = allowed["timestamp"].diff().dt.total_seconds().dropna()
    avg_time_between_allowed = allowed_times.mean() if not allowed_times.empty else None

    before_gap = (
        df_sorted.loc[max_idle_idx - 1, "timestamp"].strftime("%d-%b %Y %H:%M:%S.%f")[
            :-4
        ]
        if max_idle_idx > 0
        else None
    )
    after_gap = df_sorted.loc[max_idle_idx, "timestamp"].strftime(
        "%d-%b %Y %H:%M:%S.%f"
    )[:-4]

    return (
        max_idle_ms,
        avg_time_between_blocked,
        avg_time_between_allowed,
        before_gap,
        after_gap,
    )


def generate_client_stats(df):
    # data for number of unique clients
    logging.info("Finding number of unique clients")
    unique_clients = df["client"].nunique()

    # data for most diverse client
    logging.info("Finding most diverse client")
    diverse_client_df = (
        df.groupby("client")["domain"].nunique().reset_index(name="unique_domains")
    )
    diverse_client_df = diverse_client_df.sort_values("unique_domains", ascending=False)

    most_diverse_client = diverse_client_df.iloc[0]["client"]
    unique_domains_count = diverse_client_df.iloc[0]["unique_domains"]

    return unique_clients, most_diverse_client, unique_domains_count


def generate_reply_time_stats(df):
    # data for average reply times (in milliseconds)
    logging.info("Finding average reply times")

    avg_reply_time = round(df["reply_time"].dropna().abs().mean() * 1000, 3)
    max_reply_time = round(df["reply_time"].dropna().abs().max() * 1000, 3)
    min_reply_time = round(df["reply_time"].dropna().abs().min() * 1000, 3)
    logging.info(f"Minimum reply time is : {min_reply_time}")

    # data for slowest domain reply time (in seconds)
    logging.info("Finding domain with slowest reply time")
    avg_reply_times = df.groupby("domain")["reply_time"].mean().reset_index()
    slowest_domain_row = avg_reply_times.sort_values(
        "reply_time", ascending=False
    ).iloc[0]
    slowest_domain = slowest_domain_row["domain"]
    slowest_avg_reply_time = slowest_domain_row["reply_time"]

    return (
        avg_reply_time,
        max_reply_time,
        min_reply_time,
        slowest_domain,
        slowest_avg_reply_time,
    )


def serve_layout(db_path, days, start_date=None, end_date=None):
    # read pihole ftl db, process data, compute stats
    logging.info(f"Reading Pi hole DB from {db_path} for {days} days")
    conn = connect_to_sql(db_path)
    sample_df, chunksize = calculate_chunk_size(conn)
    df = pd.concat(
        read_pihole_ftl_db(
            conn,
            days=days,
            chunksize=chunksize,
            start_date=start_date,
            end_date=end_date,
        ),
        ignore_index=True,
    )
    logging.info("Converted DB to a pandas dataframe")

    df = process_timestamps(df)
    (
        oldest_data_point,
        min_date,
        max_date,
        data_span_days,
        data_span_str,
    ) = generate_headings_data(sample_df, df)
    top_client, top_clients, top_clients_stacked = generate_top_clients_data(df)
    allowed_df, blocked_df = generate_allowed_blocked_df(df)
    total_queries, blocked_count, allowed_count, blocked_pct, allowed_pct = (
        generate_query_stats(df)
    )
    top_allowed_domain, top_blocked_domain = generate_domain_stats(df)
    most_persistent_row, most_persistent_client, blocked_domain, repeat_attempts = (
        generate_persistent_client_stats(df)
    )
    day_df, night_df, day_stats, night_stats = generate_day_night_stats(df)
    (
        date_most_queries,
        date_most_blocked,
        date_most_allowed,
        date_least_queries,
        date_least_blocked,
        date_least_allowed,
    ) = generate_activity_stats(df)
    most_active_hour, least_active_hour, avg_queries_most, avg_queries_least = (
        generate_hourly_actvity_stats(df)
    )
    most_active_day, most_active_avg, least_active_day, least_active_avg = (
        generate_daily_activity_stats(df)
    )
    df_sorted = generate_sorted_df(df)
    (
        longest_streak_length_blocked,
        streak_date_blocked,
        streak_hour_blocked,
        longest_streak_length_allowed,
        streak_date_allowed,
        streak_hour_allowed,
    ) = generate_streak_stats(df_sorted)
    (
        max_idle_ms,
        avg_time_between_blocked,
        avg_time_between_allowed,
        before_gap,
        after_gap,
    ) = generate_time_stats(df_sorted)
    unique_clients, most_diverse_client, unique_domains_count = generate_client_stats(
        df
    )
    (
        avg_reply_time,
        max_reply_time,
        min_reply_time,
        slowest_domain,
        slowest_avg_reply_time,
    ) = generate_reply_time_stats(df)

    return df, html.Div(
        [
            html.Div(
                [
                    html.Img(src="/assets/logo_phlts.png", alt="Logo"),
                    html.H1("PiHole Long Term Stats"),
                ],
                className="heading-card",
            ),
            html.Div(
                [
                    dcc.DatePickerRange(
                        id="date-picker-range",
                        display_format="DD-MM-YYYY",
                        minimum_nights=0,
                        start_date_placeholder_text="Start",
                        end_date_placeholder_text="End",
                        className="date-picker-btn",
                        max_date_allowed=datetime.today().date()
                    ),
                    html.Button(
                        "🔄",
                        id="reload-button",
                        className="reload-btn",
                        title="Reload the data and update the dashboard.",
                    ),
                    html.A(
                        "🌟",
                        href="https://github.com/davistdaniel/PiHoleLongTermStats",
                        target="_blank",
                        className="reload-btn",
                        title="View PiHole Long Term Stats on GitHub",
                    ),
                ],
                className="reload-container",
            ),
            html.Div(
                [
                    html.H5(
                        f"Based on data from {min_date} to {max_date}, spanning {data_span_str}."
                    ),
                    html.Br(),
                    html.H6(f"Database records begin on {oldest_data_point}."),
                ],
                className="sub-heading-card",
            ),
            # info cards
            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Allowed Queries"),
                            html.P(f"{allowed_count:,} ({allowed_pct:.1f}%)"),
                            html.P(
                                f"Top allowed client was {df[df['status_type'] == 'Allowed']['client'].value_counts().idxmax()}.",
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
                                f"Top blocked client was {df[df['status_type'] == 'Blocked']['client'].value_counts().idxmax()}.",
                                style={"fontSize": "14px", "color": "#777"},
                            ),
                        ],
                        className="card",
                    ),
                    html.Div(
                        [
                            html.H3("Top Allowed Domain"),
                            html.P(
                                top_allowed_domain,
                                title=top_allowed_domain,
                                style={
                                    "fontSize": "20px",
                                    "whiteSpace": "wrap",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                },
                            ),
                            html.P(
                                f"""was allowed {df[df["domain"] == top_allowed_domain].shape[0]:,} times. This domain was queried the most by 
                            {df[(df["status_type"] == "Allowed") & (df["domain"] == top_allowed_domain)]["client"].value_counts().idxmax()}.""",
                                style={"fontSize": "14px", "color": "#777"},
                            ),
                        ],
                        className="card",
                    ),
                    html.Div(
                        [
                            html.H3("Top Blocked Domain"),
                            html.P(
                                top_blocked_domain,
                                title=top_blocked_domain,
                                style={
                                    "fontSize": "20px",
                                    "whiteSpace": "wrap",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                },
                            ),
                            html.P(
                                f"""was blocked {df[df["domain"] == top_blocked_domain].shape[0]:,} times. This domain was queried the most by 
                            {df[(df["status_type"] == "Blocked") & (df["domain"] == top_blocked_domain)]["client"].value_counts().idxmax()}.""",
                                style={"fontSize": "14px", "color": "#777"},
                            ),
                        ],
                        className="card",
                    ),
                    html.Details(
                        [
                            html.Summary(
                                "Query Stats",
                                style={"fontSize": "25px", "cursor": "pointer"},
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Total Unique Clients"),
                                    html.P(f"{unique_clients:,}"),
                                    html.P(
                                        "Devices that have made at least one query.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Total Queries"),
                                    html.P(f"{total_queries:,}"),
                                    html.P(
                                        f"Out of which {df['domain'].nunique():,} were unique, most queries came from {top_client}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Highest number of queries were on"),
                                    html.P(f"{date_most_queries}"),
                                    html.P(
                                        f"Highest number of allowed queries were on {date_most_allowed}. Highest number of blocked queries were on {date_most_blocked}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Lowest number of queries were on"),
                                    html.P(f"{date_least_queries}"),
                                    html.P(
                                        f"Lowest number of allowed queries were on {date_least_allowed}. Lowest number of blocked queries were on {date_least_blocked}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Average reply time"),
                                    html.P(f"{avg_reply_time} ms"),
                                    html.P(
                                        f"Longest reply time was {max_reply_time} ms and shortest reply time was {min_reply_time} ms.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                        ]
                    ),
                    html.Details(
                        [
                            html.Summary(
                                "Activity Stats",
                                style={"fontSize": "25px", "cursor": "pointer"},
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Most Active Hour"),
                                    html.P(
                                        f"{most_active_hour}:00 - {most_active_hour + 1}:00"
                                    ),
                                    html.P(
                                        f"On average, {avg_queries_most:,} queries are made during this time.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Least Active Hour"),
                                    html.P(
                                        f"{least_active_hour}:00 - {least_active_hour + 1}:00"
                                    ),
                                    html.P(
                                        f"On average, {avg_queries_least:,} queries are made during this time.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Most Active Day of the Week"),
                                    html.P(most_active_day),
                                    html.P(
                                        f"On average, {most_active_avg:,} queries are made on this day.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Least Active Day of the Week"),
                                    html.P(least_active_day),
                                    html.P(
                                        f"On average, {least_active_avg:,} queries are made on this day.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Longest Blocking Streak"),
                                    html.P(
                                        f"{longest_streak_length_blocked:,} queries"
                                    ),
                                    html.P(
                                        f"on {streak_date_blocked} at {streak_hour_blocked}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Longest Allowing Streak"),
                                    html.P(
                                        f"{longest_streak_length_allowed:,} queries"
                                    ),
                                    html.P(
                                        f"on {streak_date_allowed} at {streak_hour_allowed}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                        ]
                    ),
                    html.Details(
                        [
                            html.Summary(
                                "Day and Night Stats",
                                style={"fontSize": "25px", "cursor": "pointer"},
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Total queries during the day"),
                                    html.P(f"{day_stats['total_queries']:,}"),
                                    html.P(
                                        f"Most queries were from {day_stats['top_client']}. {day_stats['top_allowed_client']} had the most allowed queries and {day_stats['top_blocked_client']} had the most blocked.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Total queries during the night"),
                                    html.P(f"{night_stats['total_queries']:,}"),
                                    html.P(
                                        f"Most queries were from {night_stats['top_client']}. {night_stats['top_allowed_client']} had the most allowed queries and {night_stats['top_blocked_client']} had the most blocked.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Top allowed domain during the day"),
                                    html.P(
                                        f"{day_stats['top_allowed_domain']}",
                                        title=day_stats["top_allowed_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"""was allowed {day_df[day_df["domain"] == day_stats["top_allowed_domain"]].shape[0]:,} times. This domain was queried the most by 
                            {day_df[(day_df["status_type"] == "Allowed") & (day_df["domain"] == day_stats["top_allowed_domain"])]["client"].value_counts().idxmax()}.""",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Top blocked domain during the day"),
                                    html.P(
                                        f"{day_stats['top_blocked_domain']}",
                                        title=day_stats["top_blocked_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"""was blocked {day_df[day_df["domain"] == day_stats["top_blocked_domain"]].shape[0]:,} times. This domain was queried the most by 
                            {day_df[(day_df["status_type"] == "Blocked") & (day_df["domain"] == day_stats["top_blocked_domain"])]["client"].value_counts().idxmax()}.""",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Top allowed domain during the night"),
                                    html.P(
                                        f"{night_stats['top_allowed_domain']}",
                                        title=night_stats["top_allowed_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"""was allowed {night_df[night_df["domain"] == night_stats["top_allowed_domain"]].shape[0]:,} times. This domain was queried the most by 
                            {night_df[(night_df["status_type"] == "Allowed") & (night_df["domain"] == night_stats["top_allowed_domain"])]["client"].value_counts().idxmax()}.""",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Top blocked domain during the night"),
                                    html.P(
                                        f"{night_stats['top_blocked_domain']}",
                                        title=night_stats["top_blocked_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"""was blocked {night_df[night_df["domain"] == night_stats["top_blocked_domain"]].shape[0]:,} times. This domain was queried the most by 
                            {night_df[(night_df["status_type"] == "Blocked") & (night_df["domain"] == night_stats["top_blocked_domain"])]["client"].value_counts().idxmax()}.""",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                        ]
                    ),
                    html.Details(
                        [
                            html.Summary(
                                "Other Stats",
                                style={"fontSize": "25px", "cursor": "pointer"},
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Most Persistent Client"),
                                    html.P(f"{most_persistent_client}"),
                                    html.P(
                                        f"Tried accessing '{blocked_domain}' {repeat_attempts} times despite being blocked.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Most Diverse Client"),
                                    html.P(f"{most_diverse_client}"),
                                    html.P(
                                        f"Queried {unique_domains_count:,} unique domains.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Longest Idle Period"),
                                    html.P(f"{max_idle_ms:,.0f} s"),
                                    html.P(
                                        f"Between {before_gap} and {after_gap}",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Slowest Responding Domain"),
                                    html.P(
                                        f"{slowest_domain}",
                                        title=slowest_domain,
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"Avg reply time: {slowest_avg_reply_time * 1000:.2f} ms",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Average Time Between Blocked Queries"),
                                    html.P(
                                        f"{avg_time_between_blocked:.2f} s"
                                        if avg_time_between_blocked
                                        else "N/A"
                                    ),
                                    html.P(
                                        "Average interval between blocked queries.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Average Time Between Allowed Queries"),
                                    html.P(
                                        f"{avg_time_between_allowed:.2f} s"
                                        if avg_time_between_allowed
                                        else "N/A"
                                    ),
                                    html.P(
                                        "Average interval between successful queries.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                        ]
                    ),
                    html.Br(),
                ],
                className="kpi-container",
            ),
            html.Br(),
            # time series
            html.H2("Queries over time"),
            html.Div(
                [
                    dcc.Dropdown(
                        options=[
                            {"label": c, "value": c} for c in df["client"].unique()
                        ],
                        id="client-filter",
                        placeholder="Select a Client",
                    ),
                    dcc.Dropdown(
                        options=[
                            {"label": "Hours", "value": "h"},
                            {
                                "label": "Months",
                                "value": "ME",
                                "disabled": data_span_days < 31,
                            },
                            {
                                "label": "Years",
                                "value": "YE",
                                "disabled": data_span_days < 365,
                            },
                        ],
                        id="freq-filter",
                        placeholder="Frequency",
                    ),
                    dcc.Graph(id="filtered-view"),
                    html.H2("Top Client Activity Over Time"),
                    dcc.Graph(id="client-activity-view"),
                ],
                className="cardplot",
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
                                    blocked_df,
                                    x="Domain",
                                    y="count",
                                    labels={
                                        "Domain": "Domain",
                                        "count": "Count",
                                    },
                                    template="plotly_white",
                                    color_discrete_sequence=["#ef4444"],
                                ).update_layout(showlegend=False),
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
                                    allowed_df,
                                    x="Domain",
                                    y="count",
                                    labels={
                                        "Domain": "Domain",
                                        "count": "Count",
                                    },
                                    template="plotly_white",
                                    color_discrete_sequence=["#10b981"],
                                ).update_layout(showlegend=False),
                            ),
                        ],
                        className="cardplot",
                    ),
                ],
                className="row",
            ),
            html.Br(),
            html.H2("Top Client Activity"),
            html.Div(
                [
                    dcc.Graph(
                        id="top-clients",
                        figure=px.bar(
                            top_clients_stacked,
                            x="client",
                            y="count",
                            labels={
                                "client": "Client",
                                "count": "Count",
                                "status_type": "Query status",
                            },
                            color="status_type",
                            barmode="stack",
                            title="Top Clients by Query Type",
                            color_discrete_map={
                                "Allowed": "#10b981",
                                "Blocked": "#ef4444",
                                "Other": "#b99529",
                            },
                            template="plotly_white",
                        ),
                    ),
                ],
                className="cardplot",
            ),
            html.Br(),
            html.Div(
                [
                    html.Div(
                        [
                            html.H2("Average Reply Time per Day"),
                            dcc.Graph(
                                id="avg-reply-time",
                                figure=px.line(
                                    df.groupby("date")["reply_time"]
                                    .mean()
                                    .mul(1000)
                                    .reset_index(name="reply_time_ms"),
                                    x="date",
                                    y="reply_time_ms",
                                    labels={
                                        "reply_time_ms": "Average Reply Time (ms)",
                                        "date": "Date",
                                    },
                                    markers=True,
                                    color_discrete_sequence=["#3b82f6"],
                                    template="plotly_white",
                                ),
                            ),
                        ],
                        className="cardplot",
                    )
                ]
            ),
            html.Br(),
        ],
        className="container",
    )


####### Intializing the app #######

logging.info("Initializing Dash app")
app = Dash(__name__)
app.title = "PiHole Long Term Statistics"
PIHOLE_FTL_DF, div = serve_layout(args.db_path, args.days)
# this needs to be improved, currently reload works by using a global variable

app.layout = html.Div(
    [
        dcc.Loading(
            id="loading-main",
            type="graph",
            fullscreen=True,
            children=[
                html.Div(
                    id="page-container", children=div.children, className="container"
                )
            ],
        )
    ]
)


@app.callback(
    Output("page-container", "children"),
    Input("reload-button", "n_clicks"),
    State("date-picker-range", "start_date"),
    State("date-picker-range", "end_date"),
    prevent_initial_call=False,
)
def reload_page(n_clicks, start_date, end_date):
    global PIHOLE_FTL_DF
    logging.info(f"Reload button clicked. Date range :{start_date, end_date} ")
    PIHOLE_FTL_DF, div = serve_layout(
        args.db_path, args.days, start_date=start_date, end_date=end_date
    )
    return div.children


@app.callback(
    Output("filtered-view", "figure"),
    Input("client-filter", "value"),
    Input("freq-filter", "value"),
    Input("reload-button", "n_clicks"),
)
def update_filtered_view(client, freq, n_clicks):
    global PIHOLE_FTL_DF

    if not client:
        dff = PIHOLE_FTL_DF
    else:
        dff = PIHOLE_FTL_DF[PIHOLE_FTL_DF["client"] == client]

    if not freq:
        freq = "h"

    dff_grouped = (
        dff.groupby([pd.Grouper(key="timestamp", freq=freq), "status_type"])
        .size()
        .reset_index(name="count")
    )

    # Fill missing data with 0
    all_times = pd.date_range(
        dff_grouped["timestamp"].min(), dff_grouped["timestamp"].max(), freq=freq
    )
    status_types = ["Other", "Allowed", "Blocked"]
    full_index = pd.MultiIndex.from_product(
        [all_times, status_types], names=["timestamp", "status_type"]
    )
    dff_grouped = (
        dff_grouped.set_index(["timestamp", "status_type"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )
    dff_grouped["status_type"] = pd.Categorical(
        dff_grouped["status_type"], categories=status_types, ordered=True
    )
    dff_grouped = dff_grouped.sort_values("status_type")

    fig = px.area(
        dff_grouped,
        x="timestamp",
        y="count",
        color="status_type",
        line_group="status_type",
        title=f"DNS Queries Over Time for {client or 'All Clients'}",
        color_discrete_map={
            "Allowed": "#10b981",
            "Blocked": "#ef4444",
            "Other": "#b99529",
        },
        template="plotly_white",
        labels={"timestamp": "Date", "count": "Count", "status_type": "Query Status"},
    )

    fig.update_traces(
        mode="lines",
        line_shape="spline",
        line=dict(width=0.5),
        stackgroup="one",
    )

    return fig


@app.callback(
    Output("client-activity-view", "figure"),
    Input("client-filter", "value"),
    Input("freq-filter", "value"),
    Input("reload-button", "n_clicks"),
)
def update_client_activity(client, freq, n_clicks):
    global PIHOLE_FTL_DF

    if not client:
        dff = PIHOLE_FTL_DF
    else:
        dff = PIHOLE_FTL_DF[PIHOLE_FTL_DF["client"] == client]

    if not freq:
        freq = "h"

    top_clients = dff.groupby("client").size().nlargest(10).index
    dff_top = dff[dff["client"].isin(top_clients)]
    dff_grouped = (
        dff_top.groupby([pd.Grouper(key="timestamp", freq=freq), "client"])
        .size()
        .reset_index(name="count")
    )

    # Fill missing data with no client activity with zero.
    all_times = pd.date_range(
        dff_grouped["timestamp"].min(), dff_grouped["timestamp"].max(), freq=freq
    )
    pivot_df = (
        dff_grouped.pivot(index="timestamp", columns="client", values="count")
        .reindex(all_times)
        .fillna(0)
        .reset_index()
        .rename(columns={"index": "timestamp"})
    )
    mod_df = pivot_df.melt(id_vars="timestamp", var_name="client", value_name="count")

    default_colors = px.colors.qualitative.Plotly
    client_color_map = dict(zip(top_clients, itertools.cycle(default_colors)))

    fig = px.area(
        mod_df,
        x="timestamp",
        y="count",
        color="client",
        line_group="client",
        color_discrete_map=client_color_map,
        template="plotly_white",
        labels={"timestamp": "Date", "count": "Count", "client": "Client IP"},
    )

    fig.update_traces(
        mode="lines",
        line_shape="spline",
        line=dict(width=0.2),
        stackgroup="one",
        connectgaps=False,
    )

    return fig


# serve
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=args.port, debug=False)
