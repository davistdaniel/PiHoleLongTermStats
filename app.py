## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.1.4
## License :  MIT

import os
import gc
import sqlite3
import argparse
import itertools
import logging
import psutil
import plotly.express as px
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

__version__ = "0.1.4"

# logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
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

parser.add_argument(
    "--n_clients",
    type=int,
    default=int(os.getenv("PIHOLE_LT_STATS_NCLIENTS", 10)),
    help="Number of top clients to show in top clients plots. Env: PIHOLE_LT_STATS_NCLIENTS",
)

parser.add_argument(
    "--n_domains",
    type=int,
    default=int(os.getenv("PIHOLE_LT_STATS_NDOMAINS", 10)),
    help="Number of top domains to show in top domains plots. Env: PIHOLE_LT_STATS_NDOMAINS",
)

parser.add_argument(
    "--timezone",
    type=str,
    default=os.getenv("PIHOLE_LT_STATS_TIMEZONE", "UTC"),
    help="Timezone for display (e.g., 'America/New_York', 'Europe/London'). Env: PIHOLE_LT_STATS_TIMEZONE",
)


args = parser.parse_args()

logging.info("Setting environment variables:")
logging.info(f"PIHOLE_LT_STATS_DAYS : {args.days}")
logging.info(f"PIHOLE_LT_STATS_DB_PATH : {args.db_path}")
logging.info(f"PIHOLE_LT_STATS_PORT : {args.port}")
logging.info(f"PIHOLE_LT_STATS_NCLIENTS : {args.n_clients}")
logging.info(f"PIHOLE_LT_STATS_NDOMAINS : {args.n_domains}")
logging.info(f"PIHOLE_LT_STATS_TIMEZONE : {args.timezone}")


####### reading the database #######
def connect_to_sql(db_path):
    """Connect to an SQL database"""

    if Path(db_path).is_file():
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="replace")
        logging.info(f"Connected to SQL database at {db_path}")
        return conn
    else:
        logging.error(
            f"Database file {db_path} not found. Please provide a valid path."
        )
        raise FileNotFoundError(
            f"Database file {db_path} not found. Please provide a valid path."
        )


def probe_sample_df(conn):
    """compute basic stats from a subset of the databases"""

    # calculate safe chunksize to not overload system memory
    sample_query = """SELECT id, timestamp, type, status, domain, client, reply_time
    FROM queries LIMIT 5"""
    sample_df = pd.read_sql_query(sample_query, conn)
    sample_df["timestamp"] = pd.to_datetime(sample_df["timestamp"], unit="s")
    available_memory = psutil.virtual_memory().available
    memory_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    safe_memory = available_memory * 0.5
    chunksize = int(safe_memory / memory_per_row)
    logging.info(f"Calculated chunksize = {chunksize} based on available memory.")

    latest_ts_raw = pd.read_sql_query("SELECT MAX(timestamp) AS ts FROM queries", conn)[
        "ts"
    ].iloc[0]
    latest_ts = pd.to_datetime(latest_ts_raw, unit="s", utc=True)
    oldest_ts_raw = pd.read_sql_query("SELECT MIN(timestamp) AS ts FROM queries", conn)[
        "ts"
    ].iloc[0]
    oldest_ts = pd.to_datetime(oldest_ts_raw, unit="s", utc=True)

    del sample_df
    gc.collect()

    return chunksize, latest_ts, oldest_ts


def read_pihole_ftl_db(
    db_paths,
    conn,
    days=31,
    start_date=None,
    end_date=None,
    chunksize=None,
    timezone="UTC",
):
    """Read the PiHole FTL database lazily"""

    try:
        tz = ZoneInfo(timezone)
    except Exception:
        logging.warning(f"Invalid timezone '{timezone}', using UTC")
        tz = ZoneInfo("UTC")

    logging.info(f"Selected timezone: {timezone}")

    if start_date is not None and end_date is not None:
        # if dates are selected, use them
        logging.info(
            f"A date range was selected : {start_date} to {end_date} (TZ: {timezone})."
        )

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

        start_dt = start_dt.replace(tzinfo=tz)
        end_dt = end_dt.replace(tzinfo=tz)
    else:
        # otherwise use default day given by days (or args.days)
        logging.info(
            f"A date range was not selected. Using default number of days : {days} (TZ: {timezone})."
        )
        end_dt = datetime.now(tz)
        start_dt = end_dt - timedelta(days=days)

    logging.info(
        f"Trying to read data from PiHole-FTL database for the period ranging from {start_dt} to {end_dt} (TZ: {timezone})..."
    )

    start_timestamp = int(start_dt.astimezone(ZoneInfo("UTC")).timestamp())
    end_timestamp = int(end_dt.astimezone(ZoneInfo("UTC")).timestamp())

    logging.info(
        f"Converted dates ranging from {start_dt} to {end_dt} (TZ: {timezone}) to timestamps in UTC : {start_timestamp} to {end_timestamp}"
    )

    logging.info(
        f"Reading data from PiHole-FTL database for timestamps ranging from {start_timestamp} to {end_timestamp} (TZ: UTC)..."
    )

    query = f"""
    SELECT id, timestamp, type, status, domain, client, reply_time	 
    FROM queries
    WHERE timestamp >= {start_timestamp} AND timestamp < {end_timestamp};
    """

    for db_idx, db_path in enumerate(db_paths):
        logging.info(f"Processing database {db_idx + 1}/{len(db_paths)}: {db_path}...")
        conn = connect_to_sql(db_path)

        chunk_num = 0
        for chunk in pd.read_sql_query(query, conn, chunksize=chunksize[db_idx]):
            chunk_num += 1
            logging.info(
                f"Processing dataframe chunk {chunk_num} from database {db_idx + 1}..."
            )
            yield chunk

        conn.close()


####### data collection for cards and plots #######


# basic time processing
def preprocess_df(df, timezone="UTC"):
    """Pre-process df to generate timestamps, blocked,allowed domains etc."""

    logging.info("Pre-processing dataframe...")

    try:
        tz = ZoneInfo(timezone)  # noqa: F841
    except Exception as e:
        logging.warning(f"Invalid timezone '{timezone}', falling back to UTC: {e}")
        timezone = "UTC"

    logging.info(f"Selected timezone : {timezone}")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert(timezone)
    df["date"] = df["timestamp"].dt.normalize()  # needed in group by operations
    df["hour"] = df["timestamp"].dt.hour
    df["day_period"] = df["hour"].apply(lambda h: "Day" if 6 <= h < 24 else "Night")
    logging.info(
        f"Set timestamp, date, hour and day_period columns using timezone : {timezone}"
    )

    # status ids for pihole ftl db, see pi-hole FTL docs
    logging.info("Processing allowed and blocked status codes...")
    allowed_statuses = [2, 3, 12, 13, 14, 17]
    blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
    df["status_type"] = df["status"].apply(
        lambda x: "Allowed"
        if x in allowed_statuses
        else ("Blocked" if x in blocked_statuses else "Other")
    )

    df["day_name"] = df["timestamp"].dt.day_name()
    df["reply_time"] = pd.to_numeric(df["reply_time"], errors="coerce")
    logging.info("Set status_type, day_name and reply_time columns.")

    return df


def compute_stats(df, min_date_available, max_date_available):
    """Compute all statistics and return them as a dictionary"""

    logging.info("Started computing stats...")
    stats = {}

    # data used for first heading
    stats["n_data_points"] = len(df)
    logging.info(f"Stats will be based on {stats['n_data_points']} data points.")

    stats["oldest_data_point"] = f"{min_date_available.strftime('%-d-%-m-%Y (%H:%M)')}"
    stats["latest_data_point"] = f"{max_date_available.strftime('%-d-%-m-%Y (%H:%M)')}"
    stats["min_date"] = df["timestamp"].min().strftime("%-d-%-m-%Y (%H:%M)")
    stats["max_date"] = df["timestamp"].max().strftime("%-d-%-m-%Y (%H:%M)")
    logging.info(
        f"Stats will be computed for dates ranging from {stats['min_date']} to {stats['max_date']}"
    )

    date_diff = df["timestamp"].max() - df["timestamp"].min()
    stats["data_span_days"] = date_diff.days
    hours = (date_diff.seconds // 3600) % 24
    minutes = (date_diff.seconds // 60) % 60
    stats["data_span_str"] = f"{stats['data_span_days']}d,{hours}h and {minutes}min"
    logging.info("Computed data for headings.")

    # query stats
    stats["total_queries"] = len(df)
    stats["blocked_count"] = len(df[df["status_type"] == "Blocked"])
    stats["allowed_count"] = len(df[df["status_type"] == "Allowed"])
    stats["blocked_pct"] = (stats["blocked_count"] / stats["total_queries"]) * 100
    stats["allowed_pct"] = (stats["allowed_count"] / stats["total_queries"]) * 100
    logging.info("Computed data for query metrics.")

    # top clients
    stats["top_client"] = df["client"].value_counts().idxmax()
    stats["top_allowed_client"] = (
        df[df["status_type"] == "Allowed"]["client"].value_counts().idxmax()
    )
    stats["top_blocked_client"] = (
        df[df["status_type"] == "Blocked"]["client"].value_counts().idxmax()
    )
    logging.info("Computed data for top clients.")

    # domain stats
    stats["top_allowed_domain"] = (
        df[df["status_type"] == "Allowed"]["domain"].value_counts().idxmax()
    )
    stats["top_blocked_domain"] = (
        df[df["status_type"] == "Blocked"]["domain"].value_counts().idxmax()
    )
    stats["top_allowed_domain_count"] = df[
        df["domain"] == stats["top_allowed_domain"]
    ].shape[0]
    stats["top_blocked_domain_count"] = df[
        df["domain"] == stats["top_blocked_domain"]
    ].shape[0]
    stats["top_allowed_domain_client"] = (
        df[
            (df["status_type"] == "Allowed")
            & (df["domain"] == stats["top_allowed_domain"])
        ]["client"]
        .value_counts()
        .idxmax()
    )
    stats["top_blocked_domain_client"] = (
        df[
            (df["status_type"] == "Blocked")
            & (df["domain"] == stats["top_blocked_domain"])
        ]["client"]
        .value_counts()
        .idxmax()
    )
    stats["unique_domains"] = df["domain"].nunique()
    logging.info("Computed data for domains.")

    # most persistent client despite being blocked
    blocked_df_c = df[df["status_type"] == "Blocked"]
    persistence = (
        blocked_df_c.groupby(["client", "domain"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    most_persistent_row = persistence.iloc[0]
    stats["most_persistent_client"] = most_persistent_row["client"]
    stats["blocked_domain"] = most_persistent_row["domain"]
    stats["repeat_attempts"] = most_persistent_row["count"]
    logging.info("Computed data for most persistent client.")

    # activity stats based on date
    query_date_counts = df.groupby("date")["domain"].count()
    blocked_date_counts = (
        df[df["status_type"] == "Blocked"].groupby("date")["domain"].count()
    )
    allowed_date_counts = (
        df[df["status_type"] == "Allowed"].groupby("date")["domain"].count()
    )
    stats["date_most_queries"] = query_date_counts.idxmax().strftime("%d %B %Y")
    stats["date_most_blocked"] = blocked_date_counts.idxmax().strftime("%d %B %Y")
    stats["date_most_allowed"] = allowed_date_counts.idxmax().strftime("%d %B %Y")
    stats["date_least_queries"] = query_date_counts.idxmin().strftime("%d %B %Y")
    stats["date_least_blocked"] = blocked_date_counts.idxmin().strftime("%d %B %Y")
    stats["date_least_allowed"] = allowed_date_counts.idxmin().strftime("%d %B %Y")
    logging.info("Computed data for activity stats based on date.")

    # activity stats based on hour
    hourly_counts = df.groupby("hour").size()
    stats["most_active_hour"] = hourly_counts.idxmax()
    stats["least_active_hour"] = hourly_counts.idxmin()
    stats["avg_queries_most"] = int(hourly_counts.max())
    stats["avg_queries_least"] = int(hourly_counts.min())
    logging.info("Computed data for activity stats based based on hour.")

    # activity stats based on day
    daily_counts = (
        df.groupby(["date", "day_name"]).size().reset_index(name="query_count")
    )
    avg = (
        daily_counts.groupby("day_name")["query_count"]
        .mean()
        .sort_values(ascending=False)
    )
    stats["most_active_day"] = avg.idxmax()
    stats["most_active_avg"] = int(avg.max())
    stats["least_active_day"] = avg.idxmin()
    stats["least_active_avg"] = int(avg.min())
    logging.info("Computed data for activity stats based based on hour.")

    # day-night stats
    day_df = df[df["day_period"] == "Day"]
    night_df = df[df["day_period"] == "Night"]

    stats["day_total_queries"] = len(day_df)
    stats["day_top_client"] = day_df["client"].value_counts().idxmax()
    stats["day_top_allowed_client"] = (
        day_df[day_df["status_type"] == "Allowed"]["client"].value_counts().idxmax()
    )
    stats["day_top_blocked_client"] = (
        day_df[day_df["status_type"] == "Blocked"]["client"].value_counts().idxmax()
    )
    stats["day_top_allowed_domain"] = (
        day_df[day_df["status_type"] == "Allowed"]["domain"].value_counts().idxmax()
    )
    stats["day_top_blocked_domain"] = (
        day_df[day_df["status_type"] == "Blocked"]["domain"].value_counts().idxmax()
    )
    stats["day_top_allowed_domain_count"] = day_df[
        day_df["domain"] == stats["day_top_allowed_domain"]
    ].shape[0]
    stats["day_top_blocked_domain_count"] = day_df[
        day_df["domain"] == stats["day_top_blocked_domain"]
    ].shape[0]
    stats["day_top_allowed_domain_client"] = (
        day_df[
            (day_df["status_type"] == "Allowed")
            & (day_df["domain"] == stats["day_top_allowed_domain"])
        ]["client"]
        .value_counts()
        .idxmax()
    )
    stats["day_top_blocked_domain_client"] = (
        day_df[
            (day_df["status_type"] == "Blocked")
            & (day_df["domain"] == stats["day_top_blocked_domain"])
        ]["client"]
        .value_counts()
        .idxmax()
    )

    stats["night_total_queries"] = len(night_df)
    stats["night_top_client"] = night_df["client"].value_counts().idxmax()
    stats["night_top_allowed_client"] = (
        night_df[night_df["status_type"] == "Allowed"]["client"].value_counts().idxmax()
    )
    stats["night_top_blocked_client"] = (
        night_df[night_df["status_type"] == "Blocked"]["client"].value_counts().idxmax()
    )
    stats["night_top_allowed_domain"] = (
        night_df[night_df["status_type"] == "Allowed"]["domain"].value_counts().idxmax()
    )
    stats["night_top_blocked_domain"] = (
        night_df[night_df["status_type"] == "Blocked"]["domain"].value_counts().idxmax()
    )
    stats["night_top_allowed_domain_count"] = night_df[
        night_df["domain"] == stats["night_top_allowed_domain"]
    ].shape[0]
    stats["night_top_blocked_domain_count"] = night_df[
        night_df["domain"] == stats["night_top_blocked_domain"]
    ].shape[0]
    stats["night_top_allowed_domain_client"] = (
        night_df[
            (night_df["status_type"] == "Allowed")
            & (night_df["domain"] == stats["night_top_allowed_domain"])
        ]["client"]
        .value_counts()
        .idxmax()
    )
    stats["night_top_blocked_domain_client"] = (
        night_df[
            (night_df["status_type"] == "Blocked")
            & (night_df["domain"] == stats["night_top_blocked_domain"])
        ]["client"]
        .value_counts()
        .idxmax()
    )

    logging.info("Computed data for day and night stats.")

    # allowed and blocked streaks
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
    stats["longest_streak_length_blocked"] = int(
        longest_streak_blocked["streak_length"]
    )
    stats["streak_date_blocked"] = longest_streak_blocked["start_time"].strftime(
        "%d %B %Y"
    )
    stats["streak_hour_blocked"] = longest_streak_blocked["start_time"].strftime(
        "%H:%M"
    )

    longest_streak_allowed = streaks_allowed.loc[
        streaks_allowed["streak_length"].idxmax()
    ]
    stats["longest_streak_length_allowed"] = int(
        longest_streak_allowed["streak_length"]
    )
    stats["streak_date_allowed"] = longest_streak_allowed["start_time"].strftime(
        "%d %B %Y"
    )
    stats["streak_hour_allowed"] = longest_streak_allowed["start_time"].strftime(
        "%H:%M"
    )

    logging.info("Computed data for streak stats.")

    # idle time stats
    max_idle_ms = df_sorted["idle_gap"].max()
    max_idle_idx = df_sorted["idle_gap"].idxmax()

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

    stats["max_idle_ms"] = max_idle_ms
    stats["avg_time_between_blocked"] = avg_time_between_blocked
    stats["avg_time_between_allowed"] = avg_time_between_allowed
    stats["before_gap"] = before_gap
    stats["after_gap"] = after_gap

    logging.info("Computed data for time stats.")

    stats["unique_clients"] = df["client"].nunique()
    diverse_client_df = (
        df.groupby("client")["domain"].nunique().reset_index(name="unique_domains")
    )
    diverse_client_df = diverse_client_df.sort_values("unique_domains", ascending=False)
    stats["most_diverse_client"] = diverse_client_df.iloc[0]["client"]
    stats["unique_domains_count"] = int(diverse_client_df.iloc[0]["unique_domains"])

    # reply time stats
    stats["avg_reply_time"] = round(df["reply_time"].dropna().abs().mean() * 1000, 3)
    stats["max_reply_time"] = round(df["reply_time"].dropna().abs().max() * 1000, 3)
    stats["min_reply_time"] = round(df["reply_time"].dropna().abs().min() * 1000, 3)

    avg_reply_times = df.groupby("domain")["reply_time"].mean().reset_index()
    slowest_domain_row = avg_reply_times.sort_values(
        "reply_time", ascending=False
    ).iloc[0]
    stats["slowest_domain"] = slowest_domain_row["domain"]
    stats["slowest_avg_reply_time"] = slowest_domain_row["reply_time"]

    logging.info("Computed data for reply time stats.")

    logging.info("All stats computed.")
    # release some memory, testing if this is the memory leak
    del df_sorted, day_df, night_df, blocked_df_c, persistence
    del blocked_groups, allowed_groups, streaks_blocked, streaks_allowed
    del blocked, allowed, diverse_client_df
    gc.collect()

    return stats


def generate_plot_data(df):
    """Generate plot data separately to avoid keeping df references"""

    logging.info("Generating plot data...")

    # plot data for top clients
    top_clients = df["client"].value_counts().nlargest(args.n_clients).index
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
    logging.info("Generated plot data for top clients.")

    # plot data for allowed and blocked domains
    def shorten(s):
        return s if len(s) <= 45 else f"{s[:20]}...{s[-20:]}"

    tmp = df[df["status_type"] == "Blocked"].copy()
    tmp["domain"] = tmp["domain"].apply(shorten)

    blocked_df = (
        tmp["domain"]
        .value_counts()
        .nlargest(args.n_domains)
        .reset_index()
        .rename(columns={"index": "Count", "domain": "Domain"})
    )

    tmp = df[df["status_type"] == "Allowed"].copy()
    tmp["domain"] = tmp["domain"].apply(shorten)

    allowed_df = (
        tmp["domain"]
        .value_counts()
        .nlargest(args.n_domains)
        .reset_index()
        .rename(columns={"index": "Count", "domain": "Domain"})
    )

    del tmp
    gc.collect()

    logging.info("Generated plot data for allowed and blocked domains.")

    # plot data for reply time over days
    reply_time_df = (
        df.groupby("date")["reply_time"]
        .mean()
        .mul(1000)
        .reset_index(name="reply_time_ms")
    )

    logging.info("Generated plot data for reply time plot")

    client_list = df["client"].unique().tolist()

    logging.info("Plot data generation complete")

    return {
        "top_clients_stacked": top_clients_stacked,
        "blocked_df": blocked_df,
        "allowed_df": allowed_df,
        "reply_time_df": reply_time_df,
        "client_list": client_list,
        "data_span_days": (df["timestamp"].max() - df["timestamp"].min()).days,
    }


def prepare_hourly_aggregated_data(df):
    """Pre-aggregate data by hour"""
    logging.info("Pre-aggregating data by hour for callbacks...")

    # aggregate by hour, status_type, and client
    hourly_agg = (
        df.groupby([pd.Grouper(key="timestamp", freq="h"), "status_type", "client"])
        .size()
        .reset_index(name="count")
    )

    # get top n_clients clients for client activity view
    top_clients = df["client"].value_counts().nlargest(args.n_clients).index.tolist()

    logging.info("Hourly aggregation complete")
    return {
        "hourly_agg": hourly_agg,
        "top_clients": top_clients,
    }


def serve_layout(
    db_path,
    days,
    max_date_available,
    min_date_available,
    chunksize_list,
    start_date=None,
    end_date=None,
    timezone="UTC",
):
    """Read pihole ftl db, process data, compute stats"""

    if isinstance(db_path, str):
        db_paths = db_path.split(",")

    start_memory = psutil.virtual_memory().available

    df = pd.concat(
        read_pihole_ftl_db(
            db_paths,
            conn,
            days=days,
            chunksize=chunksize_list,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
        ),
        ignore_index=True,
    )

    logging.info("Converted DB to a pandas dataframe")

    if df.empty:
        logging.error(
            "Empty dataframe. No data returned from the database for the given parameters. Try adjusting --days to cover a larger time period."
        )
        raise RuntimeError(
            f"Empty dataframe. No data returned from the database for the given parameters. Database records range from {min_date_available} to {max_date_available}. Try increasing `--days` or the environment variable `PIHOLE_LT_STATS_DAYS`."
        )

    # should reduce some memory consumption
    df["id"] = df["id"].astype("int32")
    df["type"] = df["type"].astype("int8")
    df["status"] = df["status"].astype("int8")

    # process timestamps according to timezone
    df = preprocess_df(df, timezone=timezone)

    # compute the stats
    stats = compute_stats(df, min_date_available, max_date_available)

    # generate plot data
    plot_data = generate_plot_data(df)

    hourly_data = prepare_hourly_aggregated_data(df)

    callback_data = {
        "hourly_agg": hourly_data["hourly_agg"],
        "top_clients": hourly_data["top_clients"],
        "data_span_days": plot_data["data_span_days"],
    }

    # release memory
    del df, hourly_data
    gc.collect()

    layout = html.Div(
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
                        minimum_nights=2,
                        start_date_placeholder_text="Start",
                        end_date_placeholder_text="End",
                        className="date-picker-btn",
                        min_date_allowed=min_date_available.date(),
                        max_date_allowed=max_date_available.date(),
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
                        f"Data from {stats['min_date']} to {stats['max_date']}, spanning {stats['data_span_str']} is shown. Stats are based on {stats['n_data_points']} data points. "
                    ),
                    html.Br(),
                    html.H6(
                        f"Timezone is {timezone}. Database records begin on {stats['oldest_data_point']} and end on {stats['latest_data_point']}."
                    ),
                ],
                className="sub-heading-card",
            ),
            # info cards
            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Allowed Queries"),
                            html.P(
                                f"{stats['allowed_count']:,} ({stats['allowed_pct']:.1f}%)"
                            ),
                            html.P(
                                f"Top allowed client was {stats['top_allowed_client']}.",
                                style={"fontSize": "14px", "color": "#777"},
                            ),
                        ],
                        className="card",
                    ),
                    html.Div(
                        [
                            html.H3("Blocked Queries"),
                            html.P(
                                f"{stats['blocked_count']:,} ({stats['blocked_pct']:.1f}%)"
                            ),
                            html.P(
                                f"Top blocked client was {stats['top_blocked_client']}.",
                                style={"fontSize": "14px", "color": "#777"},
                            ),
                        ],
                        className="card",
                    ),
                    html.Div(
                        [
                            html.H3("Top Allowed Domain"),
                            html.P(
                                stats["top_allowed_domain"],
                                title=stats["top_allowed_domain"],
                                style={
                                    "fontSize": "20px",
                                    "whiteSpace": "wrap",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                },
                            ),
                            html.P(
                                f"was allowed {stats['top_allowed_domain_count']:,} times. This domain was queried the most by {stats['top_allowed_domain_client']}.",
                                style={"fontSize": "14px", "color": "#777"},
                            ),
                        ],
                        className="card",
                    ),
                    html.Div(
                        [
                            html.H3("Top Blocked Domain"),
                            html.P(
                                stats["top_blocked_domain"],
                                title=stats["top_blocked_domain"],
                                style={
                                    "fontSize": "20px",
                                    "whiteSpace": "wrap",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                },
                            ),
                            html.P(
                                f"was blocked {stats['top_blocked_domain_count']:,} times. This domain was queried the most by {stats['top_blocked_domain_client']}.",
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
                                    html.P(f"{stats['unique_clients']:,}"),
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
                                    html.P(f"{stats['total_queries']:,}"),
                                    html.P(
                                        f"Out of which {stats['unique_domains']:,} were unique, most queries came from {stats['top_client']}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Highest number of queries were on"),
                                    html.P(f"{stats['date_most_queries']}"),
                                    html.P(
                                        f"Highest number of allowed queries were on {stats['date_most_allowed']}. Highest number of blocked queries were on {stats['date_most_blocked']}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Lowest number of queries were on"),
                                    html.P(f"{stats['date_least_queries']}"),
                                    html.P(
                                        f"Lowest number of allowed queries were on {stats['date_least_allowed']}. Lowest number of blocked queries were on {stats['date_least_blocked']}.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardquery",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Average reply time"),
                                    html.P(f"{stats['avg_reply_time']} ms"),
                                    html.P(
                                        f"Longest reply time was {stats['max_reply_time']} ms and shortest reply time was {stats['min_reply_time']} ms.",
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
                                        f"{stats['most_active_hour']}:00 - {stats['most_active_hour'] + 1}:00"
                                    ),
                                    html.P(
                                        f"On average, {stats['avg_queries_most']:,} queries are made during this time.",
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
                                        f"{stats['least_active_hour']}:00 - {stats['least_active_hour'] + 1}:00"
                                    ),
                                    html.P(
                                        f"On average, {stats['avg_queries_least']:,} queries are made during this time.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Most Active Day of the Week"),
                                    html.P(stats["most_active_day"]),
                                    html.P(
                                        f"On average, {stats['most_active_avg']:,} queries are made on this day.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardactivity",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Least Active Day of the Week"),
                                    html.P(stats["least_active_day"]),
                                    html.P(
                                        f"On average, {stats['least_active_avg']:,} queries are made on this day.",
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
                                        f"{stats['longest_streak_length_blocked']:,} queries"
                                    ),
                                    html.P(
                                        f"on {stats['streak_date_blocked']} at {stats['streak_hour_blocked']}.",
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
                                        f"{stats['longest_streak_length_allowed']:,} queries"
                                    ),
                                    html.P(
                                        f"on {stats['streak_date_allowed']} at {stats['streak_hour_allowed']}.",
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
                                    html.P(f"{stats['day_total_queries']:,}"),
                                    html.P(
                                        f"Most queries were from {stats['day_top_client']}. {stats['day_top_allowed_client']} had the most allowed queries and {stats['day_top_blocked_client']} had the most blocked.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="carddaynight",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Total queries during the night"),
                                    html.P(f"{stats['night_total_queries']:,}"),
                                    html.P(
                                        f"Most queries were from {stats['night_top_client']}. {stats['night_top_allowed_client']} had the most allowed queries and {stats['night_top_blocked_client']} had the most blocked.",
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
                                        f"{stats['day_top_allowed_domain']}",
                                        title=stats["day_top_allowed_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"was allowed {stats['day_top_allowed_domain_count']:,} times. This domain was queried the most by {stats['day_top_allowed_domain_client']}.",
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
                                        f"{stats['day_top_blocked_domain']}",
                                        title=stats["day_top_blocked_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"was blocked {stats['day_top_blocked_domain_count']:,} times. This domain was queried the most by {stats['day_top_blocked_domain_client']}.",
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
                                        f"{stats['night_top_allowed_domain']}",
                                        title=stats["night_top_allowed_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"was allowed {stats['night_top_allowed_domain_count']:,} times. This domain was queried the most by {stats['night_top_allowed_domain_client']}.",
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
                                        f"{stats['night_top_blocked_domain']}",
                                        title=stats["night_top_blocked_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"was blocked {stats['night_top_blocked_domain_count']:,} times. This domain was queried the most by {stats['night_top_blocked_domain_client']}.",
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
                                    html.P(f"{stats['most_persistent_client']}"),
                                    html.P(
                                        f"Tried accessing '{stats['blocked_domain']}' {stats['repeat_attempts']} times despite being blocked.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Most Diverse Client"),
                                    html.P(f"{stats['most_diverse_client']}"),
                                    html.P(
                                        f"Queried {stats['unique_domains_count']:,} unique domains.",
                                        style={"fontSize": "14px", "color": "#777"},
                                    ),
                                ],
                                className="cardother",
                            ),
                            html.Br(),
                            html.Div(
                                [
                                    html.H3("Longest Idle Period"),
                                    html.P(f"{stats['max_idle_ms']:,.0f} s"),
                                    html.P(
                                        f"Between {stats['before_gap']} and {stats['after_gap']}",
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
                                        f"{stats['slowest_domain']}",
                                        title=stats["slowest_domain"],
                                        style={
                                            "fontSize": "18px",
                                            "whiteSpace": "wrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                    ),
                                    html.P(
                                        f"Avg reply time: {stats['slowest_avg_reply_time'] * 1000:.2f} ms",
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
                                        f"{stats['avg_time_between_blocked']:.2f} s"
                                        if stats["avg_time_between_blocked"]
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
                                        f"{stats['avg_time_between_allowed']:.2f} s"
                                        if stats["avg_time_between_allowed"]
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
            html.Div(
                [
                    html.H2("Queries over time"),
                    html.H5("Aggregated hourly"),
                    dcc.Dropdown(
                        options=[
                            {"label": c, "value": c} for c in plot_data["client_list"]
                        ],
                        id="client-filter",
                        placeholder="Select a Client",
                    ),
                    dcc.Graph(id="filtered-view"),
                    html.H2("Client Activity Over Time"),
                    html.H5("Aggregated hourly"),
                    dcc.Graph(id="client-activity-view"),
                ],
                className="cardplot",
            ),
            html.Br(),
            html.Div(
                [
                    html.H2("Top Blocked Domains"),
                    dcc.Graph(
                        id="top-blocked-domains",
                        figure=px.bar(
                            plot_data["blocked_df"],
                            y="count",
                            x="Domain",
                            labels={
                                "Domain": "Domain",
                                "count": "Count",
                            },
                            template="plotly_white",
                            color_discrete_sequence=["#ef4444"],
                        ).update_layout(
                            showlegend=False,
                            margin=dict(r=0, t=0, l=0, b=0),
                            xaxis=dict(
                                title=None,
                                automargin=True,
                                tickmode="auto",
                            ),
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
                            plot_data["allowed_df"],
                            y="count",
                            x="Domain",
                            labels={
                                "Domain": "Domain",
                                "count": "Count",
                            },
                            template="plotly_white",
                            color_discrete_sequence=["#10b981"],
                        ).update_layout(
                            showlegend=False,
                            margin=dict(r=0, t=0, l=0, b=0),
                            xaxis=dict(
                                title=None,
                                automargin=True,
                                tickmode="auto",
                            ),
                        ),
                    ),
                ],
                className="cardplot",
            ),
            html.Br(),
            html.Div(
                [
                    html.H2("Top Client Activity"),
                    dcc.Graph(
                        id="top-clients",
                        figure=px.bar(
                            plot_data["top_clients_stacked"],
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
                        ).update_layout(
                            legend=dict(
                                orientation="h",
                                yanchor="top",
                                y=-0.4,
                                xanchor="center",
                                x=0.5,
                            ),
                            xaxis=dict(title=None, automargin=True),
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
                            html.H2("Average Reply Time"),
                            dcc.Graph(
                                id="avg-reply-time",
                                figure=px.line(
                                    plot_data["reply_time_df"],
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
            html.Footer(
            f"PiHoleLongTermStats v.{__version__}",
            style={"textAlign": "center", "padding": "10px", "color": "#666"},
        ),
        ],
        className="container",
    )
    logging.info(
        f"Memory used while serving layout: {(start_memory - psutil.virtual_memory().available) / (1024.0**3)}"
    )
    return callback_data, layout


####### Intializing the app #######

logging.info("Initializing PiHoleLongTermStats Dashboard")
app = Dash("PiHoleLongTermStats")
app.title = "PiHoleLongTermStats"

if isinstance(args.db_path, str):
    db_paths = args.db_path.split(",")

chunksize_list, latest_ts_list, oldest_ts_list = (
    [],
    [],
    [],
)

for db in db_paths:
    conn = connect_to_sql(db)
    chunksize, latest_ts, oldest_ts = probe_sample_df(conn)
    chunksize_list.append(chunksize)
    latest_ts_list.append(latest_ts.tz_convert(ZoneInfo(args.timezone)))
    oldest_ts_list.append(oldest_ts.tz_convert(ZoneInfo(args.timezone)))
    conn.close()

logging.info(
    f"Latest date-time from all databases : {max(latest_ts_list)} (TZ: {args.timezone})"
)
logging.info(
    f"Oldest date-time from all databases : {min(oldest_ts_list)} (TZ: {args.timezone})"
)

# Initialize with data, no date range initially.
PHLTS_CALLBACK_DATA, initial_layout = serve_layout(
    db_path=args.db_path,
    days=args.days,
    max_date_available=max(latest_ts_list),
    min_date_available=min(oldest_ts_list),
    chunksize_list=chunksize_list,
    start_date=None,
    end_date=None,
    timezone=args.timezone,
)

logging.info("Setting initial layout...")

app.layout = html.Div(
    [
        dcc.Loading(
            id="loading-main",
            type="graph",
            fullscreen=True,
            children=[
                html.Div(
                    id="page-container",
                    children=initial_layout.children,
                    className="container",
                )
            ],
        )
    ]
)

del initial_layout
gc.collect()


@app.callback(
    Output("page-container", "children"),
    Input("reload-button", "n_clicks"),
    State("date-picker-range", "start_date"),
    State("date-picker-range", "end_date"),
    prevent_initial_call=False,
)
def reload_page(n_clicks, start_date, end_date):
    global PHLTS_CALLBACK_DATA

    logging.info(f"Reload button clicked. Selected date range: {start_date, end_date}")

    PHLTS_CALLBACK_DATA, layout = serve_layout(
        db_path=args.db_path,
        days=args.days,
        max_date_available=max(latest_ts_list),
        min_date_available=min(oldest_ts_list),
        chunksize_list=chunksize_list,
        start_date=start_date,
        end_date=end_date,
        timezone=args.timezone,
    )

    return layout.children


@app.callback(
    Output("filtered-view", "figure"),
    Input("client-filter", "value"),
    Input("reload-button", "n_clicks"),
)
def update_filtered_view(client, n_clicks):
    logging.info("Updating Queries over time plot...")
    global PHLTS_CALLBACK_DATA

    dff_grouped = PHLTS_CALLBACK_DATA["hourly_agg"]

    if client:
        logging.info(f"Selected client : {client}")
        dff_grouped = dff_grouped[dff_grouped["client"] == client]
        title_text = f"DNS Queries Over Time for {client}"
    else:
        dff_grouped = (
            dff_grouped.groupby(["timestamp", "status_type"])["count"]
            .sum()
            .reset_index()
        )
        title_text = "DNS Queries Over Time for All Clients"

    # Fill missing data with 0
    all_times = pd.date_range(
        dff_grouped["timestamp"].min(), dff_grouped["timestamp"].max(), freq="h"
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
        title=title_text,
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

    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.4, xanchor="center", x=0.5)
    )

    del dff_grouped
    gc.collect()

    return fig


@app.callback(
    Output("client-activity-view", "figure"),
    Input("client-filter", "value"),
    Input("reload-button", "n_clicks"),
)
def update_client_activity(client, n_clicks):
    logging.info("Updating Client activity over time plot...")
    global PHLTS_CALLBACK_DATA

    dff_grouped = PHLTS_CALLBACK_DATA["hourly_agg"]
    top_clients = PHLTS_CALLBACK_DATA["top_clients"]

    if client:
        logging.info(f"Selected client : {client}")
        dff_grouped = dff_grouped[dff_grouped["client"] == client]
        dff_grouped = (
            dff_grouped.groupby(["timestamp", "client"])["count"].sum().reset_index()
        )
        title_text = f"Activity for {client}"
        clients_to_show = [client]
    else:
        dff_grouped = dff_grouped[dff_grouped["client"].isin(top_clients)]
        dff_grouped = (
            dff_grouped.groupby(["timestamp", "client"])["count"].sum().reset_index()
        )
        title_text = f"Activity for top {args.n_clients} clients"
        clients_to_show = top_clients

    all_times = pd.date_range(
        dff_grouped["timestamp"].min(), dff_grouped["timestamp"].max(), freq="h"
    )
    full_index = pd.MultiIndex.from_product(
        [all_times, clients_to_show], names=["timestamp", "client"]
    )
    pivot_df = (
        dff_grouped.set_index(["timestamp", "client"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )

    default_colors = px.colors.qualitative.Plotly
    client_color_map = dict(zip(top_clients, itertools.cycle(default_colors)))

    fig = px.area(
        pivot_df,
        x="timestamp",
        y="count",
        color="client",
        line_group="client",
        title=title_text,
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

    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.4, xanchor="center", x=0.5)
    )

    del dff_grouped, pivot_df
    gc.collect()

    return fig


# serve
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=args.port, debug=False)
