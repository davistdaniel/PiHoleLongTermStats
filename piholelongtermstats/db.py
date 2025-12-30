## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.2
## License :  MIT

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import psutil
import pandas as pd
import logging
from zoneinfo import ZoneInfo
import gc


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


def get_timestamp_range(days, start_date, end_date, timezone):
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
        f"Trying to read data from PiHole-FTL database(s) for the period ranging from {start_dt} to {end_dt} (TZ: {timezone})..."
    )

    start_timestamp = int(start_dt.astimezone(ZoneInfo("UTC")).timestamp())
    end_timestamp = int(end_dt.astimezone(ZoneInfo("UTC")).timestamp())

    logging.info(
        f"Converted dates ranging from {start_dt} to {end_dt} (TZ: {timezone}) to timestamps in UTC : {start_timestamp} to {end_timestamp}"
    )

    return start_timestamp, end_timestamp


def read_pihole_ftl_db(
    db_paths,
    days=31,
    start_date=None,
    end_date=None,
    chunksize=None,
    timezone="UTC",
):
    """Read the PiHole FTL database"""

    start_timestamp, end_timestamp = get_timestamp_range(
        days, start_date, end_date, timezone
    )

    logging.info(
        f"Reading data from PiHole-FTL database(s) for timestamps ranging from {start_timestamp} to {end_timestamp} (TZ: UTC)..."
    )

    query = f"""
    SELECT id, timestamp, type, status, domain, client, reply_time	 
    FROM queries
    WHERE timestamp >= {start_timestamp} AND timestamp < {end_timestamp};
    """

    for db_idx, db_path in enumerate(db_paths):
        logging.info(
            f"Processing database {db_idx + 1}/{len(db_paths)} at {db_path}..."
        )
        conn = connect_to_sql(db_path)

        chunk_num = 0
        for chunk in pd.read_sql_query(query, conn, chunksize=chunksize[db_idx]):
            chunk_num += 1
            logging.info(
                f"Processing dataframe chunk {chunk_num} from database {db_idx + 1} at {db_path}..."
            )
            yield chunk

        conn.close()
