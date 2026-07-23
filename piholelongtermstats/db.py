## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.6
## License :  MIT

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import psutil
import pandas as pd
import logging
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


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
    """Calculate safe chunksize based on available memory and retrieve timestamp range.
    
    Analyzes a sample of the database to estimate memory requirements per row
    and retrieves the oldest and latest timestamps in the database.
    """
    sample_query = """SELECT id, timestamp, type, status, domain, client, reply_time
    FROM queries LIMIT 100"""
    sample_df = pd.read_sql_query(sample_query, conn)
    if sample_df.empty:
        raise ValueError("No data from database for the selected time frame.")
    
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

    return chunksize, latest_ts, oldest_ts


def get_timestamp_range(days, start_date, end_date, timezone):
    try:
        tz = ZoneInfo(timezone)
    except ZoneInfoNotFoundError:
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

def get_hostname_map_via_mac(conn):
    """Build ip to hostname and ip to mac dict from PiHole's network tables."""
    
    # join the network and network_adressed table
    query = """
    SELECT n.hwaddr, n.macVendor, na.ip, na.name AS hostname, na.lastSeen
    FROM network_addresses na
    JOIN network n ON n.id = na.network_id
    """
    try:
        df = pd.read_sql_query(query, conn)
    except (sqlite3.OperationalError, pd.errors.DatabaseError) as e:
        logging.warning(
            "Pi-hole network tables not found. Hostname/MAC resolution is unavailable; "
            "clients will fall back to IP address-based resolution where necessary."
        )
        logging.debug(e)
        return {}, {}

    # remove entries with no hostname
    has_hostname = df[df["hostname"].notna() & (df["hostname"] != "")]
    
    # latest first
    canonical_names = (
        has_hostname.sort_values("lastSeen", ascending=False)
        .drop_duplicates(subset="hwaddr")[["hwaddr", "hostname"]]
    )

    # latest ip first, remove rows with missing hostnames
    latest_ip = (
        df.sort_values("lastSeen", ascending=False)
        .drop_duplicates(subset=["hwaddr", "ip"])
        .drop(columns=["hostname"])
    )
    # mac,ip, hostname combined
    merged = latest_ip.merge(canonical_names, on="hwaddr", how="left")

    ip_to_hostname = merged.set_index("ip")["hostname"].to_dict()
    ip_to_mac = merged.set_index("ip")["hwaddr"].to_dict()

    return ip_to_hostname, ip_to_mac

def resolve_clients(df, hostname_map, mac_map, client_id="hostname"):
    """
    Resolve clients based on client-id parameters, default is hostname.
    """

    df["client_ip"] = df["client"]
    df["client_mac"] = df["client_ip"].map(mac_map)
    hostname = df["client_ip"].map(hostname_map)

    if client_id == "ip":
        resolved = df["client_ip"]
    elif client_id == "mac":
        resolved = df["client_mac"].fillna(df["client_ip"])
    elif client_id == "hostname":
        resolved = hostname.fillna(df["client_ip"])
    elif client_id == "hostname_mac":
        resolved = hostname.fillna(df["client_ip"]) + " (" + df["client_mac"].fillna("unknown-mac") + ")"
    elif client_id == "hostname_ip":
        resolved = hostname.where(hostname.isna(), hostname + " (" + df["client_ip"] + ")")
        resolved = resolved.fillna(df["client_ip"])
    elif client_id == "mac_ip":
        resolved = df["client_mac"].fillna("unknown-mac") + " (" + df["client_ip"] + ")"
    else:
        raise ValueError(f"Unknown mode: {client_id}")

    df["client"] = resolved
    return df

def read_pihole_ftl_db(
    db_paths,
    days=31,
    start_date=None,
    end_date=None,
    chunksize=None,
    timezone="UTC",
    client_id="hostname",
):
    """Read the PiHole FTL database"""

    start_timestamp, end_timestamp = get_timestamp_range(
        days, start_date, end_date, timezone
    )

    logging.info(
        f"Reading data from PiHole-FTL database(s) for timestamps ranging from {start_timestamp} to {end_timestamp} (TZ: UTC)..."
    )

    query = """
    SELECT id, timestamp, type, status, domain, client, reply_time	 
    FROM queries
    WHERE timestamp >= ? AND timestamp < ?;
    """
    params = [start_timestamp, end_timestamp]

    for db_idx, db_path in enumerate(db_paths):
        logging.info(
            f"Processing database {db_idx + 1}/{len(db_paths)} at {db_path}..."
        )
        conn = connect_to_sql(db_path)
        ip_to_hostname, ip_to_mac = get_hostname_map_via_mac(conn)
        try:
            chunk_num = 0
            for chunk in pd.read_sql_query(query, conn, params=params, chunksize=chunksize[db_idx]):
                chunk_num += 1
                chunk = resolve_clients(chunk, ip_to_hostname, ip_to_mac, client_id=client_id)
                logging.info(
                    f"Processing dataframe chunk {chunk_num} from database {db_idx + 1} at {db_path}..."
                )
                yield chunk
        finally:
            conn.close()
