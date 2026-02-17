## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.3
## License :  MIT

import logging
import gc


def _main_heading_stats(stats, df, min_date_available, max_date_available):
    """
    Stats displayed under the main heading.
    """
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

    return stats


def _query_stats(stats, df):
    """
    Compute query related stats such as total queires, blocked/allowed counts etc.
    """
    stats["total_queries"] = len(df)
    stats["blocked_count"] = len(df[df["status_type"] == "Blocked"])
    stats["allowed_count"] = len(df[df["status_type"] == "Allowed"])
    
    if stats["total_queries"] > 0:
        stats["blocked_pct"] = (stats["blocked_count"] / stats["total_queries"]) * 100
        stats["allowed_pct"] = (stats["allowed_count"] / stats["total_queries"]) * 100
    else:
        stats["blocked_pct"] = 0.0
        stats["allowed_pct"] = 0.0
    
    logging.info("Computed data for query metrics.")

    return stats


def _top_clients_stats(stats, df):
    """
    Compute top allowed/blocked client.
    """
    # top clients
    client_counts = df["client"].value_counts()
    stats["top_client"] = client_counts.idxmax() if not client_counts.empty else "N/A"
    
    allowed_clients = df[df["status_type"] == "Allowed"]["client"].value_counts()
    stats["top_allowed_client"] = allowed_clients.idxmax() if not allowed_clients.empty else "N/A"
    
    blocked_clients = df[df["status_type"] == "Blocked"]["client"].value_counts()
    stats["top_blocked_client"] = blocked_clients.idxmax() if not blocked_clients.empty else "N/A"
    
    logging.info("Computed data for top clients.")

    return stats


def _domain_stats(stats, df):
    """
    Compute domain related stats
    """
    allowed_domains = df[df["status_type"] == "Allowed"]["domain"].value_counts()
    blocked_domains = df[df["status_type"] == "Blocked"]["domain"].value_counts()
    
    stats["top_allowed_domain"] = allowed_domains.idxmax() if not allowed_domains.empty else "N/A"
    stats["top_blocked_domain"] = blocked_domains.idxmax() if not blocked_domains.empty else "N/A"
    
    stats["top_allowed_domain_count"] = (
        len(df[df["domain"] == stats["top_allowed_domain"]]) 
        if stats["top_allowed_domain"] != "N/A" else 0
    )
    stats["top_blocked_domain_count"] = (
        len(df[df["domain"] == stats["top_blocked_domain"]]) 
        if stats["top_blocked_domain"] != "N/A" else 0
    )
    
    allowed_domain_clients = (
        df[
            (df["status_type"] == "Allowed")
            & (df["domain"] == stats["top_allowed_domain"])
        ]["client"]
        .value_counts()
    )
    stats["top_allowed_domain_client"] = (
        allowed_domain_clients.idxmax() if not allowed_domain_clients.empty else "N/A"
    )
    
    blocked_domain_clients = (
        df[
            (df["status_type"] == "Blocked")
            & (df["domain"] == stats["top_blocked_domain"])
        ]["client"]
        .value_counts()
    )
    stats["top_blocked_domain_client"] = (
        blocked_domain_clients.idxmax() if not blocked_domain_clients.empty else "N/A"
    )
    logging.info("Computed data for domains.")

    return stats


def _most_persistent_stats(stats, df):
    """
    Compute most persistent client despite being blocked.
    """
    persistence = (
        (df[df["status_type"] == "Blocked"])
        .groupby(["client", "domain"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    
    if not persistence.empty:
        most_persistent_row = persistence.iloc[0]
        stats["most_persistent_client"] = most_persistent_row["client"]
        stats["blocked_domain"] = most_persistent_row["domain"]
        stats["repeat_attempts"] = most_persistent_row["count"]
    else:
        stats["most_persistent_client"] = "N/A"
        stats["blocked_domain"] = "N/A"
        stats["repeat_attempts"] = 0
    
    logging.info("Computed data for most persistent client.")

    del persistence
    gc.collect()

    return stats


def _activity_stats(stats, df):
    """
    Compute activity stats based on day of week, hour and date.
    """
    # activity stats based on date
    query_date_counts = df.groupby("date")["domain"].count()
    blocked_date_counts = (
        df[df["status_type"] == "Blocked"].groupby("date")["domain"].count()
    )
    allowed_date_counts = (
        df[df["status_type"] == "Allowed"].groupby("date")["domain"].count()
    )
    
    stats["date_most_queries"] = (
        query_date_counts.idxmax().strftime("%d %B %Y") 
        if not query_date_counts.empty else "N/A"
    )
    stats["date_most_blocked"] = (
        blocked_date_counts.idxmax().strftime("%d %B %Y") 
        if not blocked_date_counts.empty else "N/A"
    )
    stats["date_most_allowed"] = (
        allowed_date_counts.idxmax().strftime("%d %B %Y") 
        if not allowed_date_counts.empty else "N/A"
    )
    stats["date_least_queries"] = (
        query_date_counts.idxmin().strftime("%d %B %Y") 
        if not query_date_counts.empty else "N/A"
    )
    stats["date_least_blocked"] = (
        blocked_date_counts.idxmin().strftime("%d %B %Y") 
        if not blocked_date_counts.empty else "N/A"
    )
    stats["date_least_allowed"] = (
        allowed_date_counts.idxmin().strftime("%d %B %Y") 
        if not allowed_date_counts.empty else "N/A"
    )
    logging.info("Computed data for activity stats based on date.")

    # activity stats based on hour
    hourly_counts = df.groupby("hour").size()
    stats["most_active_hour"] = hourly_counts.idxmax() if not hourly_counts.empty else 0
    stats["least_active_hour"] = hourly_counts.idxmin() if not hourly_counts.empty else 0
    stats["avg_queries_most"] = int(hourly_counts.max()) if not hourly_counts.empty else 0
    stats["avg_queries_least"] = int(hourly_counts.min()) if not hourly_counts.empty else 0
    logging.info("Computed data for activity stats based based on hour.")

    # activity stats based on day
    daily_counts = (
        df.groupby(["date", "day_name"]).size().reset_index(name="query_count")
    )
    avg = daily_counts.groupby("day_name")["query_count"].mean()
    
    stats["most_active_day"] = avg.idxmax() if not avg.empty else "N/A"
    stats["most_active_avg"] = int(avg.max()) if not avg.empty else 0
    stats["least_active_day"] = avg.idxmin() if not avg.empty else "N/A"
    stats["least_active_avg"] = int(avg.min()) if not avg.empty else 0
    logging.info("Computed data for activity stats based based on day.")

    return stats


def _day_night_stats(stats, df):
    """
    Compute client and domain stats for day and night
    """
    day_df = df[df["day_period"] == "Day"]
    night_df = df[df["day_period"] == "Night"]

    # Day stats
    stats["day_total_queries"] = len(day_df)
    
    day_clients = day_df["client"].value_counts()
    stats["day_top_client"] = day_clients.idxmax() if not day_clients.empty else "N/A"
    
    day_allowed_clients = day_df[day_df["status_type"] == "Allowed"]["client"].value_counts()
    stats["day_top_allowed_client"] = day_allowed_clients.idxmax() if not day_allowed_clients.empty else "N/A"
    
    day_blocked_clients = day_df[day_df["status_type"] == "Blocked"]["client"].value_counts()
    stats["day_top_blocked_client"] = day_blocked_clients.idxmax() if not day_blocked_clients.empty else "N/A"
    
    day_allowed_domains = day_df[day_df["status_type"] == "Allowed"]["domain"].value_counts()
    stats["day_top_allowed_domain"] = day_allowed_domains.idxmax() if not day_allowed_domains.empty else "N/A"
    
    day_blocked_domains = day_df[day_df["status_type"] == "Blocked"]["domain"].value_counts()
    stats["day_top_blocked_domain"] = day_blocked_domains.idxmax() if not day_blocked_domains.empty else "N/A"
    
    stats["day_top_allowed_domain_count"] = (
        len(day_df[day_df["domain"] == stats["day_top_allowed_domain"]])
        if stats["day_top_allowed_domain"] != "N/A" else 0
    )
    stats["day_top_blocked_domain_count"] = (
        len(day_df[day_df["domain"] == stats["day_top_blocked_domain"]])
        if stats["day_top_blocked_domain"] != "N/A" else 0
    )
    
    day_allowed_domain_clients = (
        day_df[
            (day_df["status_type"] == "Allowed")
            & (day_df["domain"] == stats["day_top_allowed_domain"])
        ]["client"]
        .value_counts()
    )
    stats["day_top_allowed_domain_client"] = (
        day_allowed_domain_clients.idxmax() if not day_allowed_domain_clients.empty else "N/A"
    )
    
    day_blocked_domain_clients = (
        day_df[
            (day_df["status_type"] == "Blocked")
            & (day_df["domain"] == stats["day_top_blocked_domain"])
        ]["client"]
        .value_counts()
    )
    stats["day_top_blocked_domain_client"] = (
        day_blocked_domain_clients.idxmax() if not day_blocked_domain_clients.empty else "N/A"
    )

    # Night stats
    stats["night_total_queries"] = len(night_df)
    
    night_clients = night_df["client"].value_counts()
    stats["night_top_client"] = night_clients.idxmax() if not night_clients.empty else "N/A"
    
    night_allowed_clients = night_df[night_df["status_type"] == "Allowed"]["client"].value_counts()
    stats["night_top_allowed_client"] = night_allowed_clients.idxmax() if not night_allowed_clients.empty else "N/A"
    
    night_blocked_clients = night_df[night_df["status_type"] == "Blocked"]["client"].value_counts()
    stats["night_top_blocked_client"] = night_blocked_clients.idxmax() if not night_blocked_clients.empty else "N/A"
    
    night_allowed_domains = night_df[night_df["status_type"] == "Allowed"]["domain"].value_counts()
    stats["night_top_allowed_domain"] = night_allowed_domains.idxmax() if not night_allowed_domains.empty else "N/A"
    
    night_blocked_domains = night_df[night_df["status_type"] == "Blocked"]["domain"].value_counts()
    stats["night_top_blocked_domain"] = night_blocked_domains.idxmax() if not night_blocked_domains.empty else "N/A"
    
    stats["night_top_allowed_domain_count"] = (
        len(night_df[night_df["domain"] == stats["night_top_allowed_domain"]])
        if stats["night_top_allowed_domain"] != "N/A" else 0
    )
    stats["night_top_blocked_domain_count"] = (
        len(night_df[night_df["domain"] == stats["night_top_blocked_domain"]])
        if stats["night_top_blocked_domain"] != "N/A" else 0
    )
    
    night_allowed_domain_clients = (
        night_df[
            (night_df["status_type"] == "Allowed")
            & (night_df["domain"] == stats["night_top_allowed_domain"])
        ]["client"]
        .value_counts()
    )
    stats["night_top_allowed_domain_client"] = (
        night_allowed_domain_clients.idxmax() if not night_allowed_domain_clients.empty else "N/A"
    )
    
    night_blocked_domain_clients = (
        night_df[
            (night_df["status_type"] == "Blocked")
            & (night_df["domain"] == stats["night_top_blocked_domain"])
        ]["client"]
        .value_counts()
    )
    stats["night_top_blocked_domain_client"] = (
        night_blocked_domain_clients.idxmax() if not night_blocked_domain_clients.empty else "N/A"
    )

    logging.info("Computed data for day and night stats.")

    del day_df, night_df
    gc.collect()

    return stats


def _streak_stats(stats, df_sorted):
    """
    Compute allowed and blocked streak stats
    """
    blocked_groups = df_sorted[df_sorted["is_blocked"]].groupby("blocked_group")
    allowed_groups = df_sorted[df_sorted["is_allowed"]].groupby("allowed_group")
    streaks_blocked = blocked_groups.agg(
        streak_length=("is_blocked", "size"), start_time=("timestamp", "first")
    )
    streaks_allowed = allowed_groups.agg(
        streak_length=("is_allowed", "size"), start_time=("timestamp", "first")
    )

    if not streaks_blocked.empty:
        longest_streak_blocked = streaks_blocked.loc[
            streaks_blocked["streak_length"].idxmax()
        ]
        stats["longest_streak_length_blocked"] = int(longest_streak_blocked["streak_length"])
        stats["streak_date_blocked"] = longest_streak_blocked["start_time"].strftime("%d %B %Y")
        stats["streak_hour_blocked"] = longest_streak_blocked["start_time"].strftime("%H:%M")
    else:
        stats["longest_streak_length_blocked"] = 0
        stats["streak_date_blocked"] = "N/A"
        stats["streak_hour_blocked"] = "N/A"

    if not streaks_allowed.empty:
        longest_streak_allowed = streaks_allowed.loc[
            streaks_allowed["streak_length"].idxmax()
        ]
        stats["longest_streak_length_allowed"] = int(longest_streak_allowed["streak_length"])
        stats["streak_date_allowed"] = longest_streak_allowed["start_time"].strftime("%d %B %Y")
        stats["streak_hour_allowed"] = longest_streak_allowed["start_time"].strftime("%H:%M")
    else:
        stats["longest_streak_length_allowed"] = 0
        stats["streak_date_allowed"] = "N/A"
        stats["streak_hour_allowed"] = "N/A"

    logging.info("Computed data for streak stats.")

    del blocked_groups, allowed_groups, streaks_blocked, streaks_allowed
    gc.collect()

    return stats


def _idle_time_stats(stats, df_sorted):
    """
    compute idle time stats
    """
    idle_gaps = df_sorted["idle_gap"].dropna()
    
    if not idle_gaps.empty:
        max_idle_ms = idle_gaps.max()
        max_idle_idx = idle_gaps.idxmax()
    else:
        max_idle_ms = None
        max_idle_idx = None

    blocked = df_sorted[df_sorted["status_type"] == "Blocked"]
    blocked_times = blocked["timestamp"].diff().dt.total_seconds().dropna()
    avg_time_between_blocked = blocked_times.mean() if not blocked_times.empty else None

    allowed = df_sorted[df_sorted["status_type"] == "Allowed"]
    allowed_times = allowed["timestamp"].diff().dt.total_seconds().dropna()
    avg_time_between_allowed = allowed_times.mean() if not allowed_times.empty else None

    if max_idle_idx is not None and max_idle_idx > 0:
        before_gap = (
            df_sorted.loc[max_idle_idx - 1, "timestamp"].strftime("%d-%b %Y %H:%M:%S.%f")[:-4]
        )
    else:
        before_gap = None
    
    if max_idle_idx is not None:
        after_gap = df_sorted.loc[max_idle_idx, "timestamp"].strftime(
            "%d-%b %Y %H:%M:%S.%f"
        )[:-4]
    else:
        after_gap = None

    stats["max_idle_ms"] = max_idle_ms
    stats["avg_time_between_blocked"] = avg_time_between_blocked
    stats["avg_time_between_allowed"] = avg_time_between_allowed
    stats["before_gap"] = before_gap
    stats["after_gap"] = after_gap

    logging.info("Computed data for time stats.")

    del allowed, blocked
    gc.collect()

    return stats


def _unique_stats(stats, df):
    """
    Compute unique domains and clients
    """
    stats["unique_domains"] = df["domain"].nunique()
    stats["unique_clients"] = df["client"].nunique()
    diverse_client_df = (
        df.groupby("client")["domain"].nunique().reset_index(name="unique_domains")
    )
    diverse_client_df = diverse_client_df.sort_values("unique_domains", ascending=False)
    
    if not diverse_client_df.empty:
        stats["most_diverse_client"] = diverse_client_df.iloc[0]["client"]
        stats["unique_domains_count"] = int(diverse_client_df.iloc[0]["unique_domains"])
    else:
        stats["most_diverse_client"] = "N/A"
        stats["unique_domains_count"] = 0

    del diverse_client_df
    gc.collect()

    return stats


def _reply_time_stats(stats, df):
    """
    Compute reply time related stats.
    """
    reply_times = df["reply_time"].dropna()
    
    if not reply_times.empty:
        stats["avg_reply_time"] = round(reply_times.abs().mean() * 1000, 3)
        stats["max_reply_time"] = round(reply_times.abs().max() * 1000, 3)
        stats["min_reply_time"] = round(reply_times.abs().min() * 1000, 3)
    else:
        stats["avg_reply_time"] = 0.0
        stats["max_reply_time"] = 0.0
        stats["min_reply_time"] = 0.0

    avg_reply_times = df.groupby("domain")["reply_time"].mean().reset_index()
    
    if not avg_reply_times.empty:
        slowest_domain_row = avg_reply_times.sort_values(
            "reply_time", ascending=False
        ).iloc[0]
        stats["slowest_domain"] = slowest_domain_row["domain"]
        stats["slowest_avg_reply_time"] = slowest_domain_row["reply_time"]
    else:
        stats["slowest_domain"] = "N/A"
        stats["slowest_avg_reply_time"] = 0.0
    
    logging.info("Computed data for reply time stats.")

    return stats


def compute_stats(df, min_date_available, max_date_available):
    """Compute all statistics and return them as a dictionary"""

    logging.info("Started computing stats...")
    stats = {}

    stats = _main_heading_stats(stats, df, min_date_available, max_date_available)

    # query stats
    stats = _query_stats(stats, df)

    # top clients
    stats = _top_clients_stats(stats, df)

    # domain stats
    stats = _domain_stats(stats, df)

    # most persistent client despite being blocked
    stats = _most_persistent_stats(stats, df)

    # activity stats
    stats = _activity_stats(stats, df)

    # day-night stats
    stats = _day_night_stats(stats, df)

    # unique stats
    stats = _unique_stats(stats, df)

    # reply time stats
    stats = _reply_time_stats(stats, df)

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

    # allowed and blocked streaks
    stats = _streak_stats(stats, df_sorted)

    # idle time stats
    stats = _idle_time_stats(stats, df_sorted)

    logging.info("All stats computed.")
    # release some memory
    del df_sorted
    gc.collect()

    return stats
