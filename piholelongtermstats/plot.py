## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.0
## License :  MIT

import logging
import pandas as pd
import gc


def generate_plot_data(df, n_clients, n_domains):
    """Generate plot data"""

    logging.info("Generating plot data...")

    def shorten(s):
        return s if len(s) <= 45 else f"{s[:20]}...{s[-20:]}"

    top_clients = df["client"].value_counts().nlargest(n_clients).index
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
    tmp_blocked = df[df["status_type"] == "Blocked"].copy()
    tmp_blocked["domain"] = tmp_blocked["domain"].apply(shorten)

    blocked_df = (
        tmp_blocked["domain"]
        .value_counts()
        .nlargest(n_domains)
        .reset_index()
        .rename(columns={"index": "Count", "domain": "Domain"})
    )

    tmp_allowed = df[df["status_type"] == "Allowed"].copy()
    tmp_allowed["domain"] = tmp_allowed["domain"].apply(shorten)

    allowed_df = (
        tmp_allowed["domain"]
        .value_counts()
        .nlargest(n_domains)
        .reset_index()
        .rename(columns={"index": "Count", "domain": "Domain"})
    )

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

    # plot data for doman-client scatter. take minimum from n_domains or n_clients
    top_clients = df["client"].value_counts().nlargest(min(n_domains, n_clients)).index
    top_domains = df["domain"].value_counts().nlargest(min(n_domains, n_clients)).index

    df_top = df.loc[
        df["client"].isin(top_clients) & df["domain"].isin(top_domains)
    ].copy()
    df_top.loc[:, "domain"] = df_top["domain"].apply(shorten)

    client_domain_scatter_df = (
        df_top.groupby(["client", "domain", "status_type"])
        .size()
        .reset_index(name="count")
        .sort_values(by="count")
    )

    # heatmap
    order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    day_hour_heatmap = (
        df.groupby(["day_name", "hour"])
        .size()
        .reset_index(name="count")
        .pivot(index="day_name", columns="hour", values="count")
        .fillna(0)
        .reindex(order)
    )

    blocked_day_hour_heatmap = (
        tmp_blocked.groupby(["day_name", "hour"])
        .size()
        .reset_index(name="count")
        .pivot(index="day_name", columns="hour", values="count")
        .fillna(0)
        .reindex(order)
    )

    allowed_day_hour_heatmap = (
        tmp_allowed.groupby(["day_name", "hour"])
        .size()
        .reset_index(name="count")
        .pivot(index="day_name", columns="hour", values="count")
        .fillna(0)
        .reindex(order)
    )

    del df_top, top_clients, top_domains, tmp_allowed, tmp_blocked
    gc.collect()

    logging.info("Plot data generation complete")

    return {
        "top_clients_stacked": top_clients_stacked,
        "blocked_df": blocked_df,
        "allowed_df": allowed_df,
        "reply_time_df": reply_time_df,
        "client_list": client_list,
        "data_span_days": (df["timestamp"].max() - df["timestamp"].min()).days,
        "client_domain_scatter_df": client_domain_scatter_df,
        "day_hour_heatmap": day_hour_heatmap,
        "blocked_day_hour_heatmap": blocked_day_hour_heatmap,
        "allowed_day_hour_heatmap": allowed_day_hour_heatmap,
    }
