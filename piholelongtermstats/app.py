## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.2
## License :  MIT

import os
import gc
import argparse
import logging
import psutil
import plotly.express as px
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State
from zoneinfo import ZoneInfo

from piholelongtermstats.db import read_pihole_ftl_db, connect_to_sql, probe_sample_df
from piholelongtermstats.process import (
    regex_ignore_domains,
    preprocess_df,
    prepare_hourly_aggregated_data,
)
from piholelongtermstats.stats import compute_stats
from piholelongtermstats.plot import (
    generate_plot_data,
    generate_client_activity_over_time,
    generate_queries_over_time,
)

__version__ = "0.2.1"

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

parser.add_argument(
    "--ignore_domains",
    type=str,
    default=os.getenv("PIHOLE_LT_STATS_IGNORE_DOMAINS", ""),
    help="Comma-separated list of domains or regex patterns to ignore. Default: no domains ignored. Env: PIHOLE_LT_STATS_IGNORE_DOMAINS",
)

args = parser.parse_args()

logging.info("Setting environment variables:")
logging.info(f"PIHOLE_LT_STATS_DAYS : {args.days}")
logging.info(f"PIHOLE_LT_STATS_DB_PATH : {args.db_path}")
logging.info(f"PIHOLE_LT_STATS_PORT : {args.port}")
logging.info(f"PIHOLE_LT_STATS_NCLIENTS : {args.n_clients}")
logging.info(f"PIHOLE_LT_STATS_NDOMAINS : {args.n_domains}")
logging.info(f"PIHOLE_LT_STATS_TIMEZONE : {args.timezone}")
logging.info(f"PIHOLE_LT_STATS_IGNORE_DOMAINS : {args.ignore_domains}")
logging.info("Initializing PiHoleLongTermStats Dashboard")


def serve_layout(
    db_path,
    days,
    args,
    max_date_available,
    min_date_available,
    chunksize_list,
    start_date=None,
    end_date=None,
    timezone="UTC",
    ignore_domains="",
):
    """Read pihole ftl db, process data, compute stats"""

    if isinstance(db_path, str):
        db_paths = db_path.split(",")
        logging.info(f"Total number of database files provided : {len(db_paths)}")
    else:
        logging.error(f"db_path parameter must be of type str but got {type(db_path)}")
        raise ValueError(
            f"db_path parameter must be of type str but got {type(db_path)}"
        )

    if ignore_domains != "":
        if isinstance(ignore_domains, str):
            regex_pattern_list = [
                p.strip() for p in ignore_domains.split(",") if p.strip()
            ]
            logging.info(
                f"Total number of regex patterns for ignoring domains : {len(regex_pattern_list)}"
            )
        else:
            logging.error(
                f"ignore_domains parameter must be of type str but got {type(ignore_domains)}"
            )
            raise ValueError(
                f"ignore_domains parameter must be of type str but got {type(ignore_domains)}"
            )

    start_memory = psutil.virtual_memory().available

    df = pd.concat(
        read_pihole_ftl_db(
            db_paths,
            days=days,
            chunksize=chunksize_list,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
        ),
        ignore_index=True,
    )

    logging.info(f"Converted database to a pandas dataframe with {len(df)} rows.")

    if df.empty:
        logging.error(
            "Empty dataframe. No data returned from the database for the given parameters. Try adjusting --days to cover a larger time period."
        )
        raise RuntimeError(
            f"Empty dataframe. No data returned from the database for the given parameters. Database records range from {min_date_available} to {max_date_available}. Try increasing `--days` or the environment variable `PIHOLE_LT_STATS_DAYS`."
        )
    if ignore_domains != "":
        for pattern in regex_pattern_list:
            df = regex_ignore_domains(df, pattern)
            logging.info(
                f"Removed domains matching the regex pattern : {pattern}, number of rows in dataframe : {len(df)}"
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
    plot_data = generate_plot_data(df, args.n_clients, args.n_domains)

    ## agregate data
    hourly_data = prepare_hourly_aggregated_data(df, args.n_clients)

    callback_data = {
        "hourly_agg": hourly_data["hourly_agg"],
        "top_clients": hourly_data["top_clients"],
        "data_span_days": plot_data["data_span_days"],
    }

    # release some memory
    del df, hourly_data
    gc.collect()

    # generate initial plots

    initial_filtered_fig = generate_queries_over_time(
        callback_data=callback_data, client=None
    )
    initial_activity_fig = generate_client_activity_over_time(
        callback_data=callback_data, n_clients=args.n_clients, client=None
    )

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
                    html.H5("Queries from all clients. The data is aggregated hourly."),
                    dcc.Dropdown(
                        options=[
                            {"label": c, "value": c} for c in plot_data["client_list"]
                        ],
                        id="client-filter",
                        placeholder="Select a Client",
                    ),
                    dcc.Graph(id="filtered-view", figure=initial_filtered_fig),
                    html.H2("Client Activity Over Time"),
                    html.H5(
                        "Client acitivity for all clients. The data is aggregated hourly."
                    ),
                    dcc.Graph(id="client-activity-view", figure=initial_activity_fig),
                ],
                className="cardplot",
            ),
            html.Br(),
            html.Div(
                [
                    html.H2("Top Blocked Domains"),
                    html.H5(f"Top {args.n_domains} blocked domains."),
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
                    html.H5(f"Top {args.n_domains} allowed domains."),
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
                    html.H5(f"Top {args.n_clients} clients based on total queries."),
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
                    html.H2("Client-Domain Scatter"),
                    html.H5(
                        "Top domains (irrespective of blocked or allowed) vs. top clients. Size of points correspond to number of queries."
                    ),
                    dcc.Graph(
                        id="client-domain-scatter",
                        figure=px.scatter(
                            plot_data["client_domain_scatter_df"],
                            x="domain",
                            y="client",
                            size="count",
                            color="status_type",
                            color_discrete_map={
                                "Allowed": "#10b981",
                                "Blocked": "#ef4444",
                                "Other": "#b99529",
                            },
                            template="plotly_white",
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
                    html.H2("Day–Hour-Queries Heatmaps"),
                    html.H5(
                        "Heatmap corresponding to number of queries for day of the week vs. hour of the day."
                    ),
                    html.Div(
                        style={
                            "display": "flex",
                            "justify-content": "flex-start",
                            "gap": "5px",
                        },
                        children=[
                            html.Div(
                                [
                                    dcc.Graph(
                                        id="day-hour-heatmap",
                                        figure=px.imshow(
                                            plot_data["day_hour_heatmap"],
                                            labels={
                                                "x": "Hour",
                                                "y": "Day",
                                                "color": "Queries",
                                            },
                                            aspect="auto",
                                            color_continuous_scale="Blues",
                                        ).update_layout(
                                            xaxis=dict(side="bottom"),
                                            margin=dict(l=40, r=40, t=80, b=40),
                                            template="plotly_white",
                                            title="Total",
                                        ),
                                    ),
                                ],
                                style={"width": "33%"},
                            ),
                            html.Div(
                                [
                                    dcc.Graph(
                                        id="allowed-day-hour-heatmap",
                                        figure=px.imshow(
                                            plot_data["allowed_day_hour_heatmap"],
                                            labels={
                                                "x": "Hour",
                                                "y": "Day",
                                                "color": "Queries",
                                            },
                                            aspect="auto",
                                            color_continuous_scale="Greens",
                                        ).update_layout(
                                            xaxis=dict(side="bottom"),
                                            margin=dict(l=40, r=40, t=80, b=40),
                                            template="plotly_white",
                                            title="Allowed",
                                        ),
                                    ),
                                ],
                                style={"width": "33%"},
                            ),
                            html.Div(
                                [
                                    dcc.Graph(
                                        id="blocked-day-hour-heatmap",
                                        figure=px.imshow(
                                            plot_data["blocked_day_hour_heatmap"],
                                            labels={
                                                "x": "Hour",
                                                "y": "Day",
                                                "color": "Queries",
                                            },
                                            aspect="auto",
                                            color_continuous_scale="Reds",
                                        ).update_layout(
                                            xaxis=dict(side="bottom"),
                                            margin=dict(l=40, r=40, t=80, b=40),
                                            template="plotly_white",
                                            title="Blocked",
                                        ),
                                    ),
                                ],
                                style={"width": "33%"},
                            ),
                        ],
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
                            html.H5("Daily average reply time in ms."),
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

app = Dash(__name__)
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
    args=args,
    max_date_available=max(latest_ts_list),
    min_date_available=min(oldest_ts_list),
    chunksize_list=chunksize_list,
    start_date=None,
    end_date=None,
    timezone=args.timezone,
    ignore_domains=args.ignore_domains,
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
    prevent_initial_call=True,
)
def reload_page(n_clicks, start_date, end_date):
    global PHLTS_CALLBACK_DATA

    logging.info(f"Reload button clicked. Selected date range: {start_date, end_date}")

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

    PHLTS_CALLBACK_DATA, layout = serve_layout(
        db_path=args.db_path,
        days=args.days,
        args=args,
        max_date_available=max(latest_ts_list),
        min_date_available=min(oldest_ts_list),
        chunksize_list=chunksize_list,
        start_date=start_date,
        end_date=end_date,
        timezone=args.timezone,
        ignore_domains=args.ignore_domains,
    )

    return layout.children


@app.callback(
    Output("filtered-view", "figure"),
    Input("client-filter", "value"),
    Input("reload-button", "n_clicks"),
    prevent_initial_call=True,
)
def update_filtered_view(client, n_clicks):
    logging.info("Updating Queries over time plot...")
    global PHLTS_CALLBACK_DATA

    fig = generate_queries_over_time(callback_data=PHLTS_CALLBACK_DATA, client=client)

    return fig


@app.callback(
    Output("client-activity-view", "figure"),
    Input("client-filter", "value"),
    Input("reload-button", "n_clicks"),
    prevent_initial_call=True,
)
def update_client_activity(client, n_clicks):
    logging.info("Updating Client activity over time plot...")
    global PHLTS_CALLBACK_DATA

    fig = generate_client_activity_over_time(
        callback_data=PHLTS_CALLBACK_DATA, n_clients=args.n_clients, client=client
    )

    return fig


def run():
    app.run(host="0.0.0.0", port=args.port, debug=False)


# serve
if __name__ == "__main__":
    run()
