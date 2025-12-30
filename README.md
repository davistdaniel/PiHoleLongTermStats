# Pi Hole Long Term Statistics v.0.2.2

A dashboard built with **Dash** and **Plotly** to explore long-term DNS query data from **Pi-hole v.6** FTL database files. Visualize allowed vs blocked domains, top clients, and query trends over time. I originally made this for my own use, but if you find it useful, a ⭐ would be awesome!


**Disclaimer : This is an unofficial, third-party project. The Pi Hole team and the development of [PiHole](https://pi-hole.net/) software is not related to this project.**


<div align="center">
  <img src="https://raw.githubusercontent.com/davistdaniel/PiHoleLongTermStats/main/screenshots/screenshot1.png" alt="Demo Gif" width="600">
</div>

<div align="center">
  <img src="https://raw.githubusercontent.com/davistdaniel/PiHoleLongTermStats/main/screenshots/demo.gif" alt="Demo Gif" width="600">
</div>

See more [screenshots](https://github.com/davistdaniel/PiHoleLongTermStats/blob/main/screenshots/screenshots.md). (The screenshots use a mock database.)

## 🧰 Features
- ➕ Combine multiple databases and visualize consolidated stats.
- 🗂️ Info cards : Query stats, Activity stats, Day and Night stats. See [all supported metrics](https://github.com/davistdaniel/PiHoleLongTermStats?tab=readme-ov-file#supported-metrics).
- 📈 Interactive charts for query trends and client behavior, heatmaps showing day-hour stats, reply-times visualisation and more.
- 🔍 Filter queries by client.
- 🌐 View any number of top blocked/allowed domains, top clients.
- 📅 Analyze queries and compute stats over a custom date range and visualize them.
- ❌ Exclude domains using regex patterns before computing stats.

## 📦 Dependencies

- Python (>=3.11). See `requirements.txt` for dependencies.
- Pi-hole (> v.6) FTL database file (pihole-FTL.db)

## 🚀 Getting Started

> [!WARNING]
> Using your actual Pi-hole FTL db file for querying is **not** recommended and it is advised to use a copy. Place the copy in the project root or specify its path using the `--db_path` argument or `PIHOLE_LT_STATS_DB_PATH` environment variable. In any case, PiHoleLongTermStats does not monitor for changes in the Pi-hole FTL db file even if you mount it.

> [!TIP]
> * You can set up a cron job to periodically copy the FTL database to the `db_path` `PIHOLE_LT_STATS_DB_PATH` location, ensuring your stats are updated without touching the live database. Use the reload button in the dashboard to refresh the stats.
> * Set your timezone (e.g "Europe/Berlin") using `--timezone` or `PIHOLE_LT_STATS_TIMEZONE`.

> [!IMPORTANT]
> * PiHoleLongTermStats reloads the Pi-hole FTL database and recalculates stats whenever the dashboard is refreshed or the reload button is clicked.
> * If no date range is selected, it uses the default period set by `--days` or `PIHOLE_LT_STATS_DAYS`.
> * Large date ranges may lead to increased memory usage.
> * When multiple database files are provided, PiHoleLongTermStats concatenates them into a single dataframe and sorts the combined data by timestamp. Duplicate entries are **not** removed for calculating stats. Consolidating multiple databases can lead to increased memory usage.

> [!TIP]
> There are multiple ways to run the dashboard: using Python or Docker.

### 🐳 Using Docker

<details>
<summary>Click to expand.</summary>


If you have a copy of your `pihole-FTL.db` file, you can quickly run the dashboard using Docker Compose.

1. Make a directory for PiHoleLongtermStats:
    ```bash
    mkdir PiHoleLongTermStats
    cd PiHoleLongTermStats
    ```

2. Make a copy/backup of your `pihole-FTL.db` (**Important!**) and place it in the PiHoleLongTermStats directory:

   ```bash
   # Example: Copy from the default Pi-hole location
   sudo cp /etc/pihole/pihole-FTL.db . 
   # Ensure the user running the app has read permissions
   sudo chown $USER:$USER pihole-FTL.db
   ```
3. Make a docker-compose.yml file in the same directory:
    
    ```docker
    services:
      pihole-long-term-stats:
        image: ghcr.io/davistdaniel/piholelongtermstats:latest
        container_name: pihole-lt-stats
        ports:
          - "9292:9292"  # Map host port to container port
        volumes:
          - ./pihole-FTL.db:/app/pihole-FTL.db:ro  # Path to your Pi-hole DB file (adjust if it's not in current directory)
          # To include additional Pi-hole databases, mount each one similarly:
          #- ./pihole-FTL-2.db:/app/pihole-FTL-2.db:ro
        environment:
          - PIHOLE_LT_STATS_DB_PATH=/app/pihole-FTL.db  # Path inside the container to the mounted DB file
          
          # Provide multiple databases by listing their container paths as a
          # comma-separated string, e.g.:
          #- PIHOLE_LT_STATS_DB_PATH=/app/pihole-FTL.db,/app/pihole-FTL-2.db

          - PIHOLE_LT_STATS_DAYS=31                     # Number of days from now of data to analyze; change if desired
          - PIHOLE_LT_STATS_PORT=9292                   # Port the app listens to inside container; keep in sync with ports mapping
          - PIHOLE_LT_STATS_NCLIENTS=10                 # Number of clients to show in top clients plots
          - PIHOLE_LT_STATS_NDOMAINS=10                 # Number of domains to show in top domains plots
          - PIHOLE_LT_STATS_TIMEZONE=UTC                # timezone for display
          #- PIHOLE_LT_STATS_IGNORE_DOMAINS=.*\.local   # regex patterns for excluding domains. Example: to exclude all .local domains use .*\.local
          
          # To ignore multiple domain patterns, separate each regex with a comma.
          # example definition below ignores: anything ending in .local, anything starting with ads, exactly example.com
          #- PIHOLE_LT_STATS_IGNORE_DOMAINS=.*\.local,^ads\.,^example\.com$

        restart: unless-stopped
    ```
    and run using :

    ```bash
    sudo docker compose up -d
    ```
4. Open your browser and visit [http://localhost:9292](http://localhost:9292)
</details>


### 🐍 Using Python

If installing using python, you can install from this git repo or as a package from PyPi.

#### Install from PyPi :

<details>
<summary>Click to expand.</summary>

1. Install PiHoleLongTermStats from PyPi

    ```bash
    python -m pip install piholelongtermstats
    ````

2. Make a directory for PiHoleLongtermStats (optional):

    ```bash
    mkdir PiHoleLongTermStats
    cd PiHoleLongTermStats
    ```

3. Make a copy/backup of your `pihole-FTL.db` (**Important!**) and place it in the PiHoleLongTermStats directory:

   ```bash
   # Example: Copy from the default Pi-hole location
   sudo cp /etc/pihole/pihole-FTL.db . 
   # Ensure the user running the app has read permissions
   sudo chown $USER:$USER pihole-FTL.db
   ```
4. Run the app:

    ```bash
    piholelongtermstats [OPTIONS]
    ```
5. Open your browser and visit [http://localhost:9292](http://localhost:9292)


    <details>
    <summary><b>Examples for python (Click to expand.)</b></summary>

    To start the dashboard and visualize a single pihole-FTL database file for the last 15 days, with top 20 clients and top 15 domains on port 9292 while ingoring all domains which end with ".local":

    ```bash
    piholelongtermstats --db_path pihole-FTL.db --days 15 --n_clients 20 --n_domains 15 --port 9292 --ignore-domains ".*\.local"
    ```
    To combine two databases, provide paths as comma-separated strings:

    ```bash
    piholelongtermstats --db_path pihole-FTL.db,pihole-FTL-2.db
    ```
    To exclude multiple domains, provide comma-separated regex patterns:

    ```bash
    piholelongtermstats --ignore-domains ".*\.local,.*\.apple.com"
    ```
    </details>

</details>

#### Install from source :

<details>
<summary>Click to expand.</summary>

1. Clone this repository and move into the project folder:

    ```bash
    git clone https://github.com/davistdaniel/PiHoleLongTermStats.git
    cd PiHoleLongTermStats
    ```

2. Install dependencies using pip:

    ```bash
    pip install -r requirements.txt
    ```

3. Make a copy/backup of your `pihole-FTL.db` (**Important!**) and place it in the PiHoleLongTermStats directory:
    ```bash
    # Example: Copy from the default Pi-hole location
    sudo cp /etc/pihole/pihole-FTL.db . 
    # Ensure the user running the app has read permissions
    sudo chown $USER:$USER pihole-FTL.db 
    ```

4. Run the app:

    ```bash
    python piholelongtermstats/app.py [OPTIONS]
    ```

    See the Configuration section below for available options.

5. Open your browser and visit [http://localhost:9292](http://localhost:9292)


    <details>
    <summary><b>Examples for python (Click to expand.)</b></summary>

    To start the dashboard and visualize a single pihole-FTL database file for the last 15 days, with top 20 clients and top 15 domains on port 9292 while ingoring all domains which end with ".local":

    ```bash
    python piholelongtermstats/app.py --db_path pihole-FTL.db --days 15 --n_clients 20 --n_domains 15 --port 9292 --ignore-domains ".*\.local"
    ```
    To combine two databases, provide paths as comma-separated strings:

    ```bash
    python piholelongtermstats/app.py --db_path pihole-FTL.db,pihole-FTL-2.db
    ```
    To exclude multiple domains, provide comma-separated regex patterns:

    ```bash
    python piholelongtermstats/app.py --ignore-domains ".*\.local,.*\.apple.com"
    ```
    </details>
</details>

## ⚙️ Configuration

You can configure the application using command-line arguments or environment variables:

| Command-Line Argument | Environment Variable         | Default Value   | Description                                      |
|-----------------------|------------------------------|-----------------|--------------------------------------------------|
| `--db_path`      | `PIHOLE_LT_STATS_DB_PATH`    | `pihole-FTL.db` | Path to the copied Pi-hole database file. Multiple databases can be combined by providing paths as comma-separated strings.        |
| `--days`         | `PIHOLE_LT_STATS_DAYS`       | `31`           | Number of days back from today to analyze.          |
| `--port`         | `PIHOLE_LT_STATS_PORT`       | `9292`          | Port number to serve the Dash app on.            |
| `--n_clients`         | `PIHOLE_LT_STATS_NCLIENTS`       | `10`          | Number of top clients to show in top clients plots.            |
| `--n_domains`         | `PIHOLE_LT_STATS_NDOMAINS`       | `10`          | Number of top clients to show in top clients plots.            |
| `--port`         | `PIHOLE_LT_STATS_TIMEZONE`       | `UTC`          | Timezone for displaying times in the dashboard.            |
| `--ignore-domains` | `PIHOLE_LT_STATS_IGNORE_DOMAINS` | `""` | Comma-separated regex patterns to exclude domains from from stats (e.g to exlcude all .local domains, use ".*\.local") |

## 🧑‍💻 Contributing

Any contribution, feature ideas or bug fixes are always welcome.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=davistdaniel/PiHoleLongTermStats&type=date&legend=top-left)](https://www.star-history.com/#davistdaniel/PiHoleLongTermStats&type=date&legend=top-left)

## Supported metrics
| Metric | Description |
|--------|-------------|
| Allowed Queries | Total number of queries that were allowed through the Pi-hole. |
| Blocked Queries | Total number of queries that were blocked by the Pi-hole. |
| Top Allowed Domain | The domain which was allowed the most |
| Top Blocked Domain | The domain which was blocked the most |
| Total Unique Clients | The total number of unique devices that made queries. |
| Total Queries | Total number of queries processed by the Pi-hole. |
| Highest number of queries were on | The date with the peak query count. |
| Lowest number of queries were on | The date with the lowest query count. |
| Average reply time | The average time it takes for the Pi-hole to respond to a query. |
| Most Active Hour | The hour with the highest number of queries. |
| Least Active Hour | The hour with the lowest number of queries. |
| Most Active Day of the Week | The day with the highest query activity. |
| Least Active Day of the Week | The day with the lowest query activity. |
| Longest Blocking Streak | The longest consecutive period where queries were blocked. |
| Longest Allowing Streak | The longest consecutive period where queries were allowed. |
| Total queries during the day | Total number of queries made during daytime hours (06:00 to 23:59). |
| Total queries during the night | Total number of queries made during nighttime hours. |
| Top allowed domain during the day | The most allowed domain during the day. |
| Top blocked domain during the day | The most blocked domain during the day. |
| Top allowed domain during the night | The most allowed domain during the night. |
| Top blocked domain during the night | The most blocked domain during the night. |
| Most Persistent Client | The client that made the highest number of queries. |
| Most Diverse Client | The client that queried a blocked domain the most times. |
| Longest Idle Period | The longest period without any queries. |
| Slowest Responding Domain | The domain with the highest average response time. |
| Average Time Between Blocked Queries | Average interval between consecutive blocked queries. |
| Average Time Between Allowed Queries | Average interval between consecutive allowed queries. |


## 📄 License
[MIT](LICENSE)