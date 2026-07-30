# Configuration Guide

This guide is for configuring **PiHoleLongTermStats** using environment variables (ideal for Docker Compose) or command-line arguments (ideal for direct Python execution).

## Configuration Options

| Command-Line Argument | Environment Variable | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `--db_path` | `PIHOLE_LT_STATS_DB_PATH` | `pihole-FTL.db` | Path to the copy of the Pi-hole database file. To consolidate multiple databases, specify paths as comma-separated values. |
| `--days` | `PIHOLE_LT_STATS_DAYS` | `31` | Number of days of historical data to load relative to today. |
| `--port` | `PIHOLE_LT_STATS_PORT` | `9292` | Network port for the Dash server. |
| `--n_clients` | `PIHOLE_LT_STATS_NCLIENTS` | `10` | Maximum number of top clients to display in top clients plots. |
| `--n_domains` | `PIHOLE_LT_STATS_NDOMAINS` | `10` | Maximum number of top domains to display in top domains plots. |
| `--timezone` | `PIHOLE_LT_STATS_TIMEZONE` | `UTC` | Timezone database string (e.g., `Europe/Berlin`, `America/New_York`) to map and display query times. |
| `--ignore-domains` | `PIHOLE_LT_STATS_IGNORE_DOMAINS` | `""` | Comma-separated list of regex patterns for excluding domains from statistic counts (e.g., `.*\.local,^ads\.`). |
| `--client_id` | `PIHOLE_LT_STATS_CLIENT_ID` | `hostname` | Method used to identify, group, and format clients. Supported modes: `hostname`, `mac`, `hostname_mac`, `ip`, `hostname_ip`, `mac_ip`. |

---

## Configuration Details

### Database Path (`--db_path` / `PIHOLE_LT_STATS_DB_PATH`)
Specifies the path to the `pihole-FTL.db` database. 

Multiple databases can be consolidated using a comma-separated list:
```bash
# Python
piholelongtermstats --db_path "pihole-FTL.db,pihole-FTL-2.db"

```

```yaml
# Docker environment
    volumes:
      - ./:/db_dir
      # If your second database is in a different host folder, mount it to a separate container path:
      #- /path/to/second/folder:/db_dir_2
    environment:
      # If all databases are in the same folder, you only need the single volume mount above:
      PIHOLE_LT_STATS_DB_PATH=/db_dir/pihole-FTL.db,/db_dir/pihole-FTL-2.db
      
      # If they are in separate folders:
      # PIHOLE_LT_STATS_DB_PATH=/db_dir/pihole-FTL.db,/db_dir_2/pihole-FTL-2.db
```

> [!NOTE]
> **Docker Volume Mounts and Database Updates:**
> To ensure the **Reload** button works reliably when you replace your database file in a Docker setup, it is recommended to mount the host directory containing the database (e.g. `.:/db_dir`) rather than the database file directly (e.g. `./pihole-FTL.db:/app/pihole-FTL.db:ro`). 
> 


### Days (`--days` / `PIHOLE_LT_STATS_DAYS`)

Number of days of historical data to load relative to today.

If you get empty database errors, try increasing days to include older data. Verify that the timezone is set correctly. Note that increasing this value leads to higher memory consumption during application startup since a larger time range must be processed.

```bash
# Python
piholelongtermstats --days 91

```

```yaml
# Docker environment
    environment:
      PIHOLE_LT_STATS_DAYS=91

```

### Port (`--port` / `PIHOLE_LT_STATS_PORT`)

Specifies the network port on which the Dash web application will listen.

```bash
# Python
piholelongtermstats --port 9292

```

```yaml
# Docker environment
    ports:
      - "9292:9292"
    environment:
      PIHOLE_LT_STATS_PORT=9292

```

### Top Items Limit (`--n_clients`, `--n_domains` / `PIHOLE_LT_STATS_NCLIENTS`, `PIHOLE_LT_STATS_NDOMAINS`)

Controls the maximum number of items featured in the top rankings plots (e.g., top domains or top clients).

```bash
# Python
piholelongtermstats --n_clients 15 --n_domains 20

```

```yaml
# Docker environment
    environment:
      PIHOLE_LT_STATS_NCLIENTS=15
      PIHOLE_LT_STATS_NDOMAINS=20

```

### Timezone (`--timezone` / `PIHOLE_LT_STATS_TIMEZONE`)

Sets the timezone used to parse database timestamps and display time-series graphs. Uses standard IANA timezone database strings (e.g., `Europe/Berlin`, `America/New_York`, `UTC`).

```bash
# Python
piholelongtermstats --timezone "Europe/Berlin"

```

```yaml
# Docker environment
    environment:
      PIHOLE_LT_STATS_TIMEZONE=Europe/Berlin

```

### Client ID Resolution Mode (`--client_id` / `PIHOLE_LT_STATS_CLIENT_ID`)

Deafult is `hostname`. Determines how client endpoints are identified, grouped, and displayed across graphs and tables:

* `hostname`: Uses the device name if mapped in Pi-hole; falls back to IP address.
* `mac`: Uses the hardware MAC address of the device; falls back to IP address.
* `hostname_mac`: Displayed as `hostname (mac)`.
* `ip`: Standard IPv4 or IPv6 address.
* `hostname_ip`: Displayed as `hostname (ip)`.
* `mac_ip`: Displayed as `mac (ip)`.

##### Notes

- For accuracy in stats, it is best to use `mac` but for readability, this is not optimal. 
- A better balance of accuracy and readability is using `hostname`. 
- Using `ip` may lead to inaccurate stats, since devices may not have static IP adresses.
- `ip` is used as fallback if hostnames or mac aren't available.
- Long hostnames could lead to plot labels taking up extra vertical space, overlapping, or getting truncated on the dashboard charts.

```bash
# Python
piholelongtermstats --client_id "ip"

```

```yaml
# Docker environment
    environment:
      PIHOLE_LT_STATS_CLIENT_ID=ip

```

### Ignoring Domains (`--ignore-domains` / `PIHOLE_LT_STATS_IGNORE_DOMAINS`)

Allows filtering out noisy background domains using regular expressions. Multiple regex patterns can be separated by commas:

```bash
# Ignore local domains and any domains starting with "ads."
piholelongtermstats --ignore-domains ".*\.local,^ads\."

```

```yaml
# Docker environment
    environment:
      PIHOLE_LT_STATS_IGNORE_DOMAINS=.*\.local,^ads\.

```

---
