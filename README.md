# Pi Hole Long Term Statistics

A dashboard built with **Dash** and **Plotly** to explore long-term DNS query data from a **Pi-hole v.6** FTL database file. Visualize allowed vs blocked domains, top clients, and query trends over time. 

<center>
<img src="assets/screenshot.gif" alt="Dashboard Screenshot 1" width="600">

<details>
<summary>More screenshots</summary>
<img src="assets/screenshot2.png" alt="Dashboard Screenshot 2" width="600"/>
</details>
</center>

## 🧰 Features
- 🗂️ Info cards : Query stats, Activity stats, Day and Night stats
- 📈 Interactive charts for query trends and client behavior  
- 🔍 Filter queries by client  
- 🌐 View top blocked/allowed domains  
- 📅 Analyze queries over a custom number of past days  

## 📦 Dependencies

- Python 3
- Pi-hole (> v.6) FTL database file (pihole-FTL.db)

## 🚀 Getting Started

There are multiple ways to run the dashboard: using Python, Docker, or Docker Compose.

### Using Python

1. Clone this repository and move into the project folder:

    ```bash
    git clone https://github.com/davistdaniel/PiHoleLongTermStats.git
    cd PiHoleLongTermStats
    ```

2. Install dependencies using pip:

    ```bash
    pip install -r requirements.txt
    ```

3. Make a copy/backup of your `pihole-FTL.db`
    ```bash
    # Example: Copy from the default Pi-hole location
    sudo cp /etc/pihole/pihole-FTL.db . 
    # Ensure the user running the app has read permissions
    sudo chown $USER:$USER pihole-FTL.db 
    ```

> [!WARNING]
> Don't use your actual Pi-hole FTL db file for querying. Place the copy in the project root or specify its path using the `--db_path` argument or `PIHOLE_LT_STATS_DB_PATH` environment variable.

4. Run the app:

    ```bash
    python app.py [OPTIONS]
    ```
    See the Configuration section below for available options.

5. Open your browser and visit [http://localhost:9292](http://localhost:9292)

### 🐳 Using Docker Compose

If you have a copy of your `pihole-FTL.db` file, you can quickly run the dashboard using Docker Compose.

1. Clone this repository:

    ```bash
    git clone https://github.com/davistdaniel/PiHoleLongTermStats.git
    cd PiHoleLongTermStats
    ```

2. Make a copy/backup of your `pihole-FTL.db` (**Important!**) and place it in the project root directory.

   ```bash
   # Example: Copy from the default Pi-hole location
   sudo cp /etc/pihole/pihole-FTL.db . 
   # Ensure the user running the app has read permissions (Docker needs this)
   sudo chown $USER:$USER pihole-FTL.db
   ```

3. You can change the configurations in the docker-compose.yml file or start the dashboard with the default options:

   ```bash
   sudo docker compose up -d
   ```

4. Open your browser and visit [http://localhost:9292](http://localhost:9292)

### Using Docker

1. Clone this repository:

    ```bash
    git clone https://github.com/davistdaniel/PiHoleLongTermStats.git
    cd PiHoleLongTermStats
    ```

2. Make a copy/backup of your `pihole-FTL.db` (**Important!**) and place it in the project root directory.

    ```bash
    # Example: Copy from the default Pi-hole location
    sudo cp /etc/pihole/pihole-FTL.db . 
    # Ensure the user running the app has read permissions (Docker needs this)
    sudo chown $USER:$USER pihole-FTL.db
    ```

3. Build the Docker image:

    ```bash
    sudo docker build -t pihole-long-term-stats .
    ```

4. Run the Docker container, mounting the database file and mapping the port:

    ```bash
    sudo docker run -d --name pihole-LT-stats \
    --restart always \
    -p 9292:9292 \
    -v "$(pwd)/pihole-FTL.db:/app/pihole-FTL.db:ro" \
    pihole-long-term-stats
    ```

    Note: The database is mounted read-only (`:ro`). You can pass configuration options (see below). Ensure the internal path `/app/pihole-FTL.db` is used if setting `PIHOLE_LT_STATS_DB_PATH` or `--db_path` inside Docker.

5. Open your browser and visit [http://localhost:9292](http://localhost:9292)

6. To stop the container :

    ```bash
    sudo docker stop pihole-LT-stats
    sudo docker rm pihole-LT-stats
    ```


## ⚙️ Configuration

You can configure the application using command-line arguments or environment variables:

| Command-Line Argument | Environment Variable         | Default Value   | Description                                      |
|-----------------------|------------------------------|-----------------|--------------------------------------------------|
| `--db_path PATH`      | `PIHOLE_LT_STATS_DB_PATH`    | `pihole-FTL.db` | Path to the copied Pi-hole database file.        |
| `--days DAYS`         | `PIHOLE_LT_STATS_DAYS`       | `365`           | Number of days of past data to analyze.          |
| `--port PORT`         | `PIHOLE_LT_STATS_PORT`       | `9292`          | Port number to serve the Dash app on.            |

## 🧑‍💻 Contributing

Feel free to fork and contribute! Feature ideas or bug fixes are always welcome.

## 📄 License
[MIT](LICENSE)

## 📄 Disclaimer
This is an unofficial, third-party project. The Pi Hole team and the development of Pi Hole software is not related to this project.
