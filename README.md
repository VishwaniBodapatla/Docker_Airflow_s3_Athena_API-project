# 📊 Crypto Data ETL Pipeline with Apache Airflow, Docker & AWS S3

This project is about building a complete data pipeline to collect, process, and analyze cryptocurrency market data using some of the latest tools like Apache Airflow, Docker, AWS S3, and Power BI. The idea is to automatically fetch up-to-date crypto prices and historical metrics from the CoinGecko API, clean and merge the data, store it safely on the cloud, and finally visualize it with interactive dashboards. The whole setup is designed to run anywhere thanks to Docker and Airflow’s workflow management, making the process repeatable and easy to maintain.

![Airflow_docker_AWS_PowerBI_Pipeline](https://github.com/user-attachments/assets/30f2275b-2771-4954-b43a-9571328e5659)



## 🚀 Steps to Run the Project

**Step 1:** Create an AWS account with an IAM user and get the **Access Key** as well as **Secret Access Key**.

**Step 2:** Create three S3 buckets — one for current data, one for historical data, and another for merged data (current + historical). Create JSON policies and attach them to the IAM user to grant access to these buckets.

**Step 3:** Replace credentials in the `.env` file with your **Access Key** and **Secret Access Key**, and update the bucket names in the scripts with your own bucket names.

**Step 4:** Install **Docker**, **Docker Compose**, and **VS Code** on your machine (Linux kernel with **Ubuntu OS** is preferred).

**Step 5:** Open a terminal and initiate the Airflow project using the following commands:

```bash
docker compose up airflow-init
docker compose up -d
```

**Step 6:** Open [http://127.0.0.1:8080/home](http://127.0.0.1:8080/home) in your preferred web browser.

---


 

# 🚀 Features

🔗 Fetches live crypto market data and historical metrics

🧹 Cleans & transforms the data

🐳 Containerized using Docker

⏰ Orchestrated with Apache Airflow (LocalExecutor)

☁️ Uploads to Amazon S3

🧽 Deletes older S3 files beyond 7-day retention

📎 Merges current & historical data into a final S3 output



# Core Files to Run the Project on Another Machine

To run this Airflow-based crypto ETL project elsewhere, a few key files ensure everything works smoothly:

    Dockerfile: Builds a consistent Airflow environment with all needed libraries and dependencies, ensuring the project runs the same on any machine.

    requirements.txt: Lists all Python packages required, so dependencies install correctly and consistently.

    .env: Stores sensitive info like AWS credentials and Airflow login securely outside the code, making configuration easy and safe.

    docker-compose.yaml: Orchestrates all Airflow containers (scheduler, webserver, worker, database) and mounts project folders, allowing quick startup and live code updates with a single command.
   
   # How the DAG Works and Its Tasks
 
    First, it fetches the current cryptocurrency data by calling CoinGecko’s API for the top 20 coins. This data includes prices, market caps, and volumes. The data is then saved locally as a CSV file.

    Next, it runs another task that fetches historical market data and OHLC (Open, High, Low, Close) price metrics by looping over each coin’s ID. This is done through a script in the scripts folder which handles how often we call the API to avoid getting blocked.

    After the data is fetched, the pipeline transforms both current and historical data by cleaning up missing values, rounding numbers for neatness, renaming columns for consistency, and then merging these datasets together. This creates a clean, combined CSV ready for analysis.

    Then the pipeline uploads all these processed files to AWS S3 into organized folders. This way, data is stored safely and can be accessed later for reporting or other use cases.

    Finally, it cleans up old data in S3 by deleting files older than seven days, which keeps storage costs low and the bucket tidy.
    
    Besides this main workflow, there’s a secondary process that uploads older historical data to a separate S3 bucket. After both pipelines finish, a final task merges the data again and uploads the consolidated results to another dedicated bucket. This setup helps keep raw data, processed data, and merged data well separated for better management.
    
![ChatGPT Image May 22, 2025, 07_48_23 PM](https://github.com/user-attachments/assets/cf1ed55e-1101-4b3c-8e09-b8a27d89b524)

![Airflow_taskFlow](https://github.com/user-attachments/assets/6daad253-0174-4d9c-9732-05b6b7ef7e07)



# About Fetching Data and Handling API Limits

All the data fetching logic is inside the scripts/fetch_crypto_metrics.py file. CoinGecko, like many free APIs, limits how many requests you can make — in this case, 50 calls per minute. To respect this, the script keeps track of how many API calls it’s made and pauses if it’s about to exceed the limit. This prevents the pipeline from breaking due to rate limiting errors.

The script fetches data coin by coin, collects all responses into one big dataset, and then saves it to CSV. This modular and smart approach helps keep the code clean, reliable, and efficient.
    

📄 License

MIT License – for educational/research use. API usage subject to CoinGecko's Terms.
