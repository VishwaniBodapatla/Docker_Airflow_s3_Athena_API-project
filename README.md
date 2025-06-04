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
 
    a. First, it fetches the current cryptocurrency data by calling CoinGecko’s API for the top 20 coins. This data includes prices, market caps, and volumes. The data is then saved locally as a CSV file.

    b. Next, it runs another task that fetches historical market data and OHLC (Open, High, Low, Close) price metrics by looping over each coin’s ID. This is done through a script in the scripts folder which handles how often we call the API to avoid getting blocked.

    c. After the data is fetched, the pipeline transforms both current and historical data by cleaning up missing values, rounding numbers for neatness, renaming columns for consistency, and then merging these datasets together. This creates a clean, combined CSV ready for analysis.

    d. Then the pipeline uploads all these processed files to AWS S3 into organized folders. This way, data is stored safely and can be accessed later for reporting or other use cases.

    e. Finally, it cleans up old data in S3 by deleting files older than seven days, which keeps storage costs low and the bucket tidy.
    
    f.Besides this main workflow, there’s a secondary process that uploads older historical data to a separate S3 bucket. After both pipelines finish, a final task merges the data again and uploads the consolidated results to another dedicated bucket. This setup helps keep raw data, processed data, and merged data well separated for better management.
    
<img src="https://github.com/user-attachments/assets/cf1ed55e-1101-4b3c-8e09-b8a27d89b524" width="400"/>

![Airflow_taskFlow](https://github.com/user-attachments/assets/6daad253-0174-4d9c-9732-05b6b7ef7e07)



# Integration with Athena and Power BI
Though not directly included in the project files, I leveraged AWS Athena and Power BI to extend the analytical capabilities of this ETL pipeline:

Athena was used to run SQL queries directly against the S3 data lake containing the uploaded crypto datasets. I registered the S3 buckets as external tables in Athena’s Glue Data Catalog and wrote optimized SQL queries to extract time-series OHLC data and aggregate metrics for various coins.

Using the ODBC connector, I connected Power BI Desktop to Athena, enabling live querying of large-scale historical and current data without data duplication or manual exports.

In Power BI, I designed interactive visualizations:

Candlestick charts to display historical OHLC price movements per coin, supporting detailed trend analysis.

Line charts showing overall market trends with slicers allowing users to filter by coin symbol interactively.

Column charts comparing the prices of multiple coins side-by-side for snapshot comparisons.

The use of Athena and Power BI together demonstrates advanced knowledge in cloud data warehousing, SQL querying over serverless infrastructures, and dynamic business intelligence visualization, bridging the gap between raw data ingestion and actionable insights.

![PowerBIDashboardAthena](https://github.com/user-attachments/assets/28c20945-35be-4995-b4e0-d37ed203573d)

    

📄 License

MIT License – for educational/research use. API usage subject to CoinGecko's Terms.
