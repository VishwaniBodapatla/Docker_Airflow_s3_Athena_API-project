# 📊 Crypto Data ETL Pipeline with Apache Airflow, Docker & AWS S3

This project implements a robust ETL pipeline using Apache Airflow running in Docker, which extracts cryptocurrency data and historical metrics from the CoinGecko API, transforms the data using Pandas, and uploads the results to Amazon S3.

# 🚀 Features

🔗 Fetches live crypto market data and historical metrics

🧹 Cleans & transforms the data

🐳 Containerized using Docker

⏰ Orchestrated with Apache Airflow (LocalExecutor)

☁️ Uploads to Amazon S3

🧽 Deletes older S3 files beyond 7-day retention

📎 Merges current & historical data into a final S3 output



# 🧱 Tech Stack

Apache Airflow 2.7.3 (Python DAGs)

Docker and docker-compose

Python 3.x, pandas, requests, boto3

AWS S3 for cloud storage

CoinGecko API for data source

PostgreSQL for Airflow backend


📄 License

MIT License – for educational/research use. API usage subject to CoinGecko's Terms.
