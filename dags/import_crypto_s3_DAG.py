from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import requests
import pandas as pd
import boto3
import os
from io import StringIO
import sys

# Setup for external script import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append('/opt/airflow/scripts')
from fetch_crypto_metrics import run_fetcher

# Constants
Coin_Ids = [
    'bitcoin', 'ethereum', 'tether', 'ripple', 'binancecoin', 'solana', 'usd-coin', 'dogecoin',
    'cardano', 'tron', 'staked-ether', 'wrapped-bitcoin', 'sui', 'hyperliquid', 'wrapped-steth',
    'chainlink', 'avalanche-2', 'stellar', 'shiba-inu', 'bitcoin-cash'
]

# Timezone
local_tz = pendulum.timezone("America/Denver")  # Change based on your location

# Paths
TEMP_FILE_PATH = "/tmp/raw_crypto_data.csv"
TRANSFORMED_FILE_PATH = "/tmp/transformed_crypto_data.csv"
LOCAL_METRICS_PATH = "/tmp/crypto_metrics.csv"

# Defaults
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG Definition
with DAG(
    dag_id='combined_crypto_etl_and_metrics_pipeline_v5',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 5, 1, tz=local_tz),
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'combined', 'etl', 's3']
) as dag:

    def fetch_crypto_data():
        url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {
            'vs_currency': 'usd',
            'ids': ','.join(Coin_Ids),
            'order': 'market_cap_desc',
            'per_page': len(Coin_Ids),
            'page': 1,
            'sparkline': False
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        df = pd.json_normalize(response.json())
        df.to_csv(TEMP_FILE_PATH, index=False)
        print(f"Data saved to {TEMP_FILE_PATH}")

    def transform_crypto_data():
        df = pd.read_csv(TEMP_FILE_PATH)
        columns_to_drop = ['image'] + [col for col in df.columns if col.startswith('roi')]
        df.drop(columns=columns_to_drop, errors='ignore', inplace=True)
        columns_to_round = [
            'current_price', 'high_24h', 'low_24h', 'price_change_24h',
            'price_change_percentage_24h', 'market_cap_change_24h',
            'market_cap_change_percentage_24h', 'circulating_supply',
            'total_supply', 'max_supply', 'ath', 'ath_change_percentage',
            'atl_change_percentage'
        ]
        for col in columns_to_round:
            if col in df.columns:
                df[col] = df[col].round(4)
        df.to_csv(TRANSFORMED_FILE_PATH, index=False)
        print(f"Transformed data saved to {TRANSFORMED_FILE_PATH}")

    def upload_crypto_data():
        now = pendulum.now(tz=local_tz)
        today = now.strftime("%Y/%m/%d")
        date_str = now.strftime("%Y-%m-%d")

        with open(TRANSFORMED_FILE_PATH, 'r') as file:
            data = file.read()
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
        bucket_name = 's3-api-airflow-project'
        prefix = f"crypto_data/{today}/"
        s3_key = f"{prefix}crypto_data_{date_str}.csv"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        print(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")

    def cleanup_crypto_data():
        now = pendulum.now(tz=local_tz)
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
        bucket_name = 's3-api-airflow-project'
        prefix = 'crypto_data/'
        retention_days = 7
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                try:
                    key = obj['Key']
                    filename = os.path.basename(key)
                    date_str = filename.replace("crypto_data_", "").replace(".csv", "")
                    file_date = pendulum.parse(date_str)
                    if (now - file_date).days > retention_days:
                        s3.delete_object(Bucket=bucket_name, Key=key)
                        print(f"Deleted old file: {key}")
                except Exception as e:
                    print(f"Skipped file {obj['Key']}: {e}")

    def fetch_crypto_metrics():
        df = run_fetcher(Coin_Ids)
        if df.empty:
            raise ValueError("No data fetched from API.")
        df.to_csv(LOCAL_METRICS_PATH, index=False)
        print(f"Saved crypto metrics to {LOCAL_METRICS_PATH}")

    def upload_crypto_metrics():
        now = pendulum.now(tz=local_tz)
        today = now.strftime("%Y/%m/%d")
        date_str = now.strftime("%Y-%m-%d")

        if not os.path.exists(LOCAL_METRICS_PATH):
            raise FileNotFoundError(f"{LOCAL_METRICS_PATH} not found.")
        with open(LOCAL_METRICS_PATH, 'r') as f:
            csv_data = f.read()
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
        bucket_name = 's3-api-airflow-project'
        prefix = f"crypto_metrics/{today}/"
        s3_key = f"{prefix}crypto_metrics_{date_str}.csv"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_data)
        print(f"Uploaded metrics to s3://{bucket_name}/{s3_key}")

    def cleanup_crypto_metrics():
        now = pendulum.now(tz=local_tz)
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
        bucket_name = 's3-api-airflow-project'
        prefix = 'crypto_metrics/'
        retention_days = 7
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                try:
                    key = obj['Key']
                    filename = os.path.basename(key)
                    date_str = filename.replace("crypto_metrics_", "").replace(".csv", "")
                    file_date = pendulum.parse(date_str)
                    if (now - file_date).days > retention_days:
                        s3.delete_object(Bucket=bucket_name, Key=key)
                        print(f"Deleted old metrics file: {key}")
                except Exception as e:
                    print(f"Skipped metrics file {obj['Key']}: {e}")

    def join_and_upload_data():
        now = pendulum.now(tz=local_tz)
        today = now.strftime("%Y/%m/%d")
        date_str = now.strftime("%Y-%m-%d")

        df_current = pd.read_csv(TRANSFORMED_FILE_PATH)
        df_metrics = pd.read_csv(LOCAL_METRICS_PATH)

        df_merged = pd.merge(df_current, df_metrics, left_on='id', right_on='coin_id', how='inner')
        merged_file_path = "/tmp/merged_crypto_data.csv"
        df_merged.to_csv(merged_file_path, index=False)
        print(f"Merged data saved to {merged_file_path}")

        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
        bucket_name = 'crypto-history-from-airflow'
        prefix = f"crypto_history/{today}/"
        s3_key = f"{prefix}merged_crypto_data_{date_str}.csv"
        with open(merged_file_path, 'r') as f:
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=f.read())
        print(f"Uploaded merged data to s3://{bucket_name}/{s3_key}")

    # Task definitions
    crypto_fetch_data = PythonOperator(task_id='crypto_fetch_data', python_callable=fetch_crypto_data)
    crypto_transform_data = PythonOperator(task_id='crypto_transform_data', python_callable=transform_crypto_data)
    crypto_upload_data = PythonOperator(task_id='crypto_upload_data', python_callable=upload_crypto_data)
    crypto_cleanup_data = PythonOperator(task_id='crypto_cleanup_data', python_callable=cleanup_crypto_data)

    metrics_fetch_data = PythonOperator(task_id='metrics_fetch_data', python_callable=fetch_crypto_metrics)
    metrics_upload_data = PythonOperator(task_id='metrics_upload_data', python_callable=upload_crypto_metrics)
    metrics_cleanup_data = PythonOperator(task_id='metrics_cleanup_data', python_callable=cleanup_crypto_metrics)

    join_and_upload = PythonOperator(task_id='join_and_upload', python_callable=join_and_upload_data)

    # Pipeline A
    crypto_fetch_data >> crypto_transform_data >> crypto_upload_data >> crypto_cleanup_data

    # Pipeline B
    metrics_fetch_data >> metrics_upload_data >> metrics_cleanup_data

    # Join step
    [crypto_cleanup_data, metrics_cleanup_data] >> join_and_upload
