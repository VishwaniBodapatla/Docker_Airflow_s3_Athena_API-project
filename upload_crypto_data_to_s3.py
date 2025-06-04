from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import boto3
import os
from io import StringIO

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

Coin_Ids = [
    'bitcoin', 'ethereum', 'tether', 'ripple', 'binancecoin', 'solana', 'usd-coin', 'dogecoin',
    'cardano', 'tron', 'staked-ether', 'wrapped-bitcoin', 'sui', 'hyperliquid', 'wrapped-steth',
    'chainlink', 'avalanche-2', 'stellar', 'shiba-inu', 'bitcoin-cash', 'hedera-hashgraph',
    'leo-token', 'the-open-network', 'litecoin', 'polkadot', 'weth', 'monero', 'usds', 'pepe',
    'wrapped-eeth', 'bitget-token', 'binance-bridged-usdt-bnb-smart-chain', 'pi-network',
    'ethena-usde', 'coinbase-wrapped-btc', 'whitebit', 'bittensor', 'uniswap', 'aave', 'near',
    'dai', 'aptos', 'jito-staked-sol', 'ondo-finance', 'okb', 'kaspa', 'internet-computer',
    'ethereum-classic', 'crypto-com-chain', 'blackrock-usd-institutional-digital-liquidity-fund'
]

TEMP_FILE_PATH = "/tmp/raw_crypto_data.csv"
TRANSFORMED_FILE_PATH = "/tmp/transformed_crypto_data.csv"

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

    # Drop 'image' and all 'roi*' columns
    columns_to_drop = ['image'] + [col for col in df.columns if col.startswith('roi')]
    df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

    # Round specific columns to 4 decimal places
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

def upload_to_s3():
    with open(TRANSFORMED_FILE_PATH, 'r') as file:
        data = file.read()

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    bucket_name = 's3-api-airflow-project'
    timestamp = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"crypto_data/crypto_data_{timestamp}.csv"

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
    print(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")

def cleanup_old_files():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    bucket_name = 's3-api-airflow-project'
    folder_prefix = 'crypto_data/'
    retention_days = 7
    now = datetime.now()

    # List objects in the folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    if 'Contents' not in response:
        print("No files found for cleanup.")
        return

    for obj in response['Contents']:
        key = obj['Key']
        filename = os.path.basename(key)

        # Expect filename like: crypto_data_2025-05-22_10-20-15.csv
        try:
            date_str = filename.replace("crypto_data_", "").replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")
            age_days = (now - file_date).days

            if age_days > retention_days:
                s3.delete_object(Bucket=bucket_name, Key=key)
                print(f"Deleted old file: {key}")

        except Exception as e:
            print(f"Skipped file: {key}, reason: {e}")


with DAG(
    dag_id='split_crypto_etl_pipeline_v2',
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'etl', 's3']
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_crypto_data',
        python_callable=fetch_crypto_data
    )

    task_transform = PythonOperator(
        task_id='transform_crypto_data',
        python_callable=transform_crypto_data
    )

    task_upload = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    task_cleanup = PythonOperator(
        task_id='cleanup_old_files',
        python_callable=cleanup_old_files
    )

    task_fetch >> task_transform >> task_upload >> task_cleanup
