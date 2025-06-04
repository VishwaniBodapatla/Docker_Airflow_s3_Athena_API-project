from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
from io import StringIO
import sys

# Add the scripts folder to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append('/opt/airflow/scripts')
from fetch_crypto_metrics import run_fetcher

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

coin_ids = [  # Top 50 coin IDs
    'bitcoin', 'ethereum', 'tether', 'ripple', 'binancecoin', 'solana', 'usd-coin', 'dogecoin',
    'cardano', 'tron', 'staked-ether', 'wrapped-bitcoin', 'sui', 'hyperliquid', 'wrapped-steth',
    'chainlink', 'avalanche-2', 'stellar', 'shiba-inu', 'bitcoin-cash', 'hedera-hashgraph',
    'leo-token', 'the-open-network', 'litecoin', 'polkadot', 'weth', 'monero', 'usds', 'pepe',
    'wrapped-eeth', 'bitget-token', 'binance-bridged-usdt-bnb-smart-chain', 'pi-network',
    'ethena-usde', 'coinbase-wrapped-btc', 'whitebit', 'bittensor', 'uniswap', 'aave', 'near',
    'dai', 'aptos', 'jito-staked-sol', 'ondo-finance', 'okb', 'kaspa', 'internet-computer',
    'ethereum-classic', 'crypto-com-chain', 'blackrock-usd-institutional-digital-liquidity-fund'
]

def fetch_and_upload_to_s3():
    df = run_fetcher(coin_ids)

    if df.empty:
        print("No data fetched.")
        return

    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    bucket_name = 's3-api-airflow-project'
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    s3_key = f"crypto_metrics/crypto_metrics_{timestamp}.csv"

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

    print(f"Uploaded to s3://{bucket_name}/{s3_key}")

with DAG(
    dag_id='daily_crypto_metrics_to_s3',
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'coingecko', 's3']
) as dag:

    fetch_upload_task = PythonOperator(
        task_id='fetch_crypto_metrics_and_upload',
        python_callable=fetch_and_upload_to_s3
    )
