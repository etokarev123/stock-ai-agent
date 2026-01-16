import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import time

# Env vars
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY')
R2_SECRET_KEY = os.environ.get('R2_SECRET_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = os.environ.get('R2_BUCKET')

client = RESTClient(api_key=POLYGON_API_KEY)

s3 = boto3.client('s3',
                  endpoint_url=R2_ENDPOINT,
                  aws_access_key_id=R2_ACCESS_KEY,
                  aws_secret_access_key=R2_SECRET_KEY)

def file_exists_in_r2(key):
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def upload_parquet(df, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=R2_BUCKET, Key=key, Body=buffer)
    print(f"Загружено: {key} ({len(df)} строк)")

def download_daily(ticker, years_back=10):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years_back*365 + 100)  # с запасом
    aggs = client.get_aggs(
        ticker,
        1, 'day',
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d'),
        limit=50000
    )
    if not aggs:
        return None
    df = pd.DataFrame(aggs)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    return df

# Основная логика
if __name__ == "__main__":
    # Читаем тикеры
    with open('tickers_top_5000.txt', 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]

    print(f"Всего тикеров: {len(tickers)}")

    data_type = 'daily'  # пока только daily
    base_path = f'raw/{data_type}/'

    for i, ticker in enumerate(tickers, 1):
        key = f"{base_path}{ticker}_10y.parquet"
        if file_exists_in_r2(key):
            print(f"[{i}/{len(tickers)}] Пропуск {ticker} — уже существует")
            continue

        print(f"[{i}/{len(tickers)}] Скачиваю {ticker}...")
        try:
            df = download_daily(ticker)
            if df is not None and not df.empty:
                upload_parquet(df, key)
            else:
                print(f"Нет данных для {ticker}")
        except Exception as e:
            print(f"Ошибка {ticker}: {e}")

        time.sleep(0.7)  # ~85 запросов/мин — безопасно даже на платном плане

    print("Скачивание завершено")
