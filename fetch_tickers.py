import os
from polygon import RESTClient
import time
from io import StringIO
import boto3

# Env vars (из Railway)
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

def upload_txt_to_r2(content, key):
    buffer = StringIO(content)
    s3.put_object(Bucket=R2_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"Uploaded {key} to R2")

def get_all_us_stocks(target_count=5500):
    tickers = []
    params = {
        'market': 'stocks',
        'active': True,
        'limit': 1000,
        'order': 'asc',
        'sort': 'ticker'
    }

    for t in client.list_tickers(**params):
        if t.type == 'CS' and t.ticker and len(tickers) < target_count:
            tickers.append(t.ticker)
        if len(tickers) >= target_count:
            break
        time.sleep(0.1)  # Маленькая пауза для rate-limit

    return tickers

if __name__ == "__main__":
    print("Начинаем загрузку списка тикеров...")
    all_tickers = get_all_us_stocks(target_count=5500)
    tickers_text = '\n'.join(all_tickers)
    print(f"Собрано {len(all_tickers)} тикеров. Пример первых 10: {all_tickers[:10]}")

    # Загружаем в R2
    output_key = 'tickers_top_5000.txt'
    upload_txt_to_r2(tickers_text, output_key)
    print("Файл загружен в R2 как tickers_top_5000.txt")
