# src/test_polygon.py
# Масштабированный тест: несколько тикеров + сохранение в R2 с проверкой

import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Получаем ключи из переменных окружения
API_KEY = os.getenv("POLYGON_API_KEY")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")

if not all([API_KEY, R2_ACCESS_KEY, R2_SECRET_KEY, R2_ACCOUNT_ID, R2_BUCKET]):
    print("Ошибка: не все переменные R2 или Polygon найдены!")
    exit(1)

print("Все ключи найдены, начинаем работу...")

client = RESTClient(api_key=API_KEY)
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# Список тикеров для скачивания
tickers = ["AAPL", "TSLA", "NVDA", "MSFT"]  # Добавь больше по желанию

start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

print(f"Период: {start_date} — {end_date}")

for ticker in tickers:
    print(f"\nОбработка {ticker}...")

    try:
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=start_date,
            to=end_date,
            limit=50000
        )

        if not aggs:
            print(f"Нет данных для {ticker}")
            continue

        print(f"Получено {len(aggs)} баров для {ticker}")

        data = []
        for bar in aggs:
            timestamp_sec = bar.timestamp / 1000
            date = datetime.fromtimestamp(timestamp_sec)
            data.append({
                "timestamp": date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume
            })

        df = pd.DataFrame(data)
        df.set_index("timestamp", inplace=True)

        print(df.head(3))  # Показываем первые 3 строки

        # Локальный путь для временного файла
        local_dir = "/app/data"
        os.makedirs(local_dir, exist_ok=True)
        local_path = f"{local_dir}/{ticker}_daily.parquet"
        df.to_parquet(local_path)
        print(f"Локально сохранено: {local_path}")

        # Имя файла в R2 с датой скачивания
        today_str = datetime.now().strftime("%Y%m%d")
        r2_key = f"raw/{today_str}/{ticker}_daily.parquet"

        # Проверяем, существует ли файл в R2
        try:
            s3_client.head_object(Bucket=R2_BUCKET, Key=r2_key)
            print(f"Файл {r2_key} уже существует в R2 — пропускаем загрузку")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # Файл не существует — загружаем
                s3_client.upload_file(local_path, R2_BUCKET, r2_key)
                print(f"Файл загружен в R2: s3://{R2_BUCKET}/{r2_key}")
            else:
                raise e

    except Exception as e:
        print(f"Ошибка для {ticker}: {e}")

print("\nВсе тикеры обработаны. Скрипт завершён.")
