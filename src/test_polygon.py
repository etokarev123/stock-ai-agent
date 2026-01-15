# src/test_polygon.py
# Тест Polygon + сохранение Parquet в Cloudflare R2

import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime
import boto3  # для S3-совместимого R2

# Получаем ключи из переменных Railway
API_KEY = os.getenv("POLYGON_API_KEY")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")

if not all([API_KEY, R2_ACCESS_KEY, R2_SECRET_KEY, R2_ACCOUNT_ID, R2_BUCKET]):
    print("Ошибка: не все переменные R2 или Polygon найдены!")
    exit(1)

print("Ключи найдены, начинаем работу...")

client = RESTClient(api_key=API_KEY)

ticker = "AAPL"
start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

print(f"Скачиваем данные для {ticker} с {start_date} по {end_date}...")

try:
    aggs = client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="day",
        from_=start_date,
        to=end_date,
        limit=50000
    )

    if aggs:
        print(f"Получено {len(aggs)} баров")

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

        print(df.head(5))

        # Сохраняем локально для проверки (опционально)
        local_path = f"/app/data/{ticker}_daily.parquet"
        os.makedirs("/app/data", exist_ok=True)
        df.to_parquet(local_path)
        print(f"Локально сохранено: {local_path}")

        # Загрузка в R2
        s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        )

        r2_path = f"raw/{ticker}_daily.parquet"
        s3_client.upload_file(local_path, R2_BUCKET, r2_path)
        print(f"Файл загружен в R2: s3://{R2_BUCKET}/{r2_path}")

    else:
        print("Нет данных.")

except Exception as e:
    print(f"Ошибка: {e}")

print("Скрипт завершён.")
