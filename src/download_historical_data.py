```python
# src/download_historical_data.py
# Полное скачивание 10 лет daily + 2 года intraday + индикаторы + фичи

import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import time

# Ключи из переменных окружения Railway
API_KEY = os.getenv("POLYGON_API_KEY")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")

if not all([API_KEY, R2_ACCESS_KEY, R2_SECRET_KEY, R2_ACCOUNT_ID, R2_BUCKET]):
    print("Ошибка: не все переменные найдены!")
    exit(1)

print("Ключи найдены. Начинаем полное скачивание...")

client = RESTClient(api_key=API_KEY)
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# Шаг 1: Получаем список всех US stocks tickers (~4000–5000)
print("Получаем список тикеров...")
tickers = []
cursor = None
while True:
    params = {"market": "stocks", "active": True, "limit": 1000}
    if cursor:
        params["cursor"] = cursor
    resp = client.list_tickers(**params)
    tickers.extend([t.ticker for t in resp.results])
    cursor = resp.next_url
    if not cursor:
        break

print(f"Получено {len(tickers)} тикеров.")

# Сохраняем список в отдельный файл в R2 (как CSV с колонкой ticker)
df_tickers = pd.DataFrame({"ticker": tickers})
local_tickers_path = "/app/data/tickers.csv"
os.makedirs("/app/data", exist_ok=True)
df_tickers.to_csv(local_tickers_path, index=False)
r2_tickers_key = "data/tickers.csv"
try:
    s3_client.head_object(Bucket=R2_BUCKET, Key=r2_tickers_key)
    print(f"{r2_tickers_key} уже существует — пропускаем.")
except ClientError:
    s3_client.upload_file(local_tickers_path, R2_BUCKET, r2_tickers_key)
    print(f"Список тикеров сохранён в R2: s3://{R2_BUCKET}/{r2_tickers_key}")

# Параметры
start_date = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")  # 10 лет
end_date = datetime.now().strftime("%Y-%m-%d")

intraday_start_date = (datetime.now() - timedelta(days=365*2)).strftime("%Y-%m-%d")  # 2 года intraday

batch_size = 100  # По 100 тикеров за запуск
rate_sleep = 12  # Секунд паузы (для 5 calls/min limit)

print(f"Daily период: {start_date} — {end_date}")
print(f"Intraday период (1-min): {intraday_start_date} — {end_date}")
print(f"Обработка батчами по {batch_size} тикеров")

# Для RS — скачаем SPY daily
spy_aggs = client.get_aggs("SPY", 1, "day", start_date, end_date, limit=50000)
spy_df = pd.DataFrame([{'timestamp': datetime.fromtimestamp(bar.timestamp / 1000), 'close': bar.close} for bar in spy_aggs])
spy_df.set_index("timestamp", inplace=True)

for i in range(0, len(tickers), batch_size):
    batch_tickers = tickers[i:i+batch_size]
    print(f"\nБатч {i//batch_size +1}: {len(batch_tickers)} тикеров (с {i} по {i+len(batch_tickers)-1})")

    for idx, ticker in enumerate(batch_tickers, 1):
        print(f"[{idx}/{len(batch_tickers)}] Обработка {ticker}...")

        try:
            # Daily данные
            aggs_daily = client.get_aggs(
                ticker=ticker,
                multiplier=1,
                timespan="day",
                from_=start_date,
                to=end_date,
                limit=50000
            )

            if not aggs_daily:
                print(f"Нет daily данных для {ticker}")
                continue

            print(f"Получено {len(aggs_daily)} daily баров для {ticker}")

            data_daily = []
            for bar in aggs_daily:
                timestamp_sec = bar.timestamp / 1000
                date = datetime.fromtimestamp(timestamp_sec)
                data_daily.append({
                    "timestamp": date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume
                })

            df_daily = pd.DataFrame(data_daily)
            df_daily.set_index("timestamp", inplace=True)

            # Добавляем фичи
            df_daily['return'] = df_daily['close'].pct_change()
            df_daily['ma10'] = df_daily['close'].rolling(10).mean()
            df_daily['ma20'] = df_daily['close'].rolling(20).mean()
            df_daily['ma50'] = df_daily['close'].rolling(50).mean()
            df_daily['volatility'] = df_daily['close'].rolling(20).std()
            df_daily['volume_change'] = df_daily['volume'].pct_change()
            df_daily['relative_volume'] = df_daily['volume'] / df_daily['volume'].rolling(20).mean()
            df_daily['rs'] = df_daily['close'] / spy_df['close']

            # Технические индикаторы из Polygon
            rsi = client.get_rsi(ticker, "day", 1, start_date, end_date, limit=50000)
            macd = client.get_macd(ticker, "day", 1, start_date, end_date, limit=50000)
            rsi_df = pd.DataFrame(rsi.values).set_index(pd.to_datetime([datetime.fromtimestamp(t / 1000) for t in rsi.timestamp]))
            df_daily['rsi'] = rsi_df['value']
            macd_df = pd.DataFrame(macd.values).set_index(pd.to_datetime([datetime.fromtimestamp(t / 1000) for t in macd.timestamp]))
            df_daily['macd'] = macd_df['value']

            # Intraday 1-min за 2 года
            aggs_intraday = client.get_aggs(
                ticker=ticker,
                multiplier=1,
                timespan="minute",
                from_=intraday_start_date,
                to=end_date,
                limit=50000
            )

            if aggs_intraday:
                print(f"Получено {len(aggs_intraday)} minute баров для {ticker}")
                data_intraday = []
                for bar in aggs_intraday:
                    timestamp_sec = bar.timestamp / 1000
                    date = datetime.fromtimestamp(timestamp_sec)
                    data_intraday.append({
                        "timestamp": date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume
                    })

                df_intraday = pd.DataFrame(data_intraday)
                df_intraday.set_index("timestamp", inplace=True)

                # Фичи для intraday (упрощённо)
                df_intraday['return'] = df_intraday['close'].pct_change()
                df_intraday['ma10'] = df_intraday['close'].rolling(10).mean()
                df_intraday['volatility'] = df_intraday['close'].rolling(20).std()
                df_intraday['volume_change'] = df_intraday['volume'].pct_change()
                # ... (можно добавить индикаторы, но для intraday Polygon может требовать больше calls)

            else:
                df_intraday = pd.DataFrame()
                print(f"Нет intraday данных для {ticker}")

            # Сохранение
            local_daily_path = f"/app/data/{ticker}_daily_10y.parquet"
            df_daily.to_parquet(local_daily_path)
            r2_daily_key = f"raw/daily/{ticker}_10y.parquet"
            if not check_exists(r2_daily_key):
                s3_client.upload_file(local_daily_path, R2_BUCKET, r2_daily_key)
                print(f"Daily загружен в R2: s3://{R2_BUCKET}/{r2_daily_key}")

            local_intraday_path = f"/app/data/{ticker}_intraday_2y.parquet"
            df_intraday.to_parquet(local_intraday_path)
            r2_intraday_key = f"raw/intraday/{ticker}_2y.parquet"
            if not check_exists(r2_intraday_key):
                s3_client.upload_file(local_intraday_path, R2_BUCKET, r2_intraday_key)
                print(f"Intraday загружен в R2: s3://{R2_BUCKET}/{r2_intraday_key}")

            time.sleep(12)  # Пауза для rate limit (5 calls/min = 12 sec/call)

        except Exception as e:
            print(f"Ошибка для {ticker}: {e}")
            time.sleep(12)

print("\nСкачивание завершено.")

def check_exists(key):
    try:
        s3_client.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise e
