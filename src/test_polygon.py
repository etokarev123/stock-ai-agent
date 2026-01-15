# src/test_polygon.py
# Тест Polygon + сохранение в Parquet (локально в /app/data/)

import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime

# Получаем API-ключ из переменных окружения
API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    print("Ошибка: POLYGON_API_KEY не найден!")
    exit(1)

print("API-ключ найден, начинаем работу...")

client = RESTClient(api_key=API_KEY)

# Параметры
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

        # Преобразуем в DataFrame
        data = []
        for bar in aggs:
            timestamp_ms = bar.timestamp
            timestamp_sec = timestamp_ms / 1000
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

        # Показываем первые 5 строк
        print(df.head(5))

        # Сохраняем в Parquet (локально в контейнере)
        output_dir = "/app/data"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/{ticker}_daily.parquet"
        df.to_parquet(output_path)
        print(f"Данные сохранены в: {output_path}")
        print(f"Размер файла: {os.path.getsize(output_path) / 1024:.2f} KB")

    else:
        print("Нет данных за период.")

except Exception as e:
    print(f"Ошибка Polygon: {e}")

print("Скрипт завершён.")
