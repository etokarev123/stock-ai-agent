# src/test_polygon.py
# Тест Polygon API + исправленный вывод дат

import os
from polygon import RESTClient
from datetime import datetime

# Получаем API-ключ из переменных окружения Railway
API_KEY = os.getenv("POLYGON_API_KEY")

if not API_KEY:
    print("Ошибка: POLYGON_API_KEY не найден в переменных окружения!")
    exit(1)

print("API-ключ найден, начинаем работу...")

# Создаём клиента Polygon
client = RESTClient(api_key=API_KEY)

# Параметры запроса
ticker = "AAPL"
start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

print(f"Скачиваем дневные данные для {ticker} с {start_date} по {end_date}...")

try:
    aggs = client.get_aggs(
        ticker=ticker,
        multiplier=1,          # 1 = дневные бары
        timespan="day",
        from_=start_date,
        to=end_date,
        limit=50000            # берём всё
    )

    if aggs:
        print(f"Получено {len(aggs)} баров")
        for bar in aggs[:5]:  # показываем первые 5 для примера
            # bar.timestamp приходит в миллисекундах → преобразуем в дату
            timestamp_seconds = bar.timestamp / 1000
            date_str = datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d")
            print(f"{date_str} | Close: {bar.close:.2f} | Volume: {bar.volume:,}")
    else:
        print("Данные не получены — возможно, неверные даты или нет данных за период.")

except Exception as e:
    print(f"Ошибка при запросе к Polygon: {e}")

print("Скрипт завершён.")
