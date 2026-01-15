# test_polygon.py
# Простой тест, что Polygon работает

from polygon import RESTClient
from datetime import datetime

# Вставь сюда свой настоящий API-ключ от Massive.com
API_KEY = "WPF6ovyBgGtnpmNl6ItEEJCqofDf2XuT"

# Создаём клиента Polygon
client = RESTClient(api_key=API_KEY)

# Скачиваем дневные данные по Apple за 2025 год
ticker = "AAPL"
start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

print(f"Скачиваем данные для {ticker} с {start_date} по {end_date}...")

aggs = client.get_aggs(
    ticker=ticker,
    multiplier=1,          # 1 = дневные бары
    timespan="day",
    from_=start_date,
    to=end_date,
    limit=50000            # максимум, чтобы взять все дни
)

# Если данные пришли — покажем первые 5 строк
if aggs:
    print(f"Получено {len(aggs)} баров")
    for bar in aggs[:5]:
        date = bar.timestamp.strftime("%Y-%m-%d")
        print(f"{date} | Close: {bar.close:.2f} | Volume: {bar.volume}")
else:
    print("Данные не получены. Проверь API-ключ или интернет.")
