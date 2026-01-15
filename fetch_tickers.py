import os
from polygon import RESTClient
import pandas as pd
import time

# Настройки
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
OUTPUT_FILE = 'tickers_top_5000.txt'  # Будет сохранён в репозитории

client = RESTClient(api_key=POLYGON_API_KEY)

def get_all_us_stocks(limit_per_call=1000, target_count=5500):
    tickers = []
    cursor = None
    params = {
        'market': 'stocks',
        'active': True,
        'locale': 'us',
        'limit': limit_per_call,
        'order': 'asc',
        'sort': 'ticker'
    }

    while len(tickers) < target_count:
        if cursor:
            params['cursor'] = cursor

        response = client.get_tickers(**params)

        if not response.results:
            break

        for t in response.results:
            # Берём только обыкновенные акции (обычно 'CS' = common stock)
            if t.type == 'CS' and t.ticker:
                tickers.append(t.ticker)

        print(f"Собрано {len(tickers)} тикеров...")

        if not hasattr(response, 'next_url') or not response.next_url:
            break

        # Извлекаем cursor из next_url
        cursor = response.next_url.split('cursor=')[-1].split('&')[0]
        time.sleep(0.6)  # Безопасная пауза (даже на платном плане лучше не спамить)

    return tickers[:target_count]

if __name__ == "__main__":
    print("Начинаем загрузку списка тикеров...")
    all_tickers = get_all_us_stocks(target_count=5500)

    # Сохраняем в файл
    with open(OUTPUT_FILE, 'w') as f:
        for ticker in all_tickers:
            f.write(ticker + '\n')

    print(f"Готово! Сохранено {len(all_tickers)} тикеров в {OUTPUT_FILE}")
