# src/analyze_daily_data.py
import pandas as pd
from pathlib import Path
import warnings
from tqdm import tqdm

warnings.filterwarnings("ignore", category=UserWarning)

ROOT = Path("raw/daily")
OUTPUT_DIR = Path("analysis")
OUTPUT_DIR.mkdir(exist_ok=True)

def main():
    parquet_files = list(ROOT.glob("*.parquet"))
    print(f"Найдено файлов: {len(parquet_files)}")

    if not parquet_files:
        print("Нет parquet-файлов в raw/daily/ — проверь путь или запуск скачивания")
        return

    stats = []
    problematic = []

    for f in tqdm(parquet_files, desc="Анализ файлов"):
        try:
            df = pd.read_parquet(f)
            ticker = f.stem.split("_")[0]

            n_rows = len(df)
            if n_rows == 0:
                problematic.append({"ticker": ticker, "issue": "пустой файл", "rows": 0})
                continue

            # предполагаем, что есть стандартные колонки Polygon
            has_ts = "timestamp" in df.columns or df.index.name == "timestamp"
            has_date = "date" in df.columns
            has_close = "close" in df.columns
            has_vol = "volume" in df.columns

            row = {
                "ticker": ticker,
                "rows": n_rows,
                "min_date": None,
                "max_date": None,
                "null_close": 0,
                "zero_vol": 0,
                "neg_close": 0,
                "has_timestamp": has_ts,
                "has_date": has_date,
                "has_close": has_close,
                "has_volume": has_vol,
            }

            if has_ts or has_date:
                date_col = "timestamp" if has_ts else "date"
                row["min_date"] = df[date_col].min()
                row["max_date"] = df[date_col].max()

            if has_close:
                row["null_close"] = df["close"].isna().sum()
                row["neg_close"] = (df["close"] <= 0).sum()

            if has_vol:
                row["zero_vol"] = (df["volume"] == 0).sum()

            stats.append(row)

            # флаги проблем
            issues = []
            if n_rows < 400:
                issues.append(f"мало строк ({n_rows})")
            if row["null_close"] > 5:
                issues.append(f"пропуски close ({row['null_close']})")
            if row["zero_vol"] > 30:
                issues.append(f"много нулевого volume ({row['zero_vol']})")
            if row["neg_close"] > 0:
                issues.append(f"отрицательные/нулевые close ({row['neg_close']})")

            if issues:
                problematic.append({
                    "ticker": ticker,
                    "rows": n_rows,
                    "issues": "; ".join(issues),
                    **{k: row[k] for k in ["min_date", "max_date", "null_close", "zero_vol", "neg_close"]}
                })

        except Exception as e:
            problematic.append({"ticker": ticker, "issue": f"ошибка чтения: {str(e)}"})

    if stats:
        df_stats = pd.DataFrame(stats)
        df_stats.to_parquet(OUTPUT_DIR / "daily_stats.parquet", index=False)
        print(f"Сохранена общая статистика → {OUTPUT_DIR}/daily_stats.parquet")

        print("\nТоп-15 самых коротких тикеров:")
        print(df_stats.sort_values("rows").head(15)[["ticker", "rows", "min_date", "max_date"]])

        print(f"\nСреднее кол-во баров: {df_stats['rows'].mean():.0f}")
        print(f"Медиана: {df_stats['rows'].median():.0f}")

    if problematic:
        df_prob = pd.DataFrame(problematic)
        df_prob.to_csv(OUTPUT_DIR / "problematic_tickers.csv", index=False)
        print(f"\nНайдено проблемных тикеров: {len(problematic)}")
        print("Сохранено → analysis/problematic_tickers.csv")
        print(df_prob.head(20))
    else:
        print("\nСерьёзных проблем почти нет — данные выглядят чистыми")

if __name__ == "__main__":
    main()
