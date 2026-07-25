from kafka import KafkaProducer

import sys

import random
import logging

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from .utils.helpers import get_pa_table
from .config.minio_duckdb_conn import get_duckdb_conn

def main():
    conn = get_duckdb_conn()
    a = get_pa_table(conn, "us-stock", f"stock/fundamentals/us_all_co_fundamentals.parquet")
    print(a.to_pandas())
    return a

if __name__ == "__main__":
    main()

sys.exit()
# try:
#     producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
#     print("✅ 連線成功")
#     producer.close()
# except Exception as e:
#     print(f"❌ 連線失敗: {e}")

# import pandas as pd
# import yfinance as yf

# ticker = "AAPL"
# stock = yf.Ticker(ticker)

# # period='max'

# df = stock.history(period="2mo", auto_adjust=False)
# df_2 = stock.history(period="2mo")

# # 1.將 index 變成普通欄位，並自動生成新的整數 index。
# df = df.reset_index()
# df_2 = df_2.reset_index()

# # 2. 將日期轉換為 datetime 類型
# df["Date"] = pd.to_datetime(df["Date"]).dt.date
# df_2["Date"] = pd.to_datetime(df_2["Date"]).dt.date

# print(df)
# print(df_2)

