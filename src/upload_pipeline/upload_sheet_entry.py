import pandas as pd
import time

import os
import sys
import logging

from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from ..config.minio_conn import MINIO_BUCKET
from ..config.minio_duckdb_conn import get_duckdb_conn

from ..notify.slack_notify import slack_batch_pipe_notify

from ..utils.helpers import get_pa_table
from ..utils.helpers import countdown

from dotenv import load_dotenv, find_dotenv

logger = logging.getLogger(__name__)

load_dotenv(find_dotenv())

def rewrite_gsheet(df, sheet_name, worksheet_name):

    # 1.權限
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # 2.憑證
    creds = Credentials.from_service_account_file(
        Path("/opt/airflow/scripts/us-stock-analysis-batch-pipe/src/upload_pipeline/credentials.json"),
        scopes=scope
    )

    # 3.連線
    client = gspread.authorize(creds)
    worksheet = client.open(sheet_name).worksheet(worksheet_name)

    # 4.處理 df 
    df = df.copy()

    keywords = ["Date_", "created_at"]  # 自訂關鍵字
    date_cols = [col for col in df.columns if any(kw in col for kw in keywords)]
    
    # 處理日期相關欄位
    if date_cols:
        df[date_cols] = (
            df[date_cols]
            .apply(lambda x: pd.to_datetime(x, errors="coerce").dt.strftime("%Y-%m-%d"))
        )

    # 5.清空
    worksheet.clear()
    
    # 6.一次寫入
    worksheet.update(
        [df.columns.tolist()] + df.astype(str).values.tolist()
    )

    return

def main():

    try:
        with get_duckdb_conn() as conn:
            # 1.
            arrow_table = get_pa_table(
                conn = conn,
                bucket = MINIO_BUCKET,
                object_name = f"stock/history/prices/gold/conclusion/entry_conclusion.parquet"
            )

            conn.register("entry_conclusion", arrow_table)

            df = conn.execute(f"""
                SELECT * FROM "entry_conclusion"
                ORDER BY "Date_D" DESC, "ticker" ASC
            """).df().fillna("None")

        time.sleep(1)
        rewrite_gsheet(df, "entry_conclusion", "US")

        slack_batch_pipe_notify(f"🔗 5. 已上傳 entry_conclusion 至 Google Sheet\n  • {os.environ['GSHEET_ENTRY_URL']}")

    except Exception as e:
        logger.error(f"上傳失敗 : {e}", exc_info=True)

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()