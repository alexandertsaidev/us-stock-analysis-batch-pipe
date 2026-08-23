import sys

import time
from datetime import datetime, timezone

import logging

from ...notify.slack_notify import slack_batch_pipe_notify
from ...config.minio_duckdb_conn import get_duckdb_conn
from ...config.minio_conn import MINIO_BUCKET

from ...utils.helpers import get_pa_table
from ...utils.helpers import get_co_fetch_list
from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import countdown

import duckdb

logger = logging.getLogger(__name__)

def clean_and_upload(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        tickers: list,
    ):
    temp_dict = {"success": [], "failed": []}

    for ticker in tickers:
        start = time.perf_counter()

        bronze_object_name = f"stock/history/prices/bronze/{ticker}.parquet"
        silver_object_name = f"stock/history/prices/silver/{ticker}.parquet"

        try:
            bronze_pa = get_pa_table(conn, bucket, bronze_object_name)
            created_at_datetime = bronze_pa["created_at"][0].as_py()

            if created_at_datetime.date() != datetime.now(timezone.utc).date():
                logger.warning(f"{ticker}, bronze 非今日，跳過")
                temp_dict["failed"].append((ticker, 0, 0))
                continue

            conn.register("bronze_data", bronze_pa)
            bronze_rows = bronze_pa.num_rows

            silver_pa = conn.execute("""
                SELECT
                    "Date"::DATE AS "Date",
                    "ticker",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Adj Close",
                    "Volume",
                    "Dividends",
                    "Stock Splits",
                    "created_at"
                FROM bronze_data
                WHERE
                    "Open"        IS NOT NULL
                    AND "High"    IS NOT NULL
                    AND "Low"     IS NOT NULL
                    AND "Close"   IS NOT NULL
                    AND "Adj Close" IS NOT NULL
                    AND "Volume"  IS NOT NULL
            """).to_arrow_table()

            silver_rows = silver_pa.num_rows

            save_parquet_to_minio(silver_pa, bucket, silver_object_name)
            elapse = time.perf_counter() - start

            temp_dict["success"].append((ticker, elapse, bronze_rows-silver_rows))
            logger.info(f"{ticker}, {bronze_rows} 筆 → {silver_rows} 筆，丟棄 {bronze_rows-silver_rows} 筆)")

        except Exception as e:
            temp_dict["failed"].append((ticker, 0, 0))
            logger.error(f"讀取 {bucket}/{bronze_object_name} 發生錯誤: {e}", exc_info=True)
            continue

    return temp_dict

def text_summary(final_dict: dict) -> str:
    success = final_dict["success"]
    failed  = final_dict["failed"]

    lines = [
        "🧹 2. Silver 清洗結果摘要",
        f"✅ 成功 ({len(success)})｜❌ 失敗 ({len(failed)})",
        "",
    ]

    if success:
        lines.append(f"✅ 成功 ({len(success)})")
        for ticker, elapse, clean_rows in success:
            lines.append(f"  • {ticker}（{elapse:.2f}s）（丟棄 {clean_rows} 筆）")
        lines.append("")

    if failed:
        lines.append(f"❌ 失敗 ({len(failed)})")
        for ticker, _, _ in failed:
            lines.append(f"  • {ticker}")

    return "\n".join(lines)

def main():

    with get_duckdb_conn() as conn:

        tickers = get_co_fetch_list(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/screening/us_all_co_screen.parquet"
        )

        final_dict = clean_and_upload(
            conn = conn,
            bucket = MINIO_BUCKET,
            tickers = tickers,
        )

    for status, items in final_dict.items():
        logger.info(f"{status}: {len(items)}")

    slack_batch_pipe_notify( text_summary(final_dict) )

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()