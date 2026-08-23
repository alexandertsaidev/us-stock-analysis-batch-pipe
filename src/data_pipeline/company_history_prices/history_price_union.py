import sys

import pandas as pd

import logging

import time
from datetime import datetime, timezone

import duckdb

from ...notify.slack_notify import slack_batch_pipe_notify

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn
from ...config.period_indicator_config import indicator_and_period_configs

from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import save_df_as_parquet_to_minio
from ...utils.helpers import get_pa_table

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

final_parquet = "us_all_prices.parquet"
temp_parquet  = "temp.parquet"
gold_prefix   = "stock/history/prices/gold/final_all"


def upsert_parquet(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        tickers_with_periods: list[pd.DataFrame],
    ) -> bool:

    if not tickers_with_periods:
        logger.warning("tickers_with_periods 為空，略過 upsert")
        return False

    # 合併今日新資料
    new_df = pd.concat(tickers_with_periods, ignore_index=True)

    if not all(col in new_df.columns for col in ["Date", "ticker", "period"]):
        logger.error("new_df 缺少 Date, ticker,period (之一)欄位，中止")
        return False
    
    # 無條件覆寫 temp.parquet

    try:
        logger.info(f"寫入 {gold_prefix}/{temp_parquet} ...")
        save_df_as_parquet_to_minio(
            conn,
            new_df,
            bucket, 
            f"{gold_prefix}/{temp_parquet}"
        )
    except Exception as e:
        logger.info(f"寫入 {gold_prefix}/{temp_parquet} 失敗")
        return False

    # 若 us_all_prices.parquet 不存在則直接建立
    try:
        old_table = get_pa_table(
            conn,
            bucket,
            f"{gold_prefix}/{final_parquet}"
        )
        
    except FileNotFoundError as e:
        logger.info(f"{gold_prefix}/{final_parquet} 不存在，直接以 new_df 建立")

        try:
            save_df_as_parquet_to_minio(
                conn,
                new_df,
                bucket,
                f"{gold_prefix}/{final_parquet}"
            )
            return True
        
        except Exception as e:
            logger.error(f"建立 {gold_prefix}/{final_parquet} 失敗")
            return False
    
    except Exception as e:
        return False

    # DuckDB 讀取 temp.parquet，取得 tickers_literal
    try:
        temp_table = get_pa_table(
            conn,
            bucket, 
            f"{gold_prefix}/{temp_parquet}"
        )

        conn.register("temp_parquet", temp_table)

        tickers_literal = conn.execute("""
            SELECT string_agg('''' || ticker || '''', ', ')
            FROM (SELECT DISTINCT ticker FROM temp_parquet)
        """).fetchone()[0]

        logger.info(f"讀取 {gold_prefix}/{temp_parquet} 中的 tickers：{tickers_literal}")

    except Exception as e:
        logger.error(f"讀取 {gold_prefix}/{temp_parquet} 失敗: {e}", exc_info=True)
        return False

    # DuckDB 去重並合併
    try:
        conn.register("old_table", old_table)

        merged_arrow = conn.execute(f"""
            WITH old AS (
                SELECT
                    "Date"::DATE AS "Date",
                    * EXCLUDE ("Date")
                FROM old_table
                WHERE "ticker" IN ({tickers_literal})
                QUALIFY
                    ROW_NUMBER() OVER (
                        PARTITION BY "ticker", "period"
                        ORDER BY "Date" DESC
                    ) > 2

                UNION ALL

                SELECT
                    "Date"::DATE AS "Date",
                    * EXCLUDE ("Date")
                FROM old_table
                WHERE ticker NOT IN ({tickers_literal})
            )
            SELECT * FROM old

            UNION ALL

            SELECT t.*
            FROM temp_parquet t
            ANTI JOIN old o  -- ANTI JOIN 只要「不存在於 old 的資料」
                ON  t."Date"   = o."Date"
                AND t."ticker" = o."ticker"
                AND t."period" = o."period"

        """).to_arrow_table()

    except Exception as e:
        logger.error(f"DuckDB merge 失敗 - {e}", exc_info=True)
        return False

    try:
        logger.info(f"合併後共 {merged_arrow.num_rows} 筆，\n準備儲存 {gold_prefix}/{final_parquet}")

        save_parquet_to_minio(
            merged_arrow,
            bucket,
            f"{gold_prefix}/{final_parquet}" 
        )
        return True

    except Exception as e:
        logger.error(f"儲存 {gold_prefix}/{final_parquet} 失敗: {e}", exc_info=True)
        return False

def text_summary(final_dict: dict) -> str:
    success = final_dict["success"]
    failed  = final_dict["failed"]

    lines = [
        "📦 4. 股價 union 儲存結果摘要",
        f"✅ 成功 ({len(success)})｜❌ 失敗 ({len(failed)})",
        "",
    ]

    if success:
        lines.append(f"✅ 成功 ({len(success)})")
        for union, elapse in success:
            lines.append(f"  • {union}（{elapse:.2f}s）")
        lines.append("")

    if failed:
        lines.append(f"❌ 失敗 ({len(failed)})")
        for union, _ in failed:
            lines.append(f"  • {union}")

    return "\n".join(lines)

def main():

    temp_dict = {"success": [], "failed": []}
    start = time.perf_counter()

    periods = [ p for p in indicator_and_period_configs.keys() ]
    tickers_with_periods = []

    with get_duckdb_conn() as conn:

        for period in periods:

            gold_object_name = f"stock/history/prices/gold/us_{period}.parquet"

            try:
                gold_pa = get_pa_table(conn, MINIO_BUCKET, gold_object_name)
                created_at_datetime = gold_pa["created_at"][0].as_py()

                if created_at_datetime.date() != datetime.now(timezone.utc).date():
                    logger.warning(f"{MINIO_BUCKET}/{gold_object_name}, 非今日，跳過")

                    continue

                conn.register("gold_data", gold_pa)
                df = conn.execute("SELECT * FROM gold_data").df()

                df["Date"] = pd.to_datetime(df["Date"]).dt.date
                tickers_with_periods.append(df)

                logger.info(f"處理 {MINIO_BUCKET}/{gold_object_name} 成功")

            except Exception as e:

                logger.error(f"處理 {MINIO_BUCKET}/{gold_object_name} 發生錯誤: {e}", exc_info=True)
                continue

        # upsert 合併寫入
        if tickers_with_periods:

            result = upsert_parquet(
                conn=conn,
                bucket=MINIO_BUCKET,
                tickers_with_periods=tickers_with_periods,
            )

    if result:
        elapse = time.perf_counter() - start
        temp_dict["success"].append(("us_all_prices.parquet", elapse))

    else:
        temp_dict["failed"].append(("us_all_prices.parquet", 0))
    
    for status, items in temp_dict.items():
        logger.info(f"{status}: {len(items)}")

    slack_batch_pipe_notify( text_summary(temp_dict) )
 
    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()
