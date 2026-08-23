import sys
import time
from datetime import datetime, timezone

import logging

import duckdb

import pandas as pd
import numpy as np

from ...notify.slack_notify import slack_batch_pipe_notify

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...config.period_indicator_config import indicator_and_period_configs

from ...utils.helpers import get_pa_table
from ...utils.helpers import get_co_fetch_list
from ...utils.helpers import save_df_as_parquet_to_minio

from ...utils.helpers import countdown

from .analyzer import Analyzer

logger = logging.getLogger(__name__)

def calculate_clean_upload(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        tickers: list,
    ):
    temp_dict = {"success": [], "failed": []}

    for period, params in indicator_and_period_configs.items():

        start = time.perf_counter()
        tickers_with_one_period = []
        
        for ticker in tickers:

            silver_object_name = f"stock/history/prices/silver/{ticker}.parquet"

            try:
                silver_pa = get_pa_table(conn, bucket, silver_object_name)
                created_at_datetime = silver_pa["created_at"][0].as_py()

                if created_at_datetime.date() != datetime.now(timezone.utc).date():
                    logger.warning(f"{ticker}, silver 非今日，跳過")

                    continue

                conn.register("silver_data", silver_pa)
                df = conn.execute("SELECT * FROM silver_data").df()

                # calculate df
                one_ticker_one_period = Analyzer(
                    df,
                    period,
                    params
                )
                tickers_with_one_period.append(one_ticker_one_period.df)

                logger.info(f"處理 {bucket}/{silver_object_name} 成功")

            except Exception as e:

                logger.error(f"處理 {bucket}/{silver_object_name} 發生錯誤: {e}", exc_info=True)
                continue

        if not tickers_with_one_period:
            return
        
        tickers_with_one_period = pd.concat(tickers_with_one_period, ignore_index=True)

        # scale and cast df
        num_cols = [c for c in tickers_with_one_period.select_dtypes(include="number").columns]

        # apply() 批量操作整欄 or 整列
        tickers_with_one_period[num_cols] = (
            tickers_with_one_period[num_cols]
            .apply(pd.to_numeric, errors="coerce")     # pd.to_numeric() 只能處理一維(Series),每欄轉成數值,不能轉的變 NaN
            .apply(lambda x: np.trunc(x * 100) / 100)  # 截斷到小數點2位
        )

        tickers_with_one_period = tickers_with_one_period.astype(object).where(pd.notnull(tickers_with_one_period), None)
        
        try:
            save_df_as_parquet_to_minio(
                conn = conn,
                df = tickers_with_one_period,
                bucket = MINIO_BUCKET,
                object_name = f"stock/history/prices/gold/us_{period}.parquet",
            )
            elapse = time.perf_counter() - start
            temp_dict["success"].append((f"us_{period}.parquet", elapse))
            logger.info(f"儲存 us_{period}.parquet 成功")

        except Exception as e:
            temp_dict["failed"].append((f"us_{period}.parquet", 0))
            logger.error(f"儲存 us_{period}.parquet 發生錯誤: {e}", exc_info=True)
            continue

    return temp_dict

def text_summary(final_dict: dict) -> str:
    success = final_dict["success"]
    failed  = final_dict["failed"]

    lines = [
        "📈 3. Gold 指標 & 分析計算結果摘要",
        f"✅ 成功 ({len(success)})｜❌ 失敗 ({len(failed)})",
        "",
    ]

    if success:
        lines.append(f"✅ 成功 ({len(success)})")
        for period, elapse in success:
            lines.append(f"  • {period}（{elapse:.2f}s）")
        lines.append("")

    if failed:
        lines.append(f"❌ 失敗 ({len(failed)})")
        for period, _ in failed:
            lines.append(f"  • {period}")

    return "\n".join(lines)


def main():

    with get_duckdb_conn() as conn:

        tickers = get_co_fetch_list(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/screening/us_all_co_screen.parquet"
        )
        final_dict = calculate_clean_upload(
            conn = conn,
            bucket = MINIO_BUCKET,
            tickers = tickers,
        )

    if not final_dict :
        logger.warning("沒有任何計算結果，跳過通知")
        return
    
    for status, items in final_dict.items():
        logger.info(f"{status}: {len(items)}")

    slack_batch_pipe_notify( text_summary(final_dict) )

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()
