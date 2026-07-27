import io
import sys
from pathlib import Path

from datetime import datetime
from zoneinfo import ZoneInfo

import logging

import duckdb

import pandas as pd
import pyarrow.parquet as pq

import numpy as np

from ...notify.slack_notify import slack_pipe_notify

from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...config.period_indicator_config import indicator_and_period_configs

from ...utils.helpers import save_df_as_parquet_to_minio
from ...utils.helpers import countdown

from .multi_period_analyzer import MultiPeriodAnalyzer

logger = logging.getLogger(__name__)


def get_today_parquet_buffers(
        bucket: str,
        prefix: str = "",
    ):

    """
    從 MinIO 列出指定 bucket/prefix 下，今日修改的 .parquet 檔案，
    讀入記憶體後回傳 BytesIO buffer 列表，供 DuckDB 直接查詢使用。

    Args:
        bucket: MinIO bucket 名稱，例如 "stock-bucket"
        prefix: 資料夾路徑前綴，例如 "raw/"；預設為根目錄

    Returns:
        list[dict]，每筆包含：
            - ticker        (str)     : 股票代碼（從檔名取得，例如 "KO"）
            - object_name   (str)     : MinIO 完整物件路徑
            - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
            - size          (int)     : 檔案大小（bytes）
            - buffer        (BytesIO) : Parquet 內容，指標在位置 0，可直接傳入 DuckDB
    """

    tz_taipei = ZoneInfo("Asia/Taipei")
    today = datetime.now(tz_taipei).date()  # 台灣時間今天日期，用於過濾當日檔案

    # 列出 bucket/prefix 下所有物件，沒有檔案時 Contents 不存在，預設空 list
    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    objects = response.get('Contents', [])

    results = []
    for obj in objects:

        # 過濾非今日修改的檔案（LastModified 預設 UTC，需轉台灣時區再比對）
        if obj['LastModified'].astimezone(tz_taipei).date() != today:
            continue

        # 只處理 .parquet 檔，跳過其他格式
        if not obj['Key'].endswith(".parquet"):
            continue

        try:
            # 從 MinIO 下載檔案內容到記憶體（不落地）
            res = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
            buffer = io.BytesIO(res['Body'].read())
            buffer.seek(0)  # 重置指標到起始位置，確保後續讀取從頭開始

            results.append({
                "ticker":        Path(obj['Key']).stem,              # 去除路徑與副檔名，取純檔名作為股票代碼
                "object_name":   obj['Key'],                         # MinIO 完整路徑
                "last_modified": obj['LastModified'].astimezone(tz_taipei),  # 轉台灣時區
                "size":          obj['Size'],                        # 檔案大小（bytes）
                "buffer":        buffer,                             # 記憶體中的 Parquet 內容
            })
            print(f"✓ {obj['Key']}")

        except Exception as e:
            # 單一檔案失敗不中斷整體流程，印出錯誤繼續處理下一個
            print(f"✗ {obj['Key']}: {e}")

    print(f"\n共 {len(results)} 個檔案")
    return results

def text_summary(save_results: list[dict]) -> str:
    success = [r for r in save_results if r["status"] == "success"]
    failed  = [r for r in save_results if r["status"] == "failed"]

    lines = ["📈 3. Gold 指標計算結果摘要"]

    lines.append(f"\n✅ 成功 ({len(success)})")
    for r in success:
        lines.append(f"  • {r['parquet_name']}")

    lines.append(f"\n❌ 失敗 ({len(failed)})")
    for r in failed:
        lines.append(f"  • {r['parquet_name']}")

    return "\n".join(lines)

def main():

    # 1. 取得今日檔案 buffer 列表
    parquet_files = get_today_parquet_buffers(
        bucket= MINIO_BUCKET,
        prefix= "stock/history/prices/silver/"
    )

    save_results = []

    # 2. 針對今日檔案 buffer 列表 對應 n個週期
    with get_duckdb_conn() as conn:

        for period, config in indicator_and_period_configs.items():
            
            tickers_with_single_period = []

            for parequet_file in parquet_files:

                parequet_file["buffer"].seek(0)
                df = pd.read_parquet(parequet_file["buffer"])

                # 設置ticker欄位
                df["ticker"] = parequet_file["ticker"]
                # 設置日期為索引
                df.set_index("Date", inplace=True, drop=False)

                print(f"\n處理：{parequet_file['object_name']}")
                
                # calculate df
                analyzer = MultiPeriodAnalyzer()
                single_ticker_single_period = analyzer.run_single_period_with_ticker(
                    df,
                    period,
                    config
                )
                tickers_with_single_period.append(single_ticker_single_period)

            tickers_with_single_period = pd.concat(tickers_with_single_period, ignore_index=True)

            # scale and cast df
            exclude_cols = {"EMA_bullbear", "trend", "Side", "Peak", "Trough"}
            num_cols = [c for c in tickers_with_single_period.select_dtypes(include="number").columns if c not in exclude_cols]

            # apply() 可以「批量操作整欄or整列」
            tickers_with_single_period[num_cols] = (
                tickers_with_single_period[num_cols]
                .apply(pd.to_numeric, errors="coerce")     # pd.to_numeric() 只能處理一維(Series),每欄轉成數值,不能轉的變 NaN
                .apply(lambda x: np.trunc(x * 100) / 100)  # 截斷到小數點2位
            )

            tickers_with_single_period = tickers_with_single_period.astype(object).where(pd.notnull(tickers_with_single_period), None)
            
            try:
                save_df_as_parquet_to_minio(
                    conn = conn,
                    df = tickers_with_single_period,
                    bucket = MINIO_BUCKET,
                    object_name = f"stock/history/prices/gold/us_{period}.parquet",
                )
                save_results.append({
                    "parquet_name": f"us_{period}",
                    "status": "success",
                })

            except Exception as e:
                save_results.append({
                    "parquet_name": f"us_{period}",
                    "status": "failed",
                })

    # results 文字摘要（供 slack send）
    summary = text_summary(save_results)
    slack_pipe_notify(summary)


if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()