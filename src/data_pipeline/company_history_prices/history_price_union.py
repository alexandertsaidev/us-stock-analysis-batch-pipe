import io
import sys

import pandas as pd

import logging

from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ...notify.slack_notify import slack_pipe_notify

from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import save_df_as_parquet_to_minio
from ...utils.helpers import get_pa_table

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

final_parquet = "us_all_prices.parquet"
temp_parquet  = "temp.parquet"
gold_prefix   = "stock/history/prices/gold/final_all"


# ── Step 1：從 MinIO 讀取今日 Parquet ────────────────────────────────────────
def get_today_parquet_buffers(
        bucket: str,
        prefix: str = "",
    ) -> list[dict]:

    """
    從 MinIO 列出指定 bucket/prefix 下，今日修改的 .parquet 檔案，
    讀入記憶體後回傳 BytesIO buffer 列表，供後續 pandas 查詢使用。

    Returns:
        list[dict]，每筆包含：
            - object_name   (str)     : MinIO 完整物件路徑
            - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
            - size          (int)     : 檔案大小（bytes）
            - buffer        (BytesIO) : Parquet 內容，指標在位置 0
    """
    tz_taipei = ZoneInfo("Asia/Taipei")
    today     = datetime.now(tz_taipei).date()

    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    objects  = response.get("Contents", [])

    results = []
    for obj in objects:
        if obj["LastModified"].astimezone(tz_taipei).date() != today:
            continue
        if not obj["Key"].endswith(".parquet"):
            continue
        if "final_all" in obj["Key"]:          # ← 排除 final_all 目錄
            continue
        if "conclusion" in obj["Key"]:          # ← 排除 conclusion 目錄
            continue

        try:
            res    = s3_client.get_object(Bucket=bucket, Key=obj["Key"])
            buffer = io.BytesIO(res["Body"].read())
            buffer.seek(0)
            results.append({
                "object_name":   obj["Key"],
                "last_modified": obj["LastModified"].astimezone(tz_taipei),
                "size":          obj["Size"],
                "buffer":        buffer,
            })
            logger.info(f"✓ {obj['Key']}")

        except Exception as e:
            logger.warning(f"✗ {obj['Key']}: {e}")

    logger.info(f"共 {len(results)} 個檔案")
    return results


def upsert_parquet(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    tickers_with_periods: list[pd.DataFrame],
    ) -> bool:
    
    """
    流程：
      1. 合併 tickers_with_periods → new_df
      2. 將 new_df 存為 temp.parquet（直接覆寫）
      3. 若 us_all_prices.parquet 不存在 → 以 new_df 直接建立後結束
      4. 若已存在：
           a. DuckDB 讀取 temp.parquet 取得 tickers_literal
           b. DuckDB 刪除 old_df 中對應 tickers 最新 2 筆（Date DESC）
              + merge temp_parquet，去重後一次輸出 PyArrow Table
           c. 覆寫 us_all_prices.parquet
    """

    if not tickers_with_periods:
        logger.warning("tickers_with_periods 為空，略過 upsert")
        return False

    # ── Step 1：合併今日新資料 ────────────────────────────────────────────────
    new_df = pd.concat(tickers_with_periods, ignore_index=True)

    if not all(col in new_df.columns for col in ["Date", "ticker", "period"]):
        logger.error("new_df 缺少 Date, ticker,period (之一)欄位，中止")
        return False
    
    # ── Step 2：寫入 temp.parquet（覆寫）────────────────────────────────────

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

    # ── Step 3：us_all_prices.parquet 不存在 → 直接建立 ──────────────────────
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

    # ── Step 4a：DuckDB 讀取 temp.parquet，取得 tickers_literal ─────────────
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

    # ── Step 4b + 4c：DuckDB 一次完成刪舊 + merge + 去重 ────────────────────
    try:
        conn.register("old_table", old_table)

        merged_arrow = conn.execute(f"""

            /*
            * 最外層 SELECT：輸出最終結果
            *   - "Date"::DATE      確保型別為 date32
            *   - EXCLUDE _rn, _p   移除輔助欄，不帶入結果
            */
            SELECT
                "Date"::DATE AS "Date",
                * EXCLUDE ("Date", _rn, _p)
            FROM (

                /*
                * 區塊 1：舊資料
                *   - ticker NOT IN 更新名單 → 全數保留
                *   - ticker IN  更新名單 → 只保留非最新 2 筆 (_rn > 2)
                *     （最新 2 筆讓新資料覆蓋）
                *   - _p = 1，衝突時優先保留
                */
                SELECT *, 1 AS _p FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY "ticker"
                            ORDER BY "Date" DESC   -- _rn=1 為最新一筆
                        ) AS _rn
                    FROM old_table
                )
                WHERE "ticker" NOT IN ({tickers_literal}) OR _rn > 2

                UNION ALL

                /*
                * 區塊 2：新資料 (temp_parquet)
                *   - 補入舊資料不存在的 (Date, ticker, period)
                *   - _p = 2，衝突時輸給舊資料（DO NOTHING 語義）
                *   - _rn = 0 為佔位，使兩分支欄位數一致
                */
                SELECT *, 0 AS _rn, 2 AS _p FROM temp_parquet
            )

            /*
            * QUALIFY：每個 (Date, ticker, period) 組合只保留一筆
            *   - 按 _p ASC 排序 → _p=1 舊資料排第一
            *   - = 1 → 只取排名第一（舊資料優先，等同 ON CONFLICT DO NOTHING）
            */
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "Date", "ticker", "period"
                ORDER BY _p
            ) = 1

            ORDER BY "Date" DESC, "ticker" ASC

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


def text_summary(success) -> str:
    lines = ["📊 ==4.股價 union 儲存結果摘要=="]

    if success:
        lines.append(f"\n✅ 成功")
        lines.append(f"  • 已建立 {gold_prefix}/{final_parquet}")
    else:
        lines.append(f"\n❌ 失敗")
        lines.append(f"  • 未建立 {gold_prefix}/{final_parquet}")

    return "\n".join(lines)


def main():

    # 1. 取得今日檔案 buffer 列表
    parquet_files = get_today_parquet_buffers(
        bucket= MINIO_BUCKET,
        prefix= "stock/history/prices/gold/"
    )

    tickers_with_periods = []

    for parquet_file in parquet_files:

        parquet_file["buffer"].seek(0)
        df = pd.read_parquet(parquet_file["buffer"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        print(f"\n處理：{parquet_file['object_name']}")

        tickers_with_periods.append(df)

    # ── upsert 合併寫入 ───────────────────────────────────────────────────────
    if tickers_with_periods:

        with get_duckdb_conn() as conn:

            success = upsert_parquet(
                conn=conn,
                bucket=MINIO_BUCKET,
                tickers_with_periods=tickers_with_periods,
            )

        summary = text_summary(success)
        slack_pipe_notify(summary)
        
    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()