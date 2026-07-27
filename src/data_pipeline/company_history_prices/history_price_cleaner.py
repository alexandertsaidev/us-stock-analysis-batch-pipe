import io
import sys
from pathlib import Path

from datetime import datetime
from zoneinfo import ZoneInfo

import logging

import pyarrow.parquet as pq

from botocore.exceptions import ClientError

from ...notify.slack_notify import slack_pipe_notify
from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...utils.helpers import countdown

import duckdb

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

def clean_and_upload(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        files: list[dict],
    ):

    results = []

    for file in files:
        object_name = file["object_name"]

        try:
            # ────────────────────────────────────────────────────────────────
            # Step 1：DuckDB 清洗
            # ────────────────────────────────────────────────────────────────

            # BytesIO → PyArrow Table → 註冊進 DuckDB 作為虛擬資料表
            file["buffer"].seek(0)
            arrow_table = pq.read_table(file["buffer"])
            conn.register("temp_parquet", arrow_table)

            # 執行清洗
            out_buffer = io.BytesIO()

            cleaned = conn.execute("""
                SELECT
                    "Date"::DATE AS "Date",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Adj Close",
                    "Volume",
                    "Dividends",
                    "Stock Splits",
                    "created_at"
                FROM temp_parquet
                WHERE
                    "Open"        IS NOT NULL
                    AND "High"    IS NOT NULL
                    AND "Low"     IS NOT NULL
                    AND "Close"   IS NOT NULL
                    AND "Adj Close" IS NOT NULL
                    AND "Volume"  IS NOT NULL
            """).to_arrow_table()
            
            # 統計清洗前後筆數，用於 log 與回傳結果
            raw_count     = conn.execute("SELECT COUNT(*) FROM temp_parquet").fetchone()[0]
            cleaned_count = cleaned.num_rows
            dropped_count = raw_count - cleaned_count

            # cleaned PyArrow Table → Parquet bytes（保留 date32 型別）
            # pq.write_table() 回傳 None，不可用變數接住
            pq.write_table(cleaned, out_buffer)
            out_buffer.seek(0)

            # ────────────────────────────────────────────────────────────────
            # Step 2：決定輸出路徑
            # ────────────────────────────────────────────────────────────────

            out_path = object_name.replace("bronze/", "silver/", 1)

            # ────────────────────────────────────────────────────────────────
            # Step 3：上傳回 MinIO（boto3 語法）
            # ────────────────────────────────────────────────────────────────
            try:
                s3_client.head_bucket(Bucket = bucket)
            except ClientError:
                s3_client.create_bucket(Bucket = bucket)

            s3_client.put_object(
                Bucket = bucket,
                Key = out_path,
                Body = out_buffer,
            )

            result = {
                "object_name":   object_name,
                "out_path":      out_path,
                "raw_count":     raw_count,
                "cleaned_count": cleaned_count,
                "dropped_count": dropped_count,
                "status":        "success",
            }
            print(f"✓ {object_name} → {out_path}  "
                  f"({raw_count} 筆 → {cleaned_count} 筆，丟棄 {dropped_count} 筆)")

        except Exception as e:
            result = {
                "object_name": object_name,
                "status":      "failed",
                "error":       str(e),
            }
            print(f"✗ {object_name}: {e}")

        results.append(result)

    success = [r for r in results if r["status"] == "success"]
    failed  = [r for r in results if r["status"] == "failed"]
    print(f"\n完成：{len(success)} 成功 / {len(failed)} 失敗")

    return results

def text_summary(results: list[dict]) -> str:
    success = [r for r in results if r["status"] == "success"]
    failed  = [r for r in results if r["status"] == "failed"]

    lines = ["🧹 2. Silver 清洗結果摘要"]

    lines.append(f"\n✅ 成功 ({len(success)})")
    for r in success:
        ticker = Path(r["object_name"]).stem
        lines.append(
            f"  • {ticker}：{r['raw_count']} 筆 → {r['cleaned_count']} 筆"
            f"（丟棄 {r['dropped_count']} 筆）"
        )

    lines.append(f"\n❌ 失敗 ({len(failed)})")
    for r in failed:
        ticker = Path(r["object_name"]).stem
        lines.append(f"  • {ticker}：{r.get('error', 'unknown')}")

    return "\n".join(lines)

def main():

    with duckdb.connect() as conn:

        # 1. 取得今日檔案 buffer 列表
        files = get_today_parquet_buffers(
            bucket= MINIO_BUCKET, 
            prefix= "stock/history/prices/bronze/"
        )

        # 2. 逐一清洗並存回 MinIO
        results = clean_and_upload(conn, MINIO_BUCKET, files)

    # 3. results 文字摘要（供 slack send）
    summary = text_summary(results)
    slack_pipe_notify(summary)

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()