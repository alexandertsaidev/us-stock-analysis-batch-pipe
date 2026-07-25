import io
import sys
import asyncio

import time
from datetime import datetime

import random

import logging

import pyarrow as pa
import pyarrow.parquet as pq

import pandas as pd
import yfinance as yf

import duckdb

from botocore.exceptions import ClientError

from yfinance.exceptions import YFRateLimitError, YFTickerMissingError, YFInvalidPeriodError

from ...notify.slack_notify import slack_pipe_notify

from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...utils.helpers import get_now
from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

#  存 Parquet（success）
def _save_one_ticket_parquet(
    ticker: str,
    arrow_table: pa.Table,  # pa = pyarrow
    bucket: str,
    object_name: str,
    ) -> bool:

    try:
        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer, compression="snappy")
        buffer.seek(0)

        try:
            s3_client.head_bucket(Bucket = bucket)
        except ClientError:
            s3_client.create_bucket(Bucket = bucket)

        s3_client.put_object(
            Bucket = bucket,
            Key = object_name,
            Body = buffer,
        )
        logger.info(f"{ticker}: 已存入 {bucket}/{object_name}")
        return True

    except Exception as e:
        logger.error(f"{ticker}: MinIO 上傳失敗 - {e}", exc_info=True)
        return False


def get_co_fetch_list(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    object_name: str
    ) -> list[str]:

    try:
        tickers = conn.execute(f"""
            SELECT DISTINCT "ticker"
            FROM read_parquet('s3://{bucket}/{object_name}')
            WHERE "created_at" >= CURRENT_DATE - INTERVAL '2 years'
        """).df()["ticker"].tolist()
        
        logger.info(f"從 {bucket}/{object_name} 取得 {len(tickers)} 檔")
        return tickers

    except duckdb.HTTPException as e:
        logger.error(f"DuckDB 讀取 S3 失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise FileNotFoundError(f"找不到檔案: s3://{bucket}/{object_name}") from e

    except ClientError as e:
        # 保留 boto3 ClientError，以防其他地方仍用 s3_client
        logger.error(f"MinIO 讀取失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"讀取 {bucket}/{object_name} 發生未知錯誤: {e}", exc_info=True)
        raise


# 同步，抓資料
def fetch_price(ticker: str, now: datetime):

    start = time.perf_counter()

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period='max', auto_adjust=False)

        # 1. 將 index 變成普通欄位，並自動生成新的整數 index。
        df = df.reset_index()
        # 2. 將日期轉換為 datetime 類型
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        # 3. 增加 "created_at" datetime 類型
        df["created_at"] = now

        elapse = time.perf_counter() - start
        return {
            "ticker": ticker,
            "df": df,
            "status": "success" if not df.empty else "failed",
            "error_type": None,
            "error": None,
            "elapse": elapse,
        }

    except (YFTickerMissingError, YFInvalidPeriodError) as e:
        elapse = time.perf_counter() - start
        logger.warning(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "df": None,
            "status": "failed",
            "error_type": None,
            "error": None,
            "elapse": elapse,
        }

    except YFRateLimitError as e:
        elapse = time.perf_counter() - start
        logger.warning(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "df": None,
            "status": "retry",
            "error_type": None,
            "error": None,
            "elapse": elapse,
        }

    except Exception as e:
        elapse = time.perf_counter() - start
        logger.error(f"{ticker} failed: {e}", exc_info=True)
        return {
            "ticker": ticker,
            "df": None,
            "status": "failed",
            "error_type": "exception",
            "error": str(e),
            "elapse": elapse,
        }

# 非同步，包裝 fetch_price(同步)
async def fetch_one(ticker: str, now):
    # 取得當前 event loop，避免阻塞 event loop
    loop = asyncio.get_running_loop()
    # run_in_executor 把同步函式丟進執行緒池
    result = await loop.run_in_executor(
        None,                        # None 表示使用預設的 ThreadPoolExecutor
        lambda: fetch_price(ticker, now)  # 將同步函式包成 lambda 傳入執行緒
    )
    return result

# 非同步，統籌所有 fetch_one
async def fetch_all(tickers: list, now):
    # 限制同時最多 n 個 ticker 並發執行，避免對 yfinance 發送過多請求
    sem = asyncio.Semaphore(20)

    async def fetch_with_sem(ticker):
        await asyncio.sleep(random.uniform(0, 1))
        # 進入 semaphore，超過 n 個時自動等待，執行完畢後釋放名額
        async with sem:
            # 等這個 fetch_one 抓完，期間 event loop 可以去跑其他協程
            crawl_result = await fetch_one(ticker, now)

        if crawl_result["status"] == "success":
            print(f"{crawl_result['ticker']}: 爬蟲抓取成功")  # ✅ 暫時替代
            # 取得當前 event loop，避免阻塞 event loop
            loop = asyncio.get_running_loop()
            # _save_parquet 是同步函式，包進執行緒池避免阻塞 event loop
            await loop.run_in_executor(
                None,
                lambda: _save_one_ticket_parquet(
                    crawl_result["ticker"],
                    pa.Table.from_pandas(crawl_result["df"], preserve_index=False),
                    MINIO_BUCKET,
                    f"stock/history/prices/bronze/{crawl_result['ticker']}.parquet",
                )
            )

        return crawl_result
    
    # 針對需要 第1次~第n次 的爬蟲
    async def fetch_with_retry(ticker, max_retry=3):
        result = None

        for attempt in range(1, max_retry + 1):

            # 每次 attempt 開始前先等待（第1次不等）
            if attempt > 1:
                wait = (attempt - 1) * 10   # 第2次等5秒，第3次等10秒
                print(f"{ticker}: retry {attempt}/{max_retry}，等待 {wait} 秒...")
                await asyncio.sleep(wait)

            result = await fetch_with_sem(ticker)

            if result["status"] != "retry":
                return result   # success 或 failed 直接回傳

        # n次都是 retry
        print(f"{ticker}: 超過最大重試數: 3次 !!")
        return result
    
    total_start = time.perf_counter()
    results = await asyncio.gather(
        # 將所有 ticker 展開成獨立的 coroutine，同時並發執行
        *[fetch_with_retry(t) for t in tickers],
        # 單一 ticker 失敗時不中斷其他任務，例外會以物件形式放進 results
        return_exceptions=True
    )
    total_elapse = time.perf_counter() - total_start
    print(f"本輪總共花費時間:{total_elapse}")

    success, retry, failed = [], [], []

    for result in results:
        if isinstance(result, Exception):
            failed.append(result)

        elif not isinstance(result, dict):
            failed.append(result)

        else:
            status = result.get("status")

            if status == "success":
                success.append(result)
            elif status == "retry":
                retry.append(result)
            else:
                failed.append(result)

    return success, retry, failed

# 文字摘要
def text_summary(success, retry, failed) -> str:
    lines = ["📊 ==1.股價抓取結果摘要=="]
    
    lines.append(f"\n✅ 成功 success ({len(success)})")
    for r in success:
        lines.append(f"  • {r['ticker']}（{r['elapse']:.2f}s）")

    lines.append(f"\n🔁 待重試 retry ({len(retry)})")
    for r in retry:
        lines.append(f"  • {r['ticker']}")

    lines.append(f"\n❌ 失敗 failed ({len(failed)})")
    for r in failed:
        if isinstance(r, dict):
            err = r.get("error") or "unknown"
            lines.append(f"  • {r['ticker']}：{err}")
        else:
            lines.append(f"  • {r}")

    return "\n".join(lines)

def main():

    with get_duckdb_conn() as conn:

        tickers = get_co_fetch_list(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/screening/us_all_co_screen.parquet"
        )

    now = get_now(tz = "America/New_York")

    # tickers = ["BRK-B", "V", "MA", "PG", "KO", "PEP", "PM", "MO", "TSLAp"]
    success, retry, failed = asyncio.run(fetch_all(tickers, now))
    
    print(f"success={len(success)}\nfailed={len(failed)}\nretry={len(retry)}")

    summary = text_summary(success, retry, failed)
    slack_pipe_notify(summary)

    return


if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()        