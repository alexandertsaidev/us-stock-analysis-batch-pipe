import sys
import asyncio

import time
from datetime import datetime, timezone

import random

import logging

import pandas as pd
import yfinance as yf

from yfinance.exceptions import YFRateLimitError, YFTickerMissingError, YFInvalidPeriodError

from ...notify.slack_notify import slack_batch_pipe_notify

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...utils.helpers import get_co_fetch_list
from ...utils.helpers import save_df_as_parquet_to_minio
from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

def fetch_save_price(
        ticker: str,
    ):

    start = time.perf_counter()

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period='max', auto_adjust=False)

        if not df.empty:

            # 1. 將 index 變成普通欄位，並自動生成新的整數 index。
            df = df.reset_index()
            # 2. 將日期轉換為 datetime 類型
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            # 3. 增加 "created_at" datetime 類型
            df["created_at"] = datetime.now(timezone.utc)
            # 4. 增加 "ticker" 欄位
            df["ticker"] = ticker

            elapse = time.perf_counter() - start

            with get_duckdb_conn() as conn:  
                save_df_as_parquet_to_minio(
                    conn = conn,
                    df = df,
                    bucket = MINIO_BUCKET,
                    object_name = f"stock/history/prices/bronze/{ticker}.parquet",
                )
                logger.info(f"{ticker}, 花費: {elapse:.2f}s, 已成功抓取並儲存 ")
            
            return ("success", ticker, elapse)
        
        else:
            logger.warning(f"{ticker}: 無歷史股價資料")
            return ("retry", ticker, 0)

    except (YFTickerMissingError, YFInvalidPeriodError) as e:
        logger.warning(f"{ticker}: {e}")
        return ("failed", ticker, 0)

    except YFRateLimitError as e:
        logger.warning(f"{ticker}: {e}")
        return ("retry", ticker, 0)

    except Exception as e:
        logger.error(f"{ticker} failed: {e}", exc_info=True)
        return ("failed", ticker, 0)


async def async_fetch_save_price(
        ticker: str,
    ) -> tuple:

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: fetch_save_price(ticker)
    )

async def async_fetch_save_all(
        tickers: list,
    ) -> dict:

    sem = asyncio.Semaphore(20)
    temp_dict = {"success": [], "retry": [], "failed": []}

    async def run(ticker):
        await asyncio.sleep(random.uniform(0, 1))

        async with sem:
            status, ticker, elapse = await async_fetch_save_price(ticker)
            temp_dict[status].append((ticker, elapse))

    await asyncio.gather(*[run(t) for t in tickers], return_exceptions=True)

    return temp_dict

def text_summary(final_dict: dict) -> str:
    success = final_dict["success"]
    retry   = final_dict["retry"]
    failed  = final_dict["failed"]

    lines = [
        "📊 1. Bronze 股價抓取結果摘要",
        f"✅ 成功 ({len(success)})｜🔁 待重試 ({len(retry)})｜❌ 失敗 ({len(failed)})",
        "",
    ]

    if success:
        lines.append(f"✅ 成功 ({len(success)})")
        for ticker, elapse in success:
            lines.append(f"  • {ticker}（{elapse:.2f}s）")
        lines.append("")

    if retry:
        lines.append(f"🔁 待重試 ({len(retry)})")
        for ticker, _ in retry:
            lines.append(f"  • {ticker}")
        lines.append("")

    if failed:
        lines.append(f"❌ 失敗 ({len(failed)})")
        for ticker, _ in failed:
            lines.append(f"  • {ticker}")

    return "\n".join(lines)


def main():
    start = time.perf_counter()

    with get_duckdb_conn() as conn:

        tickers = get_co_fetch_list(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/screening/us_all_co_screen.parquet"
        )

    # tickers = ["BRK-B", "V", "MA", "PG", "KO", "PEP", "PM", "MO", "TSLAP"]

    final_dict = asyncio.run(async_fetch_save_all(tickers))

    retries = 3
    attempt = 0
    while attempt < retries :

        retry_tickers = [t for t, _ in final_dict["retry"] ]
        if not retry_tickers:
            break

        logger.info(f"Try {attempt + 1}/{retries}，共 {len(retry_tickers)} 檔")
        attempt += 1
        time.sleep(attempt * 15)

        temp_dict = asyncio.run(async_fetch_save_all(retry_tickers))

        # success & failed 累加
        final_dict["success"].extend(temp_dict["success"])
        final_dict["failed"].extend(temp_dict["failed"])

        # retry 永遠只保留最新一輪
        final_dict["retry"] = temp_dict["retry"]


    elapse = time.perf_counter() - start

    logger.info(f"本輪總共花費時間:{elapse:.2f}s")

    for status, items in final_dict.items():
        logger.info(f"{status}: {len(items)}")

    slack_batch_pipe_notify( text_summary(final_dict) )

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()   