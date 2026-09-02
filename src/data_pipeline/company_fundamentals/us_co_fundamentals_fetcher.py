import sys
import asyncio

import random
import logging

import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import yfinance as yf

import duckdb
import pyarrow as pa

from botocore.exceptions import ClientError
from yfinance.exceptions import YFRateLimitError, YFTickerMissingError, YFInvalidPeriodError

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...notify.slack_notify import slack_batch_pipe_notify

from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import get_pa_table
from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

def get_co_list(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        object_name: str
    ) -> list[str]:

    try:

        tickers = conn.execute(f"""
            SELECT DISTINCT "ticker"
            FROM read_parquet('s3://{bucket}/{object_name}')
            WHERE "is_active" = true
            ORDER BY "ticker" ASC
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

def _upsert_parquet_to_minio(
        conn: duckdb.DuckDBPyConnection,
        df_new: pd.DataFrame,
        bucket: str,
        temp_object_name: str,
        final_object_name: str,
    ) -> bool:

    """
    1. 新資料存成 temp.parquet
    2. 用 DuckDB SQL upsert 進 final parquet
       - 重複的 (created_at, ticker) → 新資料覆蓋
       - 新增的 → append
    3. 結果寫回 final.parquet
    """
    arrow_table_new = pa.Table.from_pandas(df_new, preserve_index=False)
    try:
        # Step 1：存 temp
        save_parquet_to_minio(arrow_table_new, bucket, temp_object_name)

        # Step 2：DuckDB SQL upsert
        conn.register("temp_data", get_pa_table(conn, bucket, temp_object_name))

        try:
            final_table = get_pa_table(conn, bucket, final_object_name)
            conn.register("final_data", final_table)
            final_exists = True

        except FileNotFoundError:
            final_exists = False
        
        if final_exists is True :

            arrow_merged = conn.execute("""
                SELECT
                    "created_at"::DATE AS "created_at",
                    * EXCLUDE ("created_at")
                FROM final_data
                WHERE ("created_at", "ticker") NOT IN (
                    SELECT "created_at", "ticker" FROM temp_data
                )
                UNION ALL
                SELECT
                    "created_at"::DATE AS "created_at",
                    * EXCLUDE ("created_at")
                FROM temp_data
            """).to_arrow_table()

            logger.info(f"Upsert 完成，合併後共 {arrow_merged.num_rows} 筆")
        else:
            # final 不存在，直接以 temp 作為初始 final
            arrow_merged = conn.execute("""
                SELECT
                    "created_at"::DATE AS "created_at",
                    * EXCLUDE ("created_at")
                FROM temp_data
            """).to_arrow_table()

            logger.info(f"本次未找到之前的 us_all_co_fundamentals.parquet ...\n正在初始化 {arrow_merged.num_rows} 筆")

        conn.close()

        # Step 4：寫回 final
        save_parquet_to_minio(arrow_merged, bucket, final_object_name)
        return True

    except Exception as e:
        logger.error(f"Upsert 失敗 - {e}", exc_info=True)
        return False

def fetch_fund(
        ticker: str,
    ):
    start = time.perf_counter()

    try:
        stock = yf.Ticker(ticker)
        market = stock.get_info()

        # 價格計算
        current_price = market.get("currentPrice", None)
        if not current_price :
            hist = stock.history(period='max', auto_adjust=False)
            if not hist.empty :
                current_price = hist["Close"].iloc[-1]

        hist_high = stock.history(period='max', auto_adjust=False)["Close"].max()
        hist_high_52w = market.get("fiftyTwoWeekHigh", None)

        price_ratio    = np.trunc((current_price / hist_high) * 100) / 100 if hist_high else None
        price_ratio_52w = np.trunc((current_price / hist_high_52w) * 100) / 100 if hist_high_52w else None

        row_data = {
            "created_at":                      datetime.now(timezone.utc).date(),
            "ticker":                          ticker,
            "市值":                              market.get("marketCap", None),
            "本益比(trailingP/E)":               market.get("trailingPE", None),
            "預期本益比(forwardP/E)":             market.get("forwardPE", None),
            "市銷率P/S":                         market.get("priceToSalesTrailing12Months", None),
            "流動比率":                          market.get("currentRatio", None),
            "產權比率/負債權益比":                market.get("debtToEquity", None),
            "ROE":                               market.get("returnOnEquity", None),
            "ROA(TTM)":                          market.get("returnOnAssets", None),
            "EPS(TTM)":                          market.get("epsTrailingTwelveMonths", None),
            "EPS年增率":                         market.get("earningsGrowth", None),
            "EPS預期":                           market.get("forwardEps", None),
            "淨利潤":                            market.get("netIncomeToCommon", None),
            "經營現金流":                        market.get("operatingCashflow", None),
            "營收年增率":                        market.get("revenueGrowth", None),
            "年銷售收入":                        market.get("totalRevenue", None),
            "目前價格":                          current_price,
            "52週最低價格":                      market.get("fiftyTwoWeekLow", None),
            "52週最高價格":                      hist_high_52w,
            "52週價格變化率":                    market.get("52WeekChange", None),
            "歷史新高率":                        price_ratio,
            "52週新高率":                        price_ratio_52w,
            "上一季日均成交":                     market.get("averageDailyVolume3Month", None),
            "機構持股比例":                      market.get("heldPercentInstitutions", None),
            "分析師平均評級":                    market.get("recommendationMean", None),
            "分析師建議":                        market.get("recommendationKey", None),
        }
        elapse = time.perf_counter() - start

        return ("success", ticker, elapse, row_data)
    
    except (YFTickerMissingError, YFInvalidPeriodError) as e:
        logger.warning(f"{ticker}: {e}")
        return ("failed", ticker, 0, {})

    except YFRateLimitError as e:
        logger.warning(f"{ticker}: {e}")
        return ("retry", ticker, 0, {})

    except Exception as e:
        logger.error(f"{ticker} failed: {e}", exc_info=True)
        return ("failed", ticker, 0, {})

async def async_fetch_fund(
        ticker: str,
    ) -> tuple:

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: fetch_fund(ticker)
    )

async def async_fetch_all(
        tickers: list,
    ):

    sem = asyncio.Semaphore(3)
    temp_dict = {"success": [], "retry": [], "failed": []}
    temp_list = []

    async def run(ticker):
        await asyncio.sleep(random.uniform(1, 1.5))

        async with sem:
            await asyncio.sleep(random.uniform(1.3, 2))
            status, ticker, elapse, row_dict = await async_fetch_fund(ticker)
            temp_dict[status].append((ticker, elapse))
            if row_dict:
                temp_list.append(row_dict)

    await asyncio.gather(*[run(t) for t in tickers], return_exceptions=True)

    return temp_dict, temp_list

def text_summary(final_dict: dict) -> str:
    success = final_dict["success"]
    retry   = final_dict["retry"]
    failed  = final_dict["failed"]

    lines = [
        "📊 1. 基本面抓取結果摘要",
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

        tickers = get_co_list(
            conn = conn,
            bucket = MINIO_BUCKET,
            object_name = f"stock/company_list/us_tickers_list.parquet"
        )

        final_dict, final_list = asyncio.run(async_fetch_all(tickers))

        retries = 3
        attempt = 0
        while attempt < retries :

            retry_tickers = [t for t, _ in final_dict["retry"] ]
            if not retry_tickers:
                break

            logger.info(f"Try {attempt + 1}/{retries}，共 {len(retry_tickers)} 檔")
            attempt += 1
            time.sleep(attempt * 15)

            temp_dict, temp_list = asyncio.run(async_fetch_all(retry_tickers))

            # success & failed 累加
            final_dict["success"].extend(temp_dict["success"])
            final_dict["failed"].extend(temp_dict["failed"])

            # retry 永遠只保留最新一輪
            final_dict["retry"] = temp_dict["retry"]
            
            # 已經成功爬蟲的資料 進行累加
            final_list.extend(temp_list)


        elapse = time.perf_counter() - start

        if final_list :

            df_all = pd.DataFrame(final_list)

            num_cols = [c for c in df_all.select_dtypes(include="number").columns ]

            # apply() 批量操作整欄or整列
            df_all[num_cols] = (
                df_all[num_cols]
                .apply(pd.to_numeric, errors="coerce")     # pd.to_numeric() 只能處理一維(Series),每欄轉成數值,不能轉的變 NaN
                .apply(lambda x: np.trunc(x * 1000) / 1000)  # 截斷到小數點3位
            )

            # 處理真正的 float inf
            df_all.replace([np.inf, -np.inf], np.nan, inplace=True)

            # 處理缺失值
            missing_values = ["None","none"]
            df_all.replace(missing_values, None, inplace=True)
            df_all.astype(object).where(pd.notnull(df_all), None)

            _upsert_parquet_to_minio(
                conn = conn,
                df_new = df_all,
                bucket = MINIO_BUCKET,
                temp_object_name = f"stock/fundamentals/temp_us_co_fundamentals.parquet",
                final_object_name = f"stock/fundamentals/us_all_co_fundamentals.parquet",
            )

    logger.info(f"本輪爬蟲總共花費時間:{elapse:.2f}s")

    for status, items in final_dict.items():
        logger.info(f"{status}: {len(items)}")

    slack_batch_pipe_notify( text_summary(final_dict) )

if __name__ == "__main__":
    main()
    countdown(10)

sys.exit()
