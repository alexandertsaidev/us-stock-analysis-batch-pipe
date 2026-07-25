import sys
import asyncio

import random
import logging

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import yfinance as yf

import duckdb
import pyarrow as pa

from botocore.exceptions import ClientError
from yfinance.exceptions import YFRateLimitError, YFTickerMissingError, YFInvalidPeriodError

from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...notify.slack_notify import slack_pipe_notify

from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import get_pa_table
from ...utils.helpers import countdown

logger = logging.getLogger(__name__)


# 1. 取得 ticker 清單
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


# 3. 抓單一 ticker 基本面（同步）
def fetch_fund(ticker: str, now_date) -> dict:
    """
    抓取單一 ticker 基本面資料，回傳統一格式的 result dict。
    status: "success" | "failed" | "retry"
    """

    start = time.perf_counter()
    try:
        stock = yf.Ticker(ticker)
        market = stock.get_info()

        # ── 價格計算 ──────────────────────────
        current_price = market.get("currentPrice")
        if current_price is None:
            hist = stock.history(period='max', auto_adjust=False)
            if hist.empty:
                raise ValueError("history 為空，無法取得 current_price")
            current_price = hist["Close"].iloc[-1]

        hist_high = stock.history(period='max', auto_adjust=False)["Close"].max()
        hist_high_52w = market.get("fiftyTwoWeekHigh")

        price_ratio    = np.trunc((current_price / hist_high) * 100) / 100 if hist_high else None
        price_ratio_52w = np.trunc((current_price / hist_high_52w) * 100) / 100 if hist_high_52w else None

        # ── 整理欄位 ──────────────────────────
        row_data = {
            "created_at":                      now_date,
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
            "EPS增長率":                         market.get("earningsGrowth", None),
            "EPS預期":                           market.get("forwardEps", None),
            "淨利潤":                            market.get("netIncomeToCommon", None),
            "經營現金流":                        market.get("operatingCashflow", None),
            "年度收入增長":                      market.get("revenueGrowth", None),
            "年銷售收入":                        market.get("totalRevenue", None),
            "目前價格":                          current_price,
            "52週價格最低/priceLow52W":          market.get("fiftyTwoWeekLow", None),
            "52週價格最高/priceHigh52W":         hist_high_52w,
            "52週價格變化/priceChange52W":       market.get("52WeekChange", None),
            "歷史新高率":                        price_ratio,
            "52週新高率":                        price_ratio_52w,
            "上一季日均成交/avgDailyVolumeLastQuarter": market.get("averageDailyVolume3Month", None),
            "機構持股比例":                      market.get("heldPercentInstitutions", None),
            "分析師平均評級/analystRatingMean":  market.get("recommendationMean", None),
            "分析師建議/analystRatingKey":       market.get("recommendationKey", None),
        }

        elapse = time.perf_counter() - start
        logger.info(f"{ticker}: 抓取成功，花費 {elapse:.1f} 秒")
        return {
            "ticker": ticker,
            "row_data": row_data,
            "status": "success",
            "error":  None,
            "elapse": elapse,
        }

    except (YFTickerMissingError, YFInvalidPeriodError) as e:
        elapse = time.perf_counter() - start
        logger.warning(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "row_data": row_data,
            "status": "failed",
            "error":  None,
            "elapse": elapse,
        }
    
    except YFRateLimitError as e:
        elapse = time.perf_counter() - start
        logger.warning(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "row_data": row_data,
            "status": "retry",
            "error":  None,
            "elapse": elapse,
        }
    except Exception as e:
        elapse = time.perf_counter() - start
        logger.error(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "row_data": row_data,
            "status": "failed",
            "error":  None,
            "elapse": elapse,
        }

# 4. 非同步包裝（對應 fetch_price → fetch_one）
async def fetch_one(ticker: str, now_date) -> dict:
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: fetch_fund(ticker, now_date)
    )
    return result

# 5. 統籌並發（對應 fetch_all）
async def fetch_all(tickers: list[str], now_date):
    sem = asyncio.Semaphore(3)
    
    async def fetch_with_sem(ticker: str) -> dict:
        await asyncio.sleep(random.uniform(1, 1.5))
        async with sem:
            await asyncio.sleep(random.uniform(1.3, 2))
            result = await fetch_one(ticker, now_date)

        if result["status"] == "success":
            logger.info(f"{result['ticker']}: 抓取成功")

        return result

    async def fetch_with_retry(ticker: str, max_retry: int = 3) -> dict:
        result = None

        for attempt in range(1, max_retry + 1):
            if attempt > 1:
                wait = (attempt - 1) * 10
                logger.info(f"{ticker}: retry {attempt}/{max_retry}，等待 {wait} 秒")
                await asyncio.sleep(wait)

            result = await fetch_with_sem(ticker)

            if result["status"] != "retry":
                return result

        logger.warning(f"{ticker}: 超過最大重試次數 {max_retry}")
        return result

    # ── 並發執行所有 ticker ───────────────────
    total_start = time.perf_counter()
    results = await asyncio.gather(
        *[fetch_with_retry(t) for t in tickers],
        return_exceptions=True,
    )
    total_elapse = time.perf_counter() - total_start
    logger.info(f"本輪總花費時間: {total_elapse:.1f}s")

    # ── 分類結果 ─────────────────────────────
    success, retry, failed = [], [], []
    for r in results:
        if isinstance(r, Exception) or not isinstance(r, dict):
            failed.append(r)
        elif r["status"] == "success":
            success.append(r)
        elif r["status"] == "retry":
            retry.append(r)
        else:
            failed.append(r)

    return success, retry, failed

# 6. 摘要文字
def text_summary(success, retry, failed) -> str:
    lines = ["📊 ==基本面抓取結果摘要=="]

    lines.append(f"\n✅ 成功 ({len(success)})")
    for r in success:
        lines.append(f"  • {r['ticker']}（{r['elapse']:.2f}s）")

    lines.append(f"\n🔁 待重試 ({len(retry)})")
    for r in retry:
        lines.append(f"  • {r['ticker']}")

    lines.append(f"\n❌ 失敗 ({len(failed)})")
    for r in failed:
        if isinstance(r, dict):
            lines.append(f"  • {r['ticker']}：{r.get('error', 'unknown')}")
        else:
            lines.append(f"  • {r}")

    return "\n".join(lines)

# 7. Entry point
def main():

    tz = "America/New_York"
    now_date = datetime.now(ZoneInfo(tz)).date()

    with get_duckdb_conn() as conn:

        tickers = get_co_list(
            conn = conn,
            bucket = MINIO_BUCKET,
            object_name = f"stock/company_list/us_tickers_list.parquet"
        )
        success, retry, failed = asyncio.run(fetch_all(tickers, now_date))

        # 所有成功的 row dict → 一張 rows_data df → 一個 Parquet
        if success:
            rows_data = [r["row_data"] for r in success]
            df_all = pd.DataFrame(rows_data)

            exclude_cols = {"created_at","ticker","分析師建議/analystRatingKey"}
            # 處理 object 欄位中的 'Infinity' 字串
            obj_cols = [c for c in df_all.select_dtypes(include="object").columns if c not in exclude_cols]
            df_all[obj_cols] = df_all[obj_cols].apply(pd.to_numeric, errors="coerce")

            # 處理真正的 float inf
            df_all.replace([np.inf, -np.inf], np.nan, inplace=True)

            num_cols = [c for c in df_all.select_dtypes(include="number").columns if c not in exclude_cols]
            # apply() ,批量操作整欄or整列
            df_all[num_cols] = (
                df_all[num_cols]
                .apply(pd.to_numeric, errors="coerce")     # pd.to_numeric() 只能處理一維(Series),每欄轉成數值,不能轉的變 NaN
                .apply(lambda x: np.trunc(x * 1000) / 1000)  # 截斷到小數點3位
            )
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

    summary = text_summary(success, retry, failed)
    slack_pipe_notify(summary)

if __name__ == "__main__":
    main()
    countdown(10)

sys.exit()