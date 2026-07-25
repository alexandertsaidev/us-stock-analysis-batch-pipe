import sys

import random
import logging

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

import duckdb
import pyarrow as pa

from fake_useragent import UserAgent

from ...config.minio_duckdb_conn import get_duckdb_conn
from ...config.minio_conn import MINIO_BUCKET

from ...utils.helpers import get_pa_table
from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

# 爬蟲
def scrape_US_tickers(
    url="https://www.sec.gov/files/company_tickers.json",
    retries=5,
    sleep_range=(1, 10)
    ):

    attempt = 0
    data_json = None

    while attempt < retries:
        try:
            ua = UserAgent()
            suffix = random.randint(1111, 9999)
            headers = {
                "User-Agent": f"{ua.random} (contact: my_name{suffix}@gmail.com)"
            }

            logger.info(f"Try {attempt + 1} | User-Agent: {headers['User-Agent']}")
            response = requests.get(url, headers=headers, timeout=15)
            data_json = response.json()
            break

        except Exception as e:
            attempt += 1
            logger.warning(f"Failed: {e} | Retry {attempt}/{retries}")
            time.sleep(random.uniform(*sleep_range))

    return data_json

def upsert_co_list(
    conn: duckdb.DuckDBPyConnection,
    json_data,
    bucket: str,
    object_name: str
    ):

    if not json_data:
        logger.warning("json_data 為空，略過")
        return

    tz = "America/New_York"
    batch_timestamp = datetime.now(ZoneInfo(tz))

    rows = [
        {
            "cik":          row["cik_str"],
            "ticker":       row["ticker"],
            "title":        row["title"],
            "last_updated": batch_timestamp,
            "is_active":    True,
        }
        for row in json_data.values()
    ]

    new_table = pa.Table.from_pylist(rows)
    conn.register("new_data", new_table)

    try:
        get_pa_table(conn, bucket, object_name)

    except FileNotFoundError as e:

        new_arrow = conn.execute(f"""
            SELECT * FROM new_data
        """).to_arrow_table()

        save_parquet_to_minio(
            new_arrow,
            bucket,
            object_name
        )

        return

    except Exception as e:
        return

    merged_arrow = conn.execute(f"""
        SELECT * FROM (
            SELECT
                e."cik",
                e."ticker",
                e."title",
                e."last_updated",
                FALSE AS "is_active"
            FROM read_parquet('s3://{bucket}/{object_name}') AS e
            WHERE (e."cik", e."ticker") NOT IN (
                SELECT "cik", "ticker" FROM new_data
            )

            UNION ALL

            SELECT * FROM new_data
        )
    """).to_arrow_table()

    save_parquet_to_minio(
        merged_arrow,
        bucket,
        object_name
    )

    return

def main():
    object_name = "stock/company_list/us_tickers_list.parquet"
    data_json = scrape_US_tickers()
    # data_json ={
    #     "0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"},
    #     "1":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
    #     "2":{"cik_str":1652044,"ticker":"GOOGL","title":"Alphabet Inc."},
    #     "3":{"cik_str":789019,"ticker":"MSFT","title":"MICROSOFT CORP"},
    #     "4":{"cik_str":1018724,"ticker":"AMZN","title":"AMAZON COM INC"},
    #     "5":{"cik_str":1730168,"ticker":"AVGO","title":"Broadcom Inc."},
    #     "6":{"cik_str":1326801,"ticker":"META","title":"Meta Platforms, Inc."},
    #     "7":{"cik_str":1318605,"ticker":"TSLA","title":"Tesla, Inc."},
    #     "8":{"cik_str":1067983,"ticker":"BRK-B","title":"BERKSHIRE HATHAWAY INC"},
    #     "9":{"cik_str":104169,"ticker":"WMT","title":"Walmart Inc."}
    # }

    with get_duckdb_conn() as conn:

        upsert_co_list(
            conn=conn,
            json_data=data_json,
            bucket=MINIO_BUCKET,
            object_name=object_name,
        )

    return

if __name__ == "__main__":
    main()
    countdown(10)

sys.exit()