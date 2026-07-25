import time
from datetime import datetime
from zoneinfo import ZoneInfo

import io
import sys
import os

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import logging
from botocore.exceptions import ClientError

from ..config.minio_conn import s3_client


def countdown(seconds):
    for i in range(seconds, 0, -1):
        print("Close in "+ str(i) +" seconds..." , end='\r')
        time.sleep(1)

def get_time_str(mode):
    """
    mode:
        "ymd" -> YYYYMMDD
        "ym"  -> YYYYMM
        "full"-> YYYYMMDD_HHMMSS
    """
    now = datetime.now()

    if mode == "ymd":
        return now.strftime("%Y%m%d")
    elif mode == "ym":
        return now.strftime("%Y%m")
    elif mode == "full":
        return now.strftime("%Y%m%d_%H%M%S")
    else:
        raise ValueError(f"Unknown mode: {mode}")

def get_now(tz: str = "America/New_York") -> datetime:
    return datetime.now(ZoneInfo(tz))

def get_pa_table(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    object_name: str
    ) -> pa.Table:
    
    logger = logging.getLogger(__name__)

    try:
        pa_table = conn.execute(f"""
            SELECT *
            FROM read_parquet('s3://{bucket}/{object_name}')
        """).to_arrow_table()
        
        logger.info(f"從 {bucket}/{object_name} 取得 {pa_table.num_rows} 檔")
        return pa_table

    except duckdb.HTTPException as e:
        if "404" in str(e):
            logger.warning(f"s3://{bucket}/{object_name} 不存在，視為首次執行")
            raise FileNotFoundError(f"找不到檔案: s3://{bucket}/{object_name}") from e  # 維持原本行為
        else:
            logger.error(f"DuckDB 讀取 S3 失敗 ({bucket}/{object_name}): {e}", exc_info=True)
            raise  # 403 / 其他錯誤 → 讓 Airflow fail

    except ClientError as e:
        # 保留 boto3 ClientError，以防其他地方仍用 s3_client
        logger.error(f"MinIO 讀取失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"讀取 {bucket}/{object_name} 發生未知錯誤: {e}", exc_info=True)
        raise

def save_parquet_to_minio(
    arrow_table: pa.Table,  # pa = pyarrow
    bucket: str,
    object_name: str,
    ):

    logger = logging.getLogger(__name__)

    try:
        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer, compression="snappy")
        buffer.seek(0)

        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(
            Bucket = bucket,
            Key = object_name,
            Body = buffer,
        )
        logger.info(f"已存入 {bucket}/{object_name}，共 {arrow_table.num_rows} 筆")

    except Exception as e:
        logger.error(f"MinIO 上傳失敗 - {e}", exc_info=True)
        raise

def save_df_as_parquet_to_minio(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    bucket: str,
    object_name: str,
    ) -> None:

    logger = logging.getLogger(__name__)
    
    try:
        conn.register("upload_df", df)
        arrow_table = conn.execute("""
            SELECT "Date"::DATE AS "Date", * EXCLUDE ("Date")
            FROM upload_df
        """).to_arrow_table()

        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer, compression="snappy")
        buffer.seek(0)

        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(
            Bucket=bucket, 
            Key=object_name,
            Body=buffer
        )
        logger.info(f"✓ 上傳完成：{bucket}/{object_name}")

    except Exception as e:
        logger.error(f"✗ 上傳失敗：{object_name} - {e}", exc_info=True)
        raise


