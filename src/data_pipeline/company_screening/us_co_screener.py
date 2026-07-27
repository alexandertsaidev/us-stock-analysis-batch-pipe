import io
import sys
from pathlib import Path

import logging

from botocore.exceptions import ClientError

import duckdb

import pyarrow as pa
import pyarrow.parquet as pq

from ...notify.slack_notify import slack_pipe_notify

from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...utils.helpers import save_parquet_to_minio
from ...utils.helpers import get_pa_table

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)


def get_co_fund(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    object_name: str
    ) -> pa.Table:

    try:
        pa_table = conn.execute(f"""
            SELECT *
            FROM read_parquet('s3://{bucket}/{object_name}')
        """).to_arrow_table()
        
        logger.info(f"從 {bucket}/{object_name} 取得 {pa_table.num_rows} 檔")
        return pa_table

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
    arrow_table: pa.Table,  # pa = pyarrow
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

    try:
        # Step 1：存 temp
        save_parquet_to_minio(arrow_table, bucket, temp_object_name)

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

            logger.info(f"本次未找到之前的 us_all_co_screen.parquet ...\n正在初始化 {arrow_merged.num_rows} 筆")

        conn.close()

        # Step 4：寫回 final
        save_parquet_to_minio(arrow_merged, bucket, final_object_name)
        return True
    
    except Exception as e:
        logger.error(f"Upsert 失敗 - {e}", exc_info=True)
        return False

def text_summary(df) -> str:
    lines = ["🎯 2. 基本面篩選結果摘要"]
    lines.append(f"\n總筆數 : {len(df)}")
    lines.append(f"日期 : {df['created_at'].iloc[0]}")
    lines.append("")
    lines.append(df.to_string(index=False))

    return "\n".join(lines)


def main():

    with get_duckdb_conn() as conn:

        co_screen: pa.Table = get_co_fund(
            conn = conn,
            bucket= MINIO_BUCKET,
            object_name= f"stock/fundamentals/temp_us_co_fundamentals.parquet"
        )

        conn.register("us_co_screen", co_screen)

        result = conn.execute(
            Path("/opt/airflow/scripts/us-stock-analysis-batch-pipe/src/data_pipeline/company_screening/us_fundamentals_screen.sql")
            .read_text()
        ).to_arrow_table()

        _upsert_parquet_to_minio(
            conn = conn,
            arrow_table = result,
            bucket = MINIO_BUCKET,
            temp_object_name = "stock/screening/temp_us_co_screen.parquet",
            final_object_name = "stock/screening/us_all_co_screen.parquet",
        )

    logger.info(f"篩選結果：{result.num_rows} 筆")
    print(result.to_pandas())

    df = result.to_pandas()
    summary = text_summary(df)
    slack_pipe_notify(summary)


if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()