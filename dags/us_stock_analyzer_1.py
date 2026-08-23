from airflow import DAG
from airflow.providers.standard.operators.python import ExternalPythonOperator
from datetime import datetime
from pendulum import timezone

BATCH_PYTHON = "/opt/venvs/us-stock-analysis-batch-pipe/bin/python"

with DAG(
    dag_id='us_stock_analyzer_1',
    start_date=datetime(2024, 1, 1, tzinfo=timezone('America/New_York')),
    schedule='0 19 1,15 * *',
    catchup=False,
    tags=["stock", "us", "batch"],
) as dag:

    def run_co_fetcher():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_symbols.us_co_symbol_fetcher"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_co_fund_fetcher():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_fundamentals.us_co_fundamentals_fetcher"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_co_screener():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_screening.us_co_screener"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_upload_sheet_screen():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.upload_pipeline.upload_sheet_screen"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    co_fetcher = ExternalPythonOperator(
        task_id='co_fetcher',
        python=BATCH_PYTHON,
        python_callable=run_co_fetcher,
    )
    co_fund_fetcher = ExternalPythonOperator(
        task_id='co_fund_fetcher',
        python=BATCH_PYTHON,
        python_callable=run_co_fund_fetcher,
    )
    co_screener = ExternalPythonOperator(
        task_id='co_screener',
        python=BATCH_PYTHON,
        python_callable=run_co_screener,
    )
    upload_sheet_screen = ExternalPythonOperator(
        task_id='upload_sheet',
        python=BATCH_PYTHON,
        python_callable=run_upload_sheet_screen,
    )
    co_fetcher >> co_fund_fetcher >> co_screener >> upload_sheet_screen
