from airflow import DAG
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

BATCH_PYTHON = "/opt/venvs/us-stock-analysis-batch-pipe/bin/python"

with DAG(
    dag_id='stock_analyzer_us_2',
    start_date=datetime(2024, 1, 1, tzinfo=timezone('America/New_York')),
    schedule='0 18 * * 1-5',
    catchup=False,
    tags=['stock_analyzer_us_2'],
) as dag:

    def run_fetcher():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_history_prices.history_price_fetcher"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_cleaner():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_history_prices.history_price_cleaner"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_calculator():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_history_prices.history_price_calculator"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_unioner():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.data_pipeline.company_history_prices.history_price_union"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_upload_sheet():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "src.upload_pipeline.upload_sheet"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    def run_upload_snowflake():
        import subprocess, sys, os
        result = subprocess.run(
            [sys.executable, "-m", "upload_pipeline.minio_to_snowflake"],
            cwd="/opt/airflow/scripts/us-stock-analysis-batch-pipe",
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": "/opt/airflow/scripts/us-stock-analysis-batch-pipe/src"},
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    fetcher = ExternalPythonOperator(
        task_id='fetcher',
        python=BATCH_PYTHON,
        python_callable=run_fetcher,
    )
    cleaner = ExternalPythonOperator(
        task_id='cleaner',
        python=BATCH_PYTHON,
        python_callable=run_cleaner,
    )
    calculator = ExternalPythonOperator(
        task_id='calculator',
        python=BATCH_PYTHON,
        python_callable=run_calculator,
    )
    unioner = ExternalPythonOperator(
        task_id='unioner',
        python=BATCH_PYTHON,
        python_callable=run_unioner,
    )
    conclusion = BashOperator(
        task_id='entry_conclusion',
        bash_command=(
            "/opt/venvs/us-stock-analysis-batch-pipe/bin/dbt run "
            "--project-dir /opt/airflow/scripts/us-stock-analysis-batch-pipe/src/dbt_project "
            "--profiles-dir /opt/airflow/scripts/us-stock-analysis-batch-pipe/src/dbt_project "
            "--select +entry_conclusion"
        ),
    )
    upload_sheet = ExternalPythonOperator(
        task_id='upload_sheet',
        python=BATCH_PYTHON,
        python_callable=run_upload_sheet,
    )
    upload_snowflake = ExternalPythonOperator(
        task_id='upload_snowflake',
        python=BATCH_PYTHON,
        python_callable=run_upload_snowflake,
    )

    fetcher >> cleaner >> calculator
    calculator >> [unioner, upload_snowflake]
    unioner >> conclusion >> upload_sheet