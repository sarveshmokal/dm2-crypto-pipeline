from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "dm2-crypto-microstructure"
REPO    = "/home/g100004344/dm2-crypto-pipeline"
DBT_DIR = f"{REPO}/dbt/crypto_dbt"

# make sure the main-env tools (python3, dbt) are found, not the airflow venv
ENV_PATH = "export PATH=$HOME/.local/bin:/usr/bin:/usr/local/bin:$PATH"

default_args = {
    "owner": "sarvesh",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="crypto_pipeline",
    description="Binance -> bronze -> silver (spark) -> gold (dbt)",
    default_args=default_args,
    schedule_interval="@hourly",      # runs automatically every hour
    start_date=datetime(2026, 6, 1),
    catchup=False,                    # don't backfill old runs
    tags=["crypto", "medallion"],
) as dag:

    # 1. ingest live trades -> GCS
    ingest_trades = BashOperator(
        task_id="ingest_trades",
        bash_command=f"{ENV_PATH} && cd {REPO} && python3 ingestion/ingest_trades_to_gcs.py",
    )

    # 2. load raw json from GCS into the bronze table (python client, consistent auth)
    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"{ENV_PATH} && cd {REPO} && python3 ingestion/load_trades_to_bronze.py",
    )

    # 3. bronze -> silver with pyspark
    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=f"{ENV_PATH} && cd {REPO} && python3 spark/silver_trades_minute.py",
    )

    # 4. silver -> gold with dbt (build + test)
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=f"{ENV_PATH} && cd {DBT_DIR} && dbt run && dbt test",
    )

    # dependency order: each runs only after the previous succeeds
    ingest_trades >> load_bronze >> spark_silver >> dbt_gold
