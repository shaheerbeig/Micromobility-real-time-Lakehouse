from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:
    # Airflow 3 location
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    # Airflow 2 location
    from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

SPARK_SUBMIT_COMMON = (
    "docker exec mmd-spark /opt/spark/bin/spark-submit "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--packages io.delta:delta-spark_2.12:3.2.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
)

with DAG(
    dag_id="pipeline_execution",
    description="Execute silver, gold, mart, then validation in sequence.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "execution"],
) as dag:
    silver = BashOperator(
        task_id="run_silver_layer",
        bash_command=SPARK_SUBMIT_COMMON + "/opt/spark_streaming/silver_writer.py",
    )

    gold = BashOperator(
        task_id="run_gold_layer",
        bash_command=SPARK_SUBMIT_COMMON + "/opt/spark_streaming/gold_writer.py",
    )

    mart = BashOperator(
        task_id="run_mart_layer",
        bash_command=SPARK_SUBMIT_COMMON + "/opt/spark_streaming/mart_writer.py",
    )

    validate = BashOperator(
        task_id="run_validation",
        bash_command=SPARK_SUBMIT_COMMON + "/opt/spark_streaming/validation.py",
    )

    silver >> gold >> mart >> validate
