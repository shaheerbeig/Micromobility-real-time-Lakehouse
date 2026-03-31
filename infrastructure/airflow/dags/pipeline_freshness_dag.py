from __future__ import annotations

import glob
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)

# Simple config-driven orchestration checks.
LAYER_CONFIG = {
    "bronze": {
        "path_pattern": "/opt/lakehouse/bronze/**/*.parquet",
        "max_age_minutes": 120,
        "min_file_count": 1,
        "fail_on_missing_files": True,
    },
    "silver": {
        "path_pattern": "/opt/lakehouse/silver/**/*.parquet",
        "max_age_minutes": 180,
        "min_file_count": 1,
        "fail_on_missing_files": True,
    },
    "gold": {
        "path_pattern": "/opt/lakehouse/gold/**/*.parquet",
        "max_age_minutes": 240,
        "min_file_count": 1,
        "fail_on_missing_files": True,
    },
    "mart": {
        "path_pattern": "/opt/lakehouse/mart/**/*.parquet",
        "max_age_minutes": 360,
        "min_file_count": 1,
        "fail_on_missing_files": False,
    },
}


def _latest_mtime(path_pattern: str) -> float | None:
    files = glob.glob(path_pattern, recursive=True)
    if not files:
        return None
    return max(os.path.getmtime(path) for path in files)


def _raise_or_warn(message: str, should_fail: bool) -> None:
    if should_fail:
        raise AirflowException(message)
    LOGGER.warning(message)


def check_layer_freshness(layer_name: str) -> None:
    run_ts = datetime.utcnow().isoformat()
    cfg = LAYER_CONFIG[layer_name]
    path_pattern = cfg["path_pattern"]
    max_age_minutes = cfg["max_age_minutes"]
    min_file_count = cfg["min_file_count"]
    fail_on_missing_files = cfg["fail_on_missing_files"]

    files = glob.glob(path_pattern, recursive=True)
    file_count = len(files)

    LOGGER.info("run_ts=%s layer=%s check_start path=%s", run_ts, layer_name, path_pattern)

    if file_count == 0:
        _raise_or_warn(
            f"run_ts={run_ts} layer={layer_name} no files found (path={path_pattern})",
            fail_on_missing_files,
        )
        return

    if file_count < min_file_count:
        raise AirflowException(
            f"run_ts={run_ts} layer={layer_name} low file count ({file_count} < {min_file_count})"
        )

    latest = _latest_mtime(path_pattern)
    if latest is None:
        _raise_or_warn(
            f"run_ts={run_ts} layer={layer_name} failed to resolve latest file timestamp",
            fail_on_missing_files,
        )
        return

    latest_dt = datetime.fromtimestamp(latest)
    age = datetime.now() - latest_dt

    if age > timedelta(minutes=max_age_minutes):
        raise AirflowException(
            f"run_ts={run_ts} layer={layer_name} stale data: latest={latest_dt}, age={age}, max_age_minutes={max_age_minutes}"
        )

    LOGGER.info(
        "run_ts=%s layer=%s status=ok file_count=%d latest=%s age=%s max_age_minutes=%d",
        run_ts,
        layer_name,
        file_count,
        latest_dt,
        age,
        max_age_minutes,
    )


def summarize_pipeline_health() -> None:
    run_ts = datetime.utcnow().isoformat()
    LOGGER.info("run_ts=%s summary_start", run_ts)
    LOGGER.info(
        "%-8s | %-10s | %-19s | %-18s | %-8s",
        "layer",
        "files",
        "latest_file_time",
        "age",
        "status",
    )
    LOGGER.info("%s", "-" * 78)

    for layer_name, cfg in LAYER_CONFIG.items():
        files = glob.glob(cfg["path_pattern"], recursive=True)
        file_count = len(files)
        latest = _latest_mtime(cfg["path_pattern"])

        if latest is None:
            latest_str = "-"
            age_str = "-"
            status = "WARN"
        else:
            latest_dt = datetime.fromtimestamp(latest)
            age = datetime.now() - latest_dt
            latest_str = latest_dt.strftime("%Y-%m-%d %H:%M:%S")
            age_str = str(age).split(".")[0]
            status = "OK" if age <= timedelta(minutes=cfg["max_age_minutes"]) else "STALE"

        LOGGER.info(
            "%-8s | %-10d | %-19s | %-18s | %-8s",
            layer_name,
            file_count,
            latest_str,
            age_str,
            status,
        )

    LOGGER.info("run_ts=%s summary_end", run_ts)


with DAG(
    dag_id="pipeline_freshness_monitor",
    start_date=datetime(2026, 3, 29),
    schedule=None,
    catchup=False,
    tags=["orchestration", "monitoring", "lakehouse"],
    default_args={
        "owner": "airflow",
        "email": ["admin@example.com"],
        "email_on_failure": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    bronze_check = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=check_layer_freshness,
        op_kwargs={"layer_name": "bronze"},
    )

    silver_check = PythonOperator(
        task_id="check_silver_freshness",
        python_callable=check_layer_freshness,
        op_kwargs={"layer_name": "silver"},
    )

    gold_check = PythonOperator(
        task_id="check_gold_freshness",
        python_callable=check_layer_freshness,
        op_kwargs={"layer_name": "gold"},
    )

    mart_check = PythonOperator(
        task_id="check_mart_freshness",
        python_callable=check_layer_freshness,
        op_kwargs={"layer_name": "mart"},
    )

    summary = PythonOperator(
        task_id="summarize_pipeline_health",
        python_callable=summarize_pipeline_health,
        trigger_rule="all_done",
    )

    [bronze_check, silver_check, gold_check, mart_check] >> summary
