"""
BATCH LAYER orchestration DAG.

Triggered once per simulated day (either by the tariff_batch_source.py
simulator calling the Airflow REST API, or by its own schedule as a
fallback safety net). For the target sim_date it:

  1. Waits for the tariff/weather CSV files to actually be present
     (FileSensor) -- handles the case where the DAG run was scheduled
     slightly ahead of the file drop.
  2. Runs the Spark batch job (spark/batch_job.py) via spark-submit, which
     joins the day's raw streaming archive with the tariff/weather files and
     writes household_bills + zone_solar_daily to Postgres.
  3. Records success/failure to `batch_run_log` (used by the API's /health
     and Prometheus /metrics) via a lightweight Python callback.
  4. Runs a final health-check task that queries Postgres to confirm the
     expected number of rows landed, and raises (failing the DAG run, which
     Airflow surfaces in its UI/alerting) if not -- the orchestration-layer
     half of the Observability requirement.

sim_date is picked up from the DAG run's `conf` (passed by the simulator) or
falls back to "today" in YYYYMMDD form for a manually-triggered/scheduled run.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    HAVE_SPARK_PROVIDER = True
except ImportError:  # spark provider not installed -> fall back to BashOperator
    HAVE_SPARK_PROVIDER = False
from airflow.operators.bash import BashOperator

logger = logging.getLogger("airflow.smart_grid_batch_dag")

BATCH_DATA_DIR = os.environ.get("BATCH_DATA_DIR", "/data/batch_incoming")
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = os.environ.get("PG_DB", "smartgrid")
PG_USER = os.environ.get("PG_USER", "smartgrid")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "smartgrid")

default_args = {
    "owner": "smart-grid-pipeline",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": False,
}


def _tariff_file_exists(**context) -> bool:
    sim_date = context["ti"].xcom_pull(key="sim_date", task_ids="resolve_sim_date")
    path = os.path.join(BATCH_DATA_DIR, f"tariff_{sim_date}.csv")
    exists = os.path.exists(path)
    logger.info(json.dumps({"component": "airflow", "stage": "orchestration",
                             "event": "tariff_file_poke", "path": path, "exists": exists}))
    return exists


def _weather_file_exists(**context) -> bool:
    sim_date = context["ti"].xcom_pull(key="sim_date", task_ids="resolve_sim_date")
    path = os.path.join(BATCH_DATA_DIR, f"weather_{sim_date}.csv")
    exists = os.path.exists(path)
    logger.info(json.dumps({"component": "airflow", "stage": "orchestration",
                             "event": "weather_file_poke", "path": path, "exists": exists}))
    return exists


def _resolve_sim_date(**context) -> str:
    conf = context["dag_run"].conf if context.get("dag_run") else {}
    sim_date = (conf or {}).get("sim_date")
    if not sim_date:
        sim_date = context["logical_date"].strftime("%Y%m%d")
    context["ti"].xcom_push(key="sim_date", value=sim_date)
    logger.info(json.dumps({"component": "airflow", "stage": "orchestration",
                             "event": "sim_date_resolved", "sim_date": sim_date}))
    return sim_date


def _record_batch_result(status: str, **context) -> None:
    import psycopg2

    sim_date = context["ti"].xcom_pull(key="sim_date", task_ids="resolve_sim_date")
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batch_run_log (sim_date, status, detail, run_at) VALUES (%s, %s, %s, now())",
                (sim_date, status, f"DAG run {context['run_id']}"),
            )
        conn.commit()
    finally:
        conn.close()
    logger.info(json.dumps({"component": "airflow", "stage": "orchestration",
                             "event": "batch_run_recorded", "sim_date": sim_date, "status": status}))


def _health_check_after_batch(**context) -> None:
    """Verifies the batch job actually landed rows for this sim_date --
    the orchestration-layer observability check. Raises to fail the DAG
    (and therefore alert) if the batch silently produced nothing."""
    import psycopg2

    sim_date = context["ti"].xcom_pull(key="sim_date", task_ids="resolve_sim_date")
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM household_bills WHERE sim_date = %s", (sim_date,))
            bill_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM zone_solar_daily WHERE sim_date = %s", (sim_date,))
            zone_count = cur.fetchone()[0]
    finally:
        conn.close()

    logger.info(json.dumps({"component": "airflow", "stage": "orchestration",
                             "event": "post_batch_health_check", "sim_date": sim_date,
                             "bill_rows": bill_count, "zone_rows": zone_count}))

    if bill_count == 0 or zone_count == 0:
        raise RuntimeError(
            f"Post-batch health check FAILED for sim_date={sim_date}: "
            f"household_bills={bill_count} zone_solar_daily={zone_count}"
        )


with DAG(
    dag_id="smart_grid_batch_dag",
    description="Daily batch layer: join raw meter archive with tariff/weather files, compute bills.",
    default_args=default_args,
    # No cron schedule: the tariff/weather simulator (sources/tariff_batch_source.py)
    # is the real trigger, calling this DAG's REST API with an explicit
    # conf={"sim_date": ...} for every simulated day it produces. A "*/5 * * * *"
    # safety-net schedule was tried here, but it fights the simulator's own
    # simulated clock: sim_date_str() in sources/common.py always counts from a
    # fixed 2026-09-01 regardless of the real calendar date, while a scheduled
    # (non-REST-triggered) DagRun has no conf and falls back to resolving
    # sim_date from *today's real date* -- which never matches a file the
    # simulator actually wrote, so every scheduled run failed by design. With
    # no cron schedule, this DAG only ever runs when explicitly triggered
    # (by the simulator, or manually), which is the only path that's correct.
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smart-grid", "batch-layer", "lambda-architecture"],
) as dag:

    resolve_sim_date = PythonOperator(
        task_id="resolve_sim_date",
        python_callable=_resolve_sim_date,
    )

    # Plain Python existence checks rather than airflow.sensors.filesystem.FileSensor:
    # FileSensor needs an Airflow Connection (fs_default, of type "File (path)")
    # to resolve its base path, and on a fresh metadata DB that connection can
    # be missing or misconfigured, which makes the sensor error out on its very
    # first poke instead of genuinely waiting for the file. A direct os.path.exists()
    # check has no such dependency.
    wait_for_tariff_file = PythonSensor(
        task_id="wait_for_tariff_file",
        python_callable=_tariff_file_exists,
        poke_interval=10,
        timeout=280,  # give up just under one simulated day so DAG runs don't pile up
        mode="poke",
    )

    wait_for_weather_file = PythonSensor(
        task_id="wait_for_weather_file",
        python_callable=_weather_file_exists,
        poke_interval=10,
        timeout=280,
        mode="poke",
    )

    spark_submit_cmd = (
        "spark-submit --master " + SPARK_MASTER +
        " --packages org.postgresql:postgresql:42.7.3"
        " /opt/spark_jobs/batch_job.py --sim-date "
        "{{ ti.xcom_pull(key='sim_date', task_ids='resolve_sim_date') }}"
    )

    if HAVE_SPARK_PROVIDER:
        run_batch_job = SparkSubmitOperator(
            task_id="run_spark_batch_job",
            application="/opt/spark_jobs/batch_job.py",
            conn_id="spark_default",
            packages="org.postgresql:postgresql:42.7.3",
            application_args=[
                "--sim-date", "{{ ti.xcom_pull(key='sim_date', task_ids='resolve_sim_date') }}"
            ],
        )
    else:
        run_batch_job = BashOperator(task_id="run_spark_batch_job", bash_command=spark_submit_cmd)

    post_batch_health_check = PythonOperator(
        task_id="post_batch_health_check",
        python_callable=_health_check_after_batch,
    )

    record_success = PythonOperator(
        task_id="record_success",
        python_callable=_record_batch_result,
        op_kwargs={"status": "SUCCESS"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    record_failure = PythonOperator(
        task_id="record_failure",
        python_callable=_record_batch_result,
        op_kwargs={"status": "FAILED"},
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    resolve_sim_date >> [wait_for_tariff_file, wait_for_weather_file] >> run_batch_job
    run_batch_job >> post_batch_health_check >> record_success
    [run_batch_job, post_batch_health_check] >> record_failure
