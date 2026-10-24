from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="compose_smoke_test",
    description="Checks that the local Airflow scheduler can execute a task; not the final data pipeline.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["smart-energy", "setup"],
) as dag:
    verify_airflow = BashOperator(
        task_id="verify_airflow_task_execution",
        bash_command='echo "Airflow is running. Replace this smoke-test DAG with the actual Spark -> Snowflake -> dbt workflow."',
    )
