from airflow import DAG
from conveyor.operators import ConveyorContainerOperatorV2
from datetime import timedelta
from airflow.utils import dates


default_args = {
    "owner": "Conveyor",
    "depends_on_past": False,
    "start_date": dates.days_ago(2),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

role = "dbt-duckdb-webinar-{{ macros.datafy.env() }}"

dag = DAG(
    "webinar_coffee_shop_dbt",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
)

staging = ConveyorContainerOperatorV2(
    dag=dag,
    task_id="staging",
    aws_role=role,
    arguments=[
        "run",
        "--target", "dev",
        "--select", "staging",
    ],
)

staging_icerberg = ConveyorContainerOperatorV2(
    dag=dag,
    task_id="staging_iceberg",
    aws_role=role,
    arguments=[
        "run",
        "--target", "dev",
        "--select", "staging-iceberg",
    ],
)


marts = ConveyorContainerOperatorV2(
    dag=dag,
    task_id="marts",
    aws_role=role,
    arguments=[
        "run",
        "--target", "dev",
        "--select", "marts",
    ],
)

staging >>  marts
staging_icerberg
