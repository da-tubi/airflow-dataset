from __future__ import annotations

import pendulum
import pprint

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

name = "weekly"

ods_dst = Dataset(f"s3://bucket/{name}_ods")
dwd_dst = Dataset(f"s3://bucket/{name}_dwd")
ads_dst = Dataset(f"s3://bucket/{name}_ads")

def print_context(**kwargs):
    pprint.pprint(kwargs)

@task.branch()
def should_run(**kwargs) -> str:
    """
    Determine which empty_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    """
    if kwargs["data_interval_start"].day_of_week == 1:
        return "ads_monday"
    else:
        return "ads_skip"


with DAG(
    dag_id=f"{name}_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        outlets=[ods_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_dwd",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[ods_dst],
) as dag:
    PythonOperator(
        task_id="task_dwd",
        outlets=[dwd_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_ads",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[dwd_dst],
) as dag:
    cond = should_run()
    skip = EmptyOperator(task_id="ads_skip")
    weekly = BashOperator(
        task_id="ads_monday",
        outlets=[ads_dst],
        bash_command="echo {{ data_interval_start | ds }}",
    )
    cond >> [skip, weekly]
