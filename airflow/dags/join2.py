import pendulum
import pprint

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

main_dst = Dataset("s3://bucket/main_ods")

name = "join2"
dag_x = f"{name}_x_ods"
dag_y = f"{name}_y_ods"
dag_x_task = "task_ods"
dag_y_task = "task_ods"


def print_context(**kwargs):
    pprint.pprint(kwargs)

def gen_date_fn(delta):
    def inner_fn(logical_date, **kwargs):
        return kwargs["data_interval_start"] - delta

    return inner_fn

with DAG(
    dag_id=f"{name}_x_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_y_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_main_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        outlets=[main_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_dwd",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[main_dst],
) as dag:
    x = ExternalTaskSensor(
        task_id = "wait_for_x_ods",
        external_dag_id=dag_x,
        external_task_id=dag_x_task,
        poke_interval=60 * 1,
        mode='reschedule',
        execution_date_fn=gen_date_fn(timedelta(hours=1))
    )
    y = ExternalTaskSensor(
        task_id = "wait_for_y_ods",
        external_dag_id=dag_y,
        external_task_id=dag_y_task,
        poke_interval=60 * 1,
        mode='reschedule',
        execution_date_fn=gen_date_fn(timedelta(hours=-1))
    )
    dwd = PythonOperator(
        task_id="task_dwd",
        python_callable=print_context
    )
    x >> dwd
    y >> dwd
