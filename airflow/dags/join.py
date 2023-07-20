import pendulum
import pprint

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

name = "join"

x_dst = Dataset("https://github.com/x/x")
y_dst = Dataset("https://3rdparty/api/v2")
z_dst = Dataset("s3://bucket/z_ods")
xyz_join_dst = Dataset(f"s3://bucket/{name}_dwd")
dwm_dst = Dataset(f"s3://bucket/{name}_dwm")

def print_context(**kwargs):
    pprint.pprint(kwargs)

with DAG(
    dag_id=f"{name}_x_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        outlets=[x_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_y_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        outlets=[y_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_z_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
) as dag:
    PythonOperator(
        task_id="task_ods",
        outlets=[z_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_xyz_dwd",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[x_dst, y_dst, z_dst],
) as dag:
    PythonOperator(
        task_id="task_dwd",
        outlets=[xyz_join_dst],
        python_callable=print_context
    )

with DAG(
    dag_id=f"{name}_xyz_dwm",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[xyz_join_dst],
) as dag:
    BashOperator(
        task_id="task_dwm",
        outlets=[dwm_dst],
        bash_command="echo {{ data_interval_start | ds }}",
    )

