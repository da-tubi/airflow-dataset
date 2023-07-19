 # +---------------- minute (0 - 59)
 # |  +------------- hour (0 - 23)
 # |  |  +---------- day of month (1 - 31)
 # |  |  |  +------- month (1 - 12)
 # |  |  |  |  +---- day of week (0 - 6) (Sunday=0 or 7)
 # |  |  |  |  |
 # *  *  *  *  *  command to be executed

import pendulum
import pprint

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

name = "xyz"

ods_dst = Dataset(f"s3://bucket/{name}_ods")
dwd_dst = Dataset(f"s3://bucket/{name}_dwd")
dwm_dst = Dataset(f"s3://bucket/{name}_dwm")

def print_context(**kwargs):
    pprint.pprint(kwargs)

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
    dag_id=f"{name}_dwm",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[dwd_dst],
) as dag:
    BashOperator(
        task_id="task_dwm",
        outlets=[dwm_dst],
        bash_command="echo {{ data_interval_start | ds }}",
    )
