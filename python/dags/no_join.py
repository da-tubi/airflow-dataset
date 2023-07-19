 # +---------------- minute (0 - 59)
 # |  +------------- hour (0 - 23)
 # |  |  +---------- day of month (1 - 31)
 # |  |  |  +------- month (1 - 12)
 # |  |  |  |  +---- day of week (0 - 6) (Sunday=0 or 7)
 # |  |  |  |  |
 # *  *  *  *  *  command to be executed

import pendulum

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

name = "xyz"

ods_dst = Dataset(f"s3://bucket/{name}_ods")
dwd_dst = Dataset(f"s3://bucket/{name}_dwd")
dwm_dst = Dataset(f"s3://bucket/{name}_dwm")

with DAG(
    dag_id=f"{name}_ods",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
) as dag:
    BashOperator(outlets=[ods_dst], task_id="task_ods", bash_command="echo ods on {{ ds }}")

with DAG(
    dag_id=f"{name}_dwd",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[ods_dst],
) as dag:
    BashOperator(
        outlets=[dwd_dst],
        task_id="task_dwd",
        bash_command="echo dwd on {{ ds }}",
    )

with DAG(
    dag_id=f"{name}_dwm",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[dwd_dst],
) as dag:
    BashOperator(
        outlets=[dwm_dst],
        task_id="task_dwm",
        bash_command="echo dwm on {{ ds }}",
    )
