#importing packages
from email.policy import default
from unittest import result
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# constants
MY_NAME = "Bongz"
MY_NUMBER = 19

def multiply_by_24(number):
    """Multiplies a number by 24 and prints the results to Airflow logs"""
    result = number * 24
    print(result)

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2022,8,27),
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['Tutorial'],
    default_args={
        "owner": MY_NAME,
        "retries":2,
        "retry_delay":timedelta(minutes=5)
    }
) as dag:
#tasks
    t1 = BashOperator(
        task_id = "multiply_my_number_by_24",
        bash_command = f"echo {MY_NAME}"
    )

    t2 = PythonOperator(
        task_id="multiply_my_number_by_24",
        python_callable=multiply_by_24,
        op_kwargs={"number": MY_NUMBER}
    )
#dependencies
t1 >> t2