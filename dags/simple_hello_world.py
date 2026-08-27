from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'owentreanor',
}

with DAG(
    'hello_world',
    description='A simple hello world DAG',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    tags=['beginner', 'bash', 'hello world'],
) as dag:

    task = BashOperator(
        task_id='hello_world_task',
        bash_command='echo Hello World',
    )

task
