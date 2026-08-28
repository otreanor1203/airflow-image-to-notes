import time

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

from tasks.detect_image import detect_image
from tasks.ocr_image import ocr_image
from tasks.add_user_prompt import add_user_prompt


default_args = {
    'owner' : 'owentreanor',
    # 'retries' : 2,
    # 'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = 'hackathon',
    description = 'Turn your photographed notes into a .docx file. To be submitted to Astronomer Beyond the Dag Hackathon.',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    tags=['hackathon', 'airflow', 'ocr', 'mizuho'],
    schedule=None,
    catchup=False,
) as dag:

    detect = PythonOperator(
        task_id="detect_image",
        python_callable=detect_image
    )

    user_prompt = add_user_prompt()

    ocr = PythonOperator(
        task_id =  "ocr_image",
        python_callable=ocr_image,
        op_args=[detect.output, user_prompt.output]
    )

detect >> user_prompt >> ocr
