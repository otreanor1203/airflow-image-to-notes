import time

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import BranchPythonOperator, PythonOperator

try:
    from tasks.detect_image import detect_image
    from tasks.ocr_image import ocr_image
    from tasks.export_notes import export_one, export_txt, export_word, revise_notes, choose_export_format
except ModuleNotFoundError:  # pragma: no cover
    from dags.tasks.detect_image import detect_image
    from dags.tasks.ocr_image import ocr_image
    from dags.tasks.export_notes import export_one, export_txt, export_word, revise_notes, choose_export_format


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

    ocr = PythonOperator(
        task_id =  "ocr_image",
        python_callable=ocr_image,
        op_args=[detect.output]
    )

    revise = PythonOperator(
        task_id="revise_notes",
        python_callable=revise_notes,
    )

    choose_export = BranchPythonOperator(
        task_id="choose_export_format",
        python_callable=choose_export_format,
    )

    export_txt_task = PythonOperator(
        task_id="export_txt",
        python_callable=export_txt,
    )

    export_word_task = PythonOperator(
        task_id="export_word",
        python_callable=export_word,
    )

    export_one_task = PythonOperator(
        task_id="export_one",
        python_callable=export_one,
    )

detect >> ocr >> revise >> choose_export
choose_export >> [export_txt_task, export_word_task, export_one_task]
