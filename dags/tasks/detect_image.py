import os
from pathlib import Path

def detect_image(**context):
    dag_run = context["dag_run"]
    image_path = dag_run.conf.get("image_path")
    if not image_path:
        raise ValueError("No image_path provided in dag_run.conf")

    return image_path
