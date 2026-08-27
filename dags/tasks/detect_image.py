import os
from pathlib import Path

def detect_image():
    uploads = Path("uploads")

    files = list(uploads.glob("*"))

    return "/home/owen/airflow/" + str(files[0]) if files else None
