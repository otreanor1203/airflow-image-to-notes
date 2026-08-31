import json
import os
from datetime import datetime
from pathlib import Path
import urllib.error
import urllib.request

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

try:
    from tasks.detect_image import detect_image
    from tasks.ocr_image import ocr_image
    from tasks.revise_notes import revise_notes
    from tasks.export_notes import (
        export_one,
        export_txt,
        export_word,
        choose_export_format,
    )
except ModuleNotFoundError:
    from dags.tasks.detect_image import detect_image
    from dags.tasks.ocr_image import ocr_image
    from dags.tasks.revise_notes import revise_notes
    from dags.tasks.export_notes import (
        export_one,
        export_txt,
        export_word,
        choose_export_format,
    )


DEFAULT_DECISION_DIR = "/home/owen/airflow/user_decisions"
DECISION_DIR = Path(
    os.environ.get("AIRFLOW_DECISION_DIR", DEFAULT_DECISION_DIR)
)


def decision_path(dag_run_id):
    return DECISION_DIR / f"{dag_run_id}.json"


def wait_for_user_choice(**context):
    dag_run = context["dag_run"]
    return decision_path(dag_run.run_id).exists()


def load_user_choice(**context):
    dag_run = context["dag_run"]
    path = decision_path(dag_run.run_id)

    if not path.exists():
        raise ValueError(f"User decision not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        decision = json.load(f)

    notes = str(decision.get("notes") or "").strip()
    prompt = str(decision.get("prompt") or "").strip()
    enhance = bool(decision.get("enhance"))

    if not notes:
        ti = context["ti"]
        previous = ti.xcom_pull(task_ids="revise_notes")

        if isinstance(previous, dict):
            notes = str(previous.get("revised_text") or "").strip()

    if not notes:
        raise ValueError("No notes were supplied.")

    return {
        "notes": notes,
        "prompt": prompt,
        "enhance": enhance,
    }


def choose_enhancement_branch(**context):
    dag_run = context["dag_run"]
    path = decision_path(dag_run.run_id)

    with path.open("r", encoding="utf-8") as f:
        decision = json.load(f)

    if decision.get("enhance") is True:
        return "gpt_enhance"

    return "choose_export_format"


def enhance_notes(**context):
    ti = context["ti"]

    choice = ti.xcom_pull(task_ids="load_user_choice") or {}

    notes = str(choice.get("notes") or "").strip()
    prompt = str(choice.get("prompt") or "").strip()

    if not notes:
        raise ValueError("No notes are available for GPT enhancement.")

    if not prompt:
        raise ValueError("No GPT prompt was supplied.")

    api_key = os.environ.get("OPENAI_API_KEY")

    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY is not configured for Airflow."
        )

    payload = {
        "model": os.environ.get(
            "OPENAI_MODEL",
            "gpt-4o-mini",
        ),
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a note-enhancement assistant. "
                    "Improve and polish the user's notes without "
                    "inventing facts. Preserve the original meaning, "
                    "fix formatting issues, and make the notes cleaner "
                    "and more readable. Return only the final enhanced "
                    "notes as plain text."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"User prompt: {prompt}\n\n"
                    f"Original notes:\n{notes}"
                ),
            },
        ],
        "temperature": 0.4,
    }

    request = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=120,
        ) as response:
            data = json.loads(
                response.read().decode("utf-8")
            )

    except urllib.error.HTTPError as exc:
        body = exc.read().decode(
            "utf-8",
            errors="replace",
        )
        raise RuntimeError(
            f"OpenAI request failed ({exc.code}): {body}"
        ) from exc

    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Could not reach OpenAI: {exc.reason}"
        ) from exc

    final_notes = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )

    if not final_notes:
        raise ValueError(
            "OpenAI returned no enhanced notes."
        )

    return {
        "final_notes": final_notes,
        "status": "enhanced",
        "prompt": prompt,
    }


default_args = {
    "owner": "owentreanor",
}


with DAG(
    dag_id="hackathon",
    description=(
        "Turn photographed notes into downloadable "
        "text, Word, or OneNote files."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    tags=[
        "hackathon",
        "airflow",
        "ocr",
        "mizuho",
    ],
    schedule=None,
    catchup=False,
) as dag:

    detect = PythonOperator(
        task_id="detect_image",
        python_callable=detect_image,
    )

    ocr = PythonOperator(
        task_id="ocr_image",
        python_callable=ocr_image,
        op_args=[detect.output],
    )

    revise = PythonOperator(
        task_id="revise_notes",
        python_callable=revise_notes,
    )

    wait_for_choice = PythonSensor(
        task_id="wait_for_user_choice",
        python_callable=wait_for_user_choice,
        poke_interval=2,
        timeout=60 * 60,
        mode="reschedule",
    )

    load_choice = PythonOperator(
        task_id="load_user_choice",
        python_callable=load_user_choice,
    )

    choose_enhancement = BranchPythonOperator(
        task_id="choose_enhancement",
        python_callable=choose_enhancement_branch,
    )

    gpt_enhance = PythonOperator(
        task_id="gpt_enhance",
        python_callable=enhance_notes,
    )

    choose_export = BranchPythonOperator(
        task_id="choose_export_format",
        python_callable=choose_export_format,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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


detect >> ocr >> revise >> wait_for_choice >> load_choice >> choose_enhancement

choose_enhancement >> [
    gpt_enhance,
    choose_export,
]

gpt_enhance >> choose_export

choose_export >> [
    export_txt_task,
    export_word_task,
    export_one_task,
]