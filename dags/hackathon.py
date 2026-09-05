import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import urllib.error
import urllib.request
from dotenv import load_dotenv

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.task.trigger_rule import TriggerRule

try:
    from tasks.detect_image import detect_image
    from tasks.ocr_image import ocr_image
    from tasks.revise_notes import revise_notes
    from tasks.export_notes import (
        export_file,
    )
except ModuleNotFoundError:
    from dags.tasks.detect_image import detect_image
    from dags.tasks.ocr_image import ocr_image
    from dags.tasks.revise_notes import revise_notes
    from dags.tasks.export_notes import (
        export_file,
    )


def choose_enhancement_branch(**context):
    choice = context["ti"].xcom_pull(task_ids="choose_enhancement") or {}
    chosen_options = choice.get("chosen_options", [])

    if chosen_options and str(chosen_options[0]).lower() == "yes":
        return "gpt_prompt"

    return "choose_export_format"


def enhance_notes(**context):
    ti = context["ti"]

    choice = ti.xcom_pull(task_ids="gpt_prompt") or {}
    params_input = choice.get("params_input", {})

    notes_result = ti.xcom_pull(task_ids="revise_notes") or {}
    notes = str(
        params_input.get("notes")
        or notes_result.get("revised_text")
        or ""
    ).strip()
    prompt = str(params_input.get("prompt") or "").strip()

    if not notes:
        raise ValueError("No notes are available for GPT enhancement.")

    if not prompt:
        raise ValueError("No GPT prompt was supplied.")

    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")

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
                    "Follow the user's prompt carefully. Improve and "
                    "polish the notes, preserve their original meaning, "
                    "and fix formatting issues. If the user explicitly "
                    "requests new factual content, include accurate, "
                    "relevant additions and keep them clearly separated "
                    "from the original notes. Return only the final "
                    "enhanced notes as plain text."
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

    choose_enhancement = HITLEntryOperator(
        task_id="choose_enhancement",
        subject="Enhance your notes with GPT?",
        body="Choose Yes to provide an enhancement prompt, or No to export the notes as they are.",
        options=["yes", "no"],
        defaults=["no"],
        params={
            "notes": Param(
                "",
                type="string",
                title="Finalized notes",
                description="The finalized OCR text for the next task.",
            ),
        },
        response_timeout=timedelta(hours=1),
    )

    enhancement_branch = BranchPythonOperator(
        task_id="enhancement_branch",
        python_callable=choose_enhancement_branch,
    )

    gpt_prompt = HITLEntryOperator(
        task_id="gpt_prompt",
        subject="Enter a prompt for GPT",
        body="Tell GPT how you want the notes improved.",
        options=["submit"],
        defaults=["submit"],
        params={
            "notes": Param(
                "",
                type="string",
                title="Finalized notes",
                description="The finalized OCR text to enhance.",
            ),
            "prompt": Param(
                "",
                type="string",
                title="Enhancement prompt",
                description="Describe how GPT should improve the notes.",
            ),
        },
        response_timeout=timedelta(hours=1),
    )

    gpt_enhance = PythonOperator(
        task_id="gpt_enhance",
        python_callable=enhance_notes,
    )

    choose_export = HITLEntryOperator(
        task_id="choose_export_format",
        subject="Choose an export format",
        body="Select the file format to create.",
        options=["txt", "docx", "one"],
        defaults=["txt"],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        response_timeout=timedelta(hours=1),
    )

    export_file_task = PythonOperator(
        task_id="export_file",
        python_callable=export_file,
    )


detect >> ocr >> revise >> choose_enhancement >> enhancement_branch

enhancement_branch >> [
    gpt_prompt,
    choose_export,
]

gpt_prompt >> gpt_enhance
gpt_enhance >> choose_export
choose_export >> export_file_task
