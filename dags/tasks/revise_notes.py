from airflow.exceptions import AirflowSkipException


def revise_notes(**context):
    """Human edit/review step before export.

    Skippable: pass conf={"skip_review": true} when triggering the DAG
    to bypass this step entirely. Airflow will mark this task instance
    as 'skipped' in the UI rather than running it.
    """
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {} if dag_run is not None else {}

    if conf.get("skip_review"):
        raise AirflowSkipException(
            "Review step skipped via DAG run conf (skip_review=true)."
        )

    notes = conf.get("final_text") or conf.get("reviewed_text") or conf.get("notes") or ""

    if not notes:
        ti = context.get("ti")
        if ti is not None:
            previous = ti.xcom_pull(task_ids="ocr_image")
            if isinstance(previous, dict):
                notes = previous.get("transcribed_text") or ""

    if not notes:
        raise ValueError("No notes are available to revise.")

    revised_text = notes.strip()
    return {"revised_text": revised_text, "status": "revised"}
