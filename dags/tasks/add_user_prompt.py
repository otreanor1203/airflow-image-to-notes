from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.sdk.definitions.param import Param


def add_user_prompt():
    return HITLEntryOperator(
        task_id="add_user_prompt",
        subject="Describe the notes",
        params={
            "note_context": Param(
                "",
                type="string",
                title="Notes Context",
                description="Briefly describe what these notes are about"
            )
        }
    )

