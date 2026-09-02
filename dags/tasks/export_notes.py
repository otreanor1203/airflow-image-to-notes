import html
import json
import zipfile
from pathlib import Path


def create_export_file(notes: str, file_type: str = "txt", output_dir: str | None = None) -> str:
    """Create a final notes export file using Airflow-managed file generation logic."""
    if not notes or not isinstance(notes, str):
        raise ValueError("notes must be a non-empty string")

    normalized_type = (file_type or "txt").lower()
    export_root = Path(output_dir) if output_dir else Path(__file__).resolve().parents[2] / "exports"
    export_root.mkdir(parents=True, exist_ok=True)

    safe_name = "notes"

    if normalized_type in {"txt", "text"}:
        file_path = export_root / f"{safe_name}.txt"
        file_path.write_text(notes, encoding="utf-8")
        return str(file_path)

    if normalized_type in {"doc", "word"}:
        document = (
            "<!DOCTYPE html><html><head><meta charset='utf-8' />"
            "<title>Notes</title></head><body>"
            f"{html.escape(notes).replace(chr(10), '<br/>')}"
            "</body></html>"
        )
        file_path = export_root / f"{safe_name}.doc"
        file_path.write_text(document, encoding="utf-8")
        return str(file_path)

    if normalized_type == "docx":
        paragraphs = "".join(
            f"<w:p><w:r><w:t xml:space='preserve'>{html.escape(line)}</w:t></w:r></w:p>"
            for line in notes.splitlines()
        ) or "<w:p/>"
        document_xml = (
            "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
            "<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'>"
            f"<w:body>{paragraphs}<w:sectPr/></w:body></w:document>"
        )
        file_path = export_root / f"{safe_name}.docx"
        with zipfile.ZipFile(file_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "[Content_Types].xml",
                "<?xml version='1.0' encoding='UTF-8'?>"
                "<Types xmlns='http://schemas.openxmlformats.org/package/2006/content-types'>"
                "<Default Extension='rels' ContentType='application/vnd.openxmlformats-package.relationships+xml'/>"
                "<Default Extension='xml' ContentType='application/xml'/>"
                "<Override PartName='/word/document.xml' ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml'/>"
                "</Types>",
            )
            archive.writestr(
                "_rels/.rels",
                "<?xml version='1.0' encoding='UTF-8'?>"
                "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'>"
                "<Relationship Id='rId1' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument' Target='word/document.xml'/>"
                "</Relationships>",
            )
            archive.writestr("word/document.xml", document_xml)
        return str(file_path)

    if normalized_type in {"one", "onenote"}:
        file_path = export_root / f"{safe_name}.one"
        with zipfile.ZipFile(file_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("notes.txt", notes, compress_type=zipfile.ZIP_DEFLATED)
            archive.writestr("README.txt", "This is a OneNote-compatible package stub created by Airflow.")
        return str(file_path)

    raise ValueError(f"Unsupported export type: {file_type}")


def export_notes(**context):
    dag_run = context.get("dag_run")
    final_text = ""
    file_type = "txt"

    if dag_run is not None:
        conf = getattr(dag_run, "conf", {}) or {}
        final_text = conf.get("final_text") or conf.get("reviewed_text") or conf.get("notes") or ""
        file_type = conf.get("export_format") or conf.get("file_type") or file_type

    if not final_text:
        ti = context.get("ti")
        if ti is not None:
            previous = ti.xcom_pull(task_ids="revise_notes")
            if isinstance(previous, dict):
                final_text = previous.get("revised_text") or ""

    if not final_text:
        raise ValueError("No notes available to export")

    output_path = create_export_file(final_text, file_type=file_type)
    return {
        "output_path": output_path,
        "file_type": file_type,
        "notes": final_text,
    }


def export_file(**context):
    """Create one export after the HITL task supplies the requested format."""
    ti = context.get("ti")
    notes = ""
    file_type = "txt"

    choice = ti.xcom_pull(task_ids="choose_export_format") if ti is not None else None
    if isinstance(choice, dict):
        chosen_options = choice.get("chosen_options") or []
        if chosen_options:
            file_type = str(chosen_options[0]).lower()

    if ti is not None:
        previous = ti.xcom_pull(task_ids="revise_notes")
        if isinstance(previous, dict):
            notes = previous.get("revised_text") or ""

        if not notes:
            ocr_result = ti.xcom_pull(task_ids="ocr_image")
            if isinstance(ocr_result, dict):
                notes = ocr_result.get("transcribed_text") or ""

    if not notes:
        raise ValueError("No revised notes available for export")

    output_path = create_export_file(notes, file_type=file_type)
    return {"output_path": output_path, "file_type": file_type, "notes": notes}