import base64
import json
import os
import mimetypes
from urllib.request import Request, urlopen
from pathlib import Path


def load_env_file():
    current_path = Path(__file__).resolve()

    for parent_directory in current_path.parents:
        env_path = parent_directory / ".env"
        if not env_path.exists():
            continue

        with env_path.open() as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

        return


load_env_file()


def build_image_data_url(image_result):
    mime_type, _ = mimetypes.guess_type(image_result)
    if mime_type is None:
        mime_type = "application/octet-stream"

    with open(image_result, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

    return f"data:{mime_type};base64,{encoded_image}"

def ocr_image(image_result, user_prompt_result, **context):
    print("RAW HITL XCOM:", repr(user_prompt_result))
    note_context = user_prompt_result["params_input"]["note_context"]
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")

    image_data_url = build_image_data_url(image_result)

    context_line = f" User-provided context about these notes: {note_context}." if note_context else ""

    print(f"Pinging OpenAI API with image: {image_result}")
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an OCR assistant. You are analyzing a handwritten note image."
                    + context_line +
                    " Here are your tasks:"
                    "1. Extract all text from the image "
                    "2. Assess how confident you are in the extraction "
                    "3. Identify any words, phrases, or sections you are uncertain about "
                    "4. Recommend whether a human should review the result\n\n"
                    "Return ONLY valid JSON (no markdown, no code fences) in this exact format:\n"
                    '{"transcribed_text": "<full extracted text>", '
                    '"confidence": <integer 0-100>, '
                    '"needs_human_review": <true|false>, '
                    '"uncertain_sections": ["<section 1>", "<section 2>"], '
                    '"reason": "<short explanation>"}\n\n'
                    "Scoring guidance:\n"
                    "95-100: very clear handwriting, almost no ambiguity\n"
                    "85-94: mostly clear with minor uncertain words\n"
                    "70-84: several uncertain words or sections\n"
                    "below 70: substantial uncertainty or illegible content\n\n"
                    "Set needs_human_review to true when:\n"
                    "- confidence is below 85\n"
                    "- multiple words are unclear\n"
                    "- portions of the note cannot be confidently interpreted"
                )
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Please extract the text from this image."},
                    {"type": "image_url", "image_url": {"url": image_data_url}}
                ]
            }
        ]
    }

    request = Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        },
        method="POST",
    )

    with urlopen(request) as response:
        response_body = json.loads(response.read().decode("utf-8"))

    content = response_body["choices"][0]["message"]["content"]

    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
        cleaned = cleaned.strip()

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError:
        print("Could not parse model response as JSON:")
        print(content)
        raise

    print("OCR result:")
    print(json.dumps(result, indent=2))
    print(f"Confidence: {result.get('confidence')}  |  Needs review: {result.get('needs_human_review')}")

    print("Transcribed text:")
    print(result.get("transcribed_text", ""))

    return result