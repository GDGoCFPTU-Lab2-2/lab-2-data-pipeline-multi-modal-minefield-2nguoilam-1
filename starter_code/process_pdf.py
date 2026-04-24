import json
import os
import re
import time

import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_API_ENDPOINT = os.getenv('GEMINI_API_ENDPOINT')
GEMINI_MODEL = os.getenv('GEMINI_MODEL', 'gemini-2.5-flash')

configure_kwargs = {}
if GEMINI_API_KEY:
    configure_kwargs['api_key'] = GEMINI_API_KEY
if GEMINI_API_ENDPOINT:
    configure_kwargs['client_options'] = {
        'api_endpoint': GEMINI_API_ENDPOINT.rstrip('/'),
    }

if configure_kwargs:
    genai.configure(**configure_kwargs)


def extract_pdf_data(file_path):
    if not os.path.exists(file_path):
        print(f'Error: File not found at {file_path}')
        return None

    if not GEMINI_API_KEY:
        print('GEMINI_API_KEY is missing. Skipping PDF extraction.')
        return None

    model = genai.GenerativeModel(GEMINI_MODEL)

    prompt = """
Analyze this document and output exactly one JSON object with these fields:
{
  "document_id": "pdf-doc-001",
  "content": "A concise summary of the document (3-5 sentences).",
  "source_type": "PDF",
  "author": "Extracted author or Unknown",
  "timestamp": null,
  "source_metadata": {
    "original_file": "lecture_notes.pdf",
    "title": "Extracted title if available",
    "main_topics": ["topic 1", "topic 2"],
    "tables_detected": true
  }
}
Return JSON only, no markdown code fences.
""".strip()

    pdf_file = _upload_with_backoff(file_path)
    if pdf_file is None:
        return None

    response_text = _generate_with_backoff(model, [pdf_file, prompt])
    if response_text is None:
        return None

    extracted_data = _parse_json_response(response_text)
    if extracted_data is None:
        return {
            'document_id': 'pdf-doc-001',
            'content': response_text.strip(),
            'source_type': 'PDF',
            'author': 'Unknown',
            'timestamp': None,
            'source_metadata': {
                'original_file': os.path.basename(file_path),
                'parse_warning': 'model_output_not_valid_json',
            },
        }

    extracted_data.setdefault('document_id', 'pdf-doc-001')
    extracted_data.setdefault('content', '')
    extracted_data.setdefault('source_type', 'PDF')
    extracted_data.setdefault('author', 'Unknown')
    extracted_data.setdefault('timestamp', None)

    source_metadata = extracted_data.get('source_metadata')
    if not isinstance(source_metadata, dict):
        source_metadata = {}
    source_metadata.setdefault('original_file', os.path.basename(file_path))
    extracted_data['source_metadata'] = source_metadata

    return extracted_data


def _upload_with_backoff(file_path, retries=5, base_sleep=1.0):
    for attempt in range(retries):
        try:
            print(f'Uploading {file_path} to Gemini...')
            return genai.upload_file(path=file_path)
        except Exception as exc:
            if attempt == retries - 1 or not _is_retryable_error(exc):
                print(f'Failed to upload file to Gemini: {exc}')
                return None
            delay = base_sleep * (2 ** attempt)
            print(f'Upload retry in {delay:.1f}s due to rate limit...')
            time.sleep(delay)
    return None


def _generate_with_backoff(model, payload, retries=5, base_sleep=1.0):
    for attempt in range(retries):
        try:
            print('Generating content from PDF using Gemini...')
            response = model.generate_content(payload)
            return getattr(response, 'text', '')
        except Exception as exc:
            if attempt == retries - 1 or not _is_retryable_error(exc):
                print(f'Failed to generate content from PDF: {exc}')
                return None
            delay = base_sleep * (2 ** attempt)
            print(f'Generate retry in {delay:.1f}s due to rate limit...')
            time.sleep(delay)
    return None


def _is_retryable_error(exc):
    message = str(exc).lower()
    retry_tokens = ['429', 'resource_exhausted', 'rate', 'too many requests']
    return any(token in message for token in retry_tokens)


def _parse_json_response(content_text):
    if not content_text:
        return None

    text = content_text.strip()
    if text.startswith('```json'):
        text = text[7:]
    if text.startswith('```'):
        text = text[3:]
    if text.endswith('```'):
        text = text[:-3]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Fallback: extract the first JSON object from mixed text.
    match = re.search(r'\{[\s\S]*\}', text)
    if not match:
        return None

    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
