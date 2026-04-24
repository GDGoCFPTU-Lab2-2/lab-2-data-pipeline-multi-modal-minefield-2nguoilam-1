import json
import os
import time

from pydantic import ValidationError

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'raw_data')


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.


def main():
    start_time = time.time()
    final_kb = []

    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, 'lecture_notes.pdf')
    trans_path = os.path.join(RAW_DATA_DIR, 'demo_transcript.txt')
    html_path = os.path.join(RAW_DATA_DIR, 'product_catalog.html')
    csv_path = os.path.join(RAW_DATA_DIR, 'sales_records.csv')
    code_path = os.path.join(RAW_DATA_DIR, 'legacy_pipeline.py')

    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), 'processed_knowledge_base.json')
    # ----------------------------------------------

    sources = [
        ('PDF', extract_pdf_data, pdf_path),
        ('Transcript', clean_transcript, trans_path),
        ('HTML', parse_html_catalog, html_path),
        ('CSV', process_sales_csv, csv_path),
        ('LegacyCode', extract_logic_from_code, code_path),
    ]

    for source_name, processor, path in sources:
        if not os.path.exists(path):
            print(f'[WARN] Missing input for {source_name}: {path}')
            continue

        try:
            raw_output = processor(path)
        except Exception as exc:
            print(f'[ERROR] {source_name} processor failed: {exc}')
            continue

        for document_dict in _as_document_list(raw_output):
            if not run_quality_gate(document_dict):
                print(f"[SKIP] Quality gate rejected: {document_dict.get('document_id', 'unknown-id')}")
                continue

            try:
                validated = UnifiedDocument(**document_dict)
            except ValidationError as exc:
                print(f"[SKIP] Schema validation failed for {document_dict.get('document_id', 'unknown-id')}: {exc}")
                continue

            final_kb.append(_serialize_model(validated))

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_kb, f, ensure_ascii=False, indent=2)

    end_time = time.time()
    print(f'Pipeline finished in {end_time - start_time:.2f} seconds.')
    print(f'Total valid documents stored: {len(final_kb)}')
    print(f'Output written to: {output_path}')


def _as_document_list(raw_output):
    if raw_output is None:
        return []
    if isinstance(raw_output, list):
        return [doc for doc in raw_output if isinstance(doc, dict)]
    if isinstance(raw_output, dict):
        return [raw_output]
    return []


def _serialize_model(model_obj):
    # Pydantic v2
    if hasattr(model_obj, 'model_dump'):
        return model_obj.model_dump(mode='json')

    # Pydantic v1 fallback
    return json.loads(model_obj.json())


if __name__ == '__main__':
    main()
