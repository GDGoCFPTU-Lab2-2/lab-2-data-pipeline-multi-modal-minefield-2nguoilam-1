import re
from datetime import datetime

import pandas as pd

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.


def process_sales_csv(file_path):
    # --- FILE READING (Handled for students) ---
    df = pd.read_csv(file_path)
    # ------------------------------------------

    # Remove duplicate IDs while keeping the first observed row.
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')

    documents = []
    for _, row in df.iterrows():
        row_id = row.get('id')
        currency = _safe_str(row.get('currency'))
        price_raw = _safe_str(row.get('price'))
        parsed_price = _parse_price(price_raw, currency)
        normalized_date = _normalize_date(row.get('date_of_sale'))

        content = (
            f"Sale record for {_safe_str(row.get('product_name'))} "
            f"in {_safe_str(row.get('category'))}. "
            f"Price raw={price_raw}, currency={currency}, sale_date={normalized_date}."
        )

        documents.append(
            {
                'document_id': f"csv-{int(row_id)}" if pd.notna(row_id) else 'csv-unknown',
                'content': content,
                'source_type': 'CSV',
                'author': 'Sales System',
                'timestamp': normalized_date,
                'source_metadata': {
                    'id': int(row_id) if pd.notna(row_id) else None,
                    'product_name': _safe_str(row.get('product_name')),
                    'category': _safe_str(row.get('category')),
                    'price_raw': price_raw,
                    'price_value': parsed_price,
                    'currency': currency,
                    'date_of_sale': normalized_date,
                    'seller_id': _safe_str(row.get('seller_id')),
                    'stock_quantity': _safe_int(row.get('stock_quantity')),
                    'original_file': 'sales_records.csv',
                },
            }
        )

    return documents


def _safe_str(value):
    if pd.isna(value):
        return ''
    return str(value).strip()


def _safe_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_price(raw_price, currency):
    value = (raw_price or '').strip()
    lowered = value.lower()

    if lowered in {'', 'n/a', 'null', 'none'} or 'liên hệ' in lowered:
        return None

    word_to_number = {
        'five dollars': 5.0,
    }
    if lowered in word_to_number:
        return word_to_number[lowered]

    # Remove formatting chars while keeping numeric symbols.
    cleaned = re.sub(r'[^\d\.-]', '', value)
    if cleaned in {'', '-', '.', '-.'}:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_date(value):
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    known_formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y/%m/%d',
        '%d %b %Y',
        '%B %dth %Y',
        '%B %dst %Y',
        '%B %dnd %Y',
        '%B %drd %Y',
    ]

    for fmt in known_formats:
        try:
            return datetime.strptime(text, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', text, flags=re.IGNORECASE)
    parsed = pd.to_datetime(cleaned, errors='coerce', dayfirst=True)
    if pd.isna(parsed):
        parsed = pd.to_datetime(cleaned, errors='coerce')

    if pd.isna(parsed):
        return None

    return parsed.strftime('%Y-%m-%d')
