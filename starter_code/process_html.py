import re
from bs4 import BeautifulSoup

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.


def parse_html_catalog(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    # ------------------------------------------

    table = soup.find('table', {'id': 'main-catalog'})
    if table is None:
        return []

    rows = table.find_all('tr')
    documents = []

    for idx, row in enumerate(rows[1:], start=1):
        cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
        if len(cells) < 6:
            continue

        product_code, product_name, category, price_raw, stock_raw, rating = cells[:6]

        documents.append(
            {
                'document_id': f'html-{product_code or idx}',
                'content': (
                    f"Product {product_name} in category {category}. "
                    f"Listed price: {price_raw}. Stock: {stock_raw}. Rating: {rating}."
                ),
                'source_type': 'HTML',
                'author': 'VinShop Catalog',
                'timestamp': None,
                'source_metadata': {
                    'product_code': product_code,
                    'product_name': product_name,
                    'category': category,
                    'price_raw': price_raw,
                    'price_value': _parse_price_value(price_raw),
                    'stock_quantity': _safe_int(stock_raw),
                    'rating_raw': rating,
                    'original_file': 'product_catalog.html',
                },
            }
        )

    return documents


def _parse_price_value(value):
    lowered = (value or '').strip().lower()
    if lowered in {'n/a', 'null', 'none', 'lien he'} or 'liên hệ' in lowered:
        return None

    digits = re.sub(r'[^\d\.-]', '', value)
    if digits in {'', '-', '.', '-.'}:
        return None

    try:
        return float(digits)
    except ValueError:
        return None


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
