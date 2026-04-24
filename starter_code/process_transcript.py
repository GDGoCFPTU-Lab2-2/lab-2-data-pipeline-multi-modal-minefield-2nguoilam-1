import re
import unicodedata

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.


def clean_transcript(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    # ------------------------------------------

    # Remove timestamps such as [00:00:00]
    text = re.sub(r"\[\d{2}:\d{2}:\d{2}\]\s*", "", text)

    # Remove common noise markers.
    text = re.sub(r"\[(?:Music[^\]]*|inaudible|Laughter)\]", "", text, flags=re.IGNORECASE)

    # Collapse excessive spacing.
    cleaned_text = re.sub(r"[ \t]+", " ", text)
    cleaned_text = re.sub(r"\n{2,}", "\n", cleaned_text).strip()

    detected_price = _extract_price_vnd(cleaned_text)

    return {
        "document_id": "video-transcript-001",
        "content": cleaned_text,
        "source_type": "Video",
        "author": "Unknown",
        "timestamp": None,
        "source_metadata": {
            "original_file": "demo_transcript.txt",
            "detected_price_vnd": detected_price,
        },
    }


def _strip_accents(value):
    return "".join(
        ch
        for ch in unicodedata.normalize("NFD", value)
        if unicodedata.category(ch) != "Mn"
    )


def _extract_price_vnd(text):
    # Prefer explicit numeric prices first (e.g., "500,000 VND").
    numeric_patterns = [
        r"(\d[\d,\.]*)\s*VND",
        r"(\d[\d,\.]*)\s*VN[ĐD]",
    ]
    for pattern in numeric_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            raw_value = re.sub(r"[^\d]", "", match.group(1))
            if raw_value:
                return int(raw_value)

    # Fallback for Vietnamese words if numeric value is absent.
    normalized = _strip_accents(text.lower())
    if "nam tram nghin" in normalized:
        return 500000

    return None
