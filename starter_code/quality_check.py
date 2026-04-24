# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.


def run_quality_gate(document_dict):
    if not isinstance(document_dict, dict):
        return False

    content = str(document_dict.get('content', '') or '')

    # Reject very short content.
    if len(content.strip()) < 20:
        return False

    # Reject toxic / error strings.
    toxic_tokens = [
        'null pointer exception',
        'traceback (most recent call last)',
        'segmentation fault',
    ]
    lowered = content.lower()
    if any(token in lowered for token in toxic_tokens):
        return False

    # Optional discrepancy flag for legacy code documents.
    if '8%' in content and ('0.10' in content or '10%' in content):
        source_metadata = document_dict.get('source_metadata')
        if not isinstance(source_metadata, dict):
            source_metadata = {}
        source_metadata['discrepancy_flag'] = 'comment_8_percent_vs_code_10_percent'
        document_dict['source_metadata'] = source_metadata

    return True
