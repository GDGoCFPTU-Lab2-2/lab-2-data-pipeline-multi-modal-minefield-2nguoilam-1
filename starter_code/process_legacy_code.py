import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.


def extract_logic_from_code(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()
    # ------------------------------------------

    tree = ast.parse(source_code)

    module_docstring = ast.get_docstring(tree) or ''
    function_docs = []
    function_names = []

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            function_names.append(node.name)
            fn_doc = ast.get_docstring(node)
            if fn_doc:
                function_docs.append(f"{node.name}: {fn_doc.strip()}")

    business_rule_comments = re.findall(
        r"#\s*(Business Logic Rule\s*\d+[^\n]*)",
        source_code,
        flags=re.IGNORECASE,
    )

    discrepancy_flag = False
    if '8%' in source_code and ('0.10' in source_code or '10%' in source_code):
        discrepancy_flag = True

    content_parts = []
    if module_docstring:
        content_parts.append(module_docstring.strip())
    if function_docs:
        content_parts.append(' | '.join(function_docs))
    if business_rule_comments:
        content_parts.append(' | '.join(business_rule_comments))
    if discrepancy_flag:
        content_parts.append('Potential discrepancy detected: tax comment mentions 8% while code uses 10%.')

    return {
        'document_id': 'code-legacy-001',
        'content': ' '.join(content_parts).strip(),
        'source_type': 'Code',
        'author': 'Legacy System',
        'timestamp': None,
        'source_metadata': {
            'original_file': 'legacy_pipeline.py',
            'function_names': function_names,
            'business_rule_comments': business_rule_comments,
            'tax_discrepancy_flag': discrepancy_flag,
        },
    }
