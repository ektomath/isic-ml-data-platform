"""Canonical label vocabularies and shared label-normalization helpers for Silver. Pure Python
(tests/test_labels.py). Only label columns meant to be comparable across datasets get a vocabulary
(ADR 002); `specific_diagnosis` is free text.
"""

from __future__ import annotations

# Every dataset's label function must map onto this set; spark_io.assert_controlled_vocabularies
# fails the Silver run otherwise.
MALIGNANCY_VALUES = ("benign", "malignant", "indeterminate")

# The DDL for the two shared label columns of silver.image_inventory, added by each dataset's
# 05_setup_tables_and_folders notebook (ADR 002).
SILVER_LABEL_COLUMN_DDL = {
    "malignancy": "STRING COMMENT 'benign/malignant/indeterminate, from diagnosis_1'",
    "specific_diagnosis": "STRING COMMENT 'deepest available diagnosis value'",
}


def normalize_malignancy_prefix(diagnosis_1: str | None) -> str | None:
    """Map a raw top-level diagnosis onto MALIGNANCY_VALUES by prefix, ignoring case and surrounding
    whitespace. Fits the ISIC Archive convention that isic_2019 and milk10k share; a dataset that
    encodes malignancy differently needs its own mapping.
    """
    if diagnosis_1 is None:
        return None
    value = diagnosis_1.strip().lower()
    if not value:
        return None
    for prefix in MALIGNANCY_VALUES:
        if value.startswith(prefix):
            return prefix
    return None


def deepest_available(*values: str | None) -> str | None:
    """Return the last non-empty value in a most-general-to-most-specific hierarchy.

    Dataset-agnostic: works for any diagnosis hierarchy regardless of depth
    (ISIC 2019 has 5 levels, MILK10k has 4) — a dataset's normalize_labels
    passes its own diagnosis_1..N columns in order.
    """
    for value in reversed(values):
        if value is not None and value.strip():
            return value.strip()
    return None


def normalize_diagnosis_labels(*diagnosis_levels: str | None) -> dict[str, str]:
    """Map an ISIC-Archive-style diagnosis_1..N hierarchy to the Silver label columns.

    `diagnosis_levels` are the dataset's diagnosis columns, most general first. `malignancy` comes
    from diagnosis_1 and `specific_diagnosis` is the deepest level present. A label that can't be
    resolved is left out of the result; an empty dict means the row should be rejected.

    Used by isic_2019 and milk10k, which differ only in how many levels they have.
    """
    labels: dict[str, str] = {}

    malignancy = normalize_malignancy_prefix(diagnosis_levels[0] if diagnosis_levels else None)
    if malignancy is not None:
        labels["malignancy"] = malignancy

    specific_diagnosis = deepest_available(*diagnosis_levels)
    if specific_diagnosis is not None:
        labels["specific_diagnosis"] = specific_diagnosis

    return labels
