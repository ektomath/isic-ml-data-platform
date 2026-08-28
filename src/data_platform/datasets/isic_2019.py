"""Label normalization for the ISIC 2019 dataset."""

from __future__ import annotations

_MALIGNANCY_VALUES = ("benign", "malignant", "indeterminate")


def _normalize_malignancy(diagnosis_1: str | None) -> str | None:
    """Map diagnosis_1 to a canonical benign/malignant/indeterminate value."""
    if diagnosis_1 is None:
        return None
    value = diagnosis_1.strip().lower()
    if not value:
        return None
    for prefix in _MALIGNANCY_VALUES:
        if value.startswith(prefix):
            return prefix
    return None


def _deepest_available(*diagnosis_levels: str | None) -> str | None:
    """Return the deepest (last) non-empty diagnosis level, preferring more specific levels."""
    for value in reversed(diagnosis_levels):
        if value is not None and value.strip():
            return value.strip()
    return None


def normalize_labels(
    diagnosis_1: str | None,
    diagnosis_2: str | None,
    diagnosis_3: str | None,
) -> dict[str, str]:
    """Map ISIC 2019 source diagnosis fields to canonical Silver label columns.

    Returns a dict with up to two keys, `malignancy` and `specific_diagnosis`,
    omitting whichever cannot be resolved. An empty dict means neither
    resolved and the caller should reject the row.
    """
    labels: dict[str, str] = {}

    malignancy = _normalize_malignancy(diagnosis_1)
    if malignancy is not None:
        labels["malignancy"] = malignancy

    specific_diagnosis = _deepest_available(diagnosis_1, diagnosis_2, diagnosis_3)
    if specific_diagnosis is not None:
        labels["specific_diagnosis"] = specific_diagnosis

    return labels
