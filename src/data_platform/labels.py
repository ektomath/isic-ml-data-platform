"""Dataset-agnostic canonical label vocabularies and shared label-normalization
helpers for Silver label columns.

Pure Python, no Spark dependency, unit-tested locally (see tests/test_labels.py).
Only label axes meant to be cross-dataset comparable belong here — see
docs/decisions/003-silver-label-columns-not-map.md. `specific_diagnosis` has no
canonical vocabulary here and isn't meant to: open-ended free text by design.
"""

from __future__ import annotations

# Enforced by data_platform.spark_io.assert_controlled_vocabularies via
# apply_label_normalization's controlled_vocabularies param — a normalize_fn
# emitting anything else fails the Silver run rather than silently splitting
# this column across datasets. Fixed going forward: every dataset's
# normalize_labels must map onto this set, not the other way around.
MALIGNANCY_VALUES = ("benign", "malignant", "indeterminate")

# The `silver.image_inventory` DDL fragment for the two shared label columns,
# used by each dataset's 05_setup_tables_and_folders.ipynb guarded
# `ALTER TABLE ADD COLUMNS` (see docs/decisions/003-silver-label-columns-not-map.md).
# Both isic_2019 and milk10k populate exactly these two columns with the same
# meaning, so this is one shared constant rather than a dict re-typed verbatim
# per dataset notebook. A dataset needing a genuinely different label axis
# (e.g. a future `severity` column) defines its own DDL fragment inline in its
# own setup notebook instead of adding to this one.
SILVER_LABEL_COLUMN_DDL = {
    "malignancy": "STRING COMMENT 'benign/malignant/indeterminate, from diagnosis_1'",
    "specific_diagnosis": "STRING COMMENT 'deepest available diagnosis value'",
}


def normalize_malignancy_prefix(diagnosis_1: str | None) -> str | None:
    """Map a raw top-level diagnosis value onto MALIGNANCY_VALUES via a
    case/whitespace-insensitive prefix match.

    Shared by ISIC-Archive-family datasets (isic_2019, milk10k) that use the
    same benign/malignant/indeterminate top-level diagnosis convention —
    confirmed identical logic across both once a real second dataset existed
    to check it against, not assumed upfront. A dataset whose raw malignancy
    signal isn't shaped this way (a different word, a numeric code) should
    write its own mapping in its own data_platform.datasets.<dataset> module
    instead of force-fitting this helper.
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
    """Map an ISIC-Archive-style diagnosis_1..N hierarchy to canonical Silver label columns.

    `diagnosis_levels` is a dataset's diagnosis_1..N columns, most general first.
    `malignancy` comes from diagnosis_1 (normalize_malignancy_prefix); `specific_diagnosis`
    is the deepest available level (deepest_available). Returns a dict with up to two
    keys, omitting whichever cannot be resolved — an empty dict means neither resolved
    and the caller should reject the row.

    Shared by isic_2019 and milk10k: once both datasets' normalize_labels bodies turned
    out to be identical except for how many diagnosis levels they pass, that arity
    difference is configuration, not logic, so it moved here rather than staying
    duplicated. A dataset whose diagnosis representation doesn't match this shape (a
    different word, a numeric code, a differently-ordered hierarchy) should write its
    own data_platform.datasets.<dataset>.normalize_labels instead of force-fitting this.
    """
    labels: dict[str, str] = {}

    malignancy = normalize_malignancy_prefix(diagnosis_levels[0] if diagnosis_levels else None)
    if malignancy is not None:
        labels["malignancy"] = malignancy

    specific_diagnosis = deepest_available(*diagnosis_levels)
    if specific_diagnosis is not None:
        labels["specific_diagnosis"] = specific_diagnosis

    return labels
