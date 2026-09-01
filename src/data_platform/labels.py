"""Dataset-agnostic canonical label vocabularies for Silver label columns.

Pure Python, no Spark dependency, unit-tested locally (see tests/test_labels.py).
Only label axes meant to be cross-dataset comparable belong here — see
docs/decisions/003-silver-label-columns-not-map.md. `specific_diagnosis` is
deliberately absent: open-ended free text by design, not canonicalized.
"""

from __future__ import annotations

# Enforced by data_platform.spark_io.apply_label_normalization's
# controlled_vocabularies param — a normalize_fn emitting anything else fails
# the Silver run rather than silently splitting this column across datasets.
# Fixed going forward: every dataset's normalize_labels must map onto this set,
# not the other way around.
MALIGNANCY_VALUES = ("benign", "malignant", "indeterminate")
