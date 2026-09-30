"""Metadata preprocessing-config loading and resolution into a runtime feature vector.

Pure Python, no Spark dependency -- unit-tested locally like `ml.preprocessing`
(see tests/test_ml_metadata_preprocessing.py).

A metadata preprocessing config (`config/metadata_preprocessing/<name>.yaml`) is the versioned,
reviewable answer to "which Bronze source-metadata fields does this model actually use, and how
are they encoded" -- exactly the kind of decision (drop irrelevant fields, bucket/impute the
rest) that must never happen as an ad hoc local edit to a downloaded CSV, or a trained model's
reported metrics stop being traceable back to a reproducible input (see
docs/decisions/009-pin-metadata-feature-config-per-training-run.md). Mirrors `ml.preprocessing`'s
role for images: this module is the one place a config dict turns into an actual runtime
transform, applied at training/inference load time only, never baked into a stored file.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from data_platform.dataset_layout import load_yaml_config
from ml.preprocessing import require_fields

METADATA_PREPROCESSING_REQUIRED_FIELDS = ("metadata_preprocessing_version", "fields")

# Each field's kind fixes which keys its own spec must carry -- "numeric" needs a sentinel for a
# null/unparseable source value, "categorical" needs both its fixed vocabulary (order matters --
# it fixes each category's one-hot slot, not derived from a scan) and a bucket for a null or
# out-of-vocabulary value, so a config can never silently produce a different-shaped vector
# depending on what happened to be present in the data it was built against.
_REQUIRED_FIELD_SPEC_KEYS_BY_KIND = {
    "numeric": ("name", "kind", "missing_value"),
    "categorical": ("name", "kind", "categories", "unknown_category"),
}


# Optional per-field keys, present only for a field that needs cross-dataset reconciliation
# (docs/decisions/009-pin-metadata-feature-config-per-training-run.md's Decision section
# on reconciling datasets): a release combining more than one dataset_key whose raw column names and/or raw
# values for the "same" conceptual field don't already agree. Both are keyed by dataset_key,
# and both are no-ops for a dataset_key not listed in them -- a single-dataset config (or a
# field that happens to line up across every dataset in the release) never needs either.
_OPTIONAL_RECONCILIATION_KEYS = ("source_columns", "value_map")


def _validate_field_specs(fields: list[dict]) -> None:
    if not fields:
        raise ValueError("metadata preprocessing config 'fields' must list at least one field")

    seen_names = set()
    for field in fields:
        name = field.get("name")
        kind = field.get("kind")
        if kind not in _REQUIRED_FIELD_SPEC_KEYS_BY_KIND:
            raise ValueError(
                f"metadata preprocessing field {name!r} has unknown kind {kind!r}; expected one of "
                f"{sorted(_REQUIRED_FIELD_SPEC_KEYS_BY_KIND)}"
            )
        missing_keys = [key for key in _REQUIRED_FIELD_SPEC_KEYS_BY_KIND[kind] if key not in field]
        if missing_keys:
            raise ValueError(
                f"metadata preprocessing field {name!r} (kind={kind!r}) is missing required key(s): {missing_keys}"
            )
        if name in seen_names:
            raise ValueError(f"metadata preprocessing field {name!r} is listed more than once")
        seen_names.add(name)

        for optional_key in _OPTIONAL_RECONCILIATION_KEYS:
            if optional_key in field and not isinstance(field[optional_key], dict):
                raise ValueError(
                    f"metadata preprocessing field {name!r}'s {optional_key!r} must be a mapping of "
                    f"dataset_key -> ..., got {type(field[optional_key]).__name__}"
                )


def load_metadata_preprocessing_config(config_root: str | Path, metadata_preprocessing_version: str) -> dict:
    """Load config/metadata_preprocessing/<metadata_preprocessing_version>.yaml and validate its
    top-level shape plus every field spec in it
    (docs/data_contract.md's config/metadata_preprocessing/<name>.yaml contract)."""
    path = Path(config_root) / "metadata_preprocessing" / f"{metadata_preprocessing_version}.yaml"
    config = load_yaml_config(path)
    require_fields(config, METADATA_PREPROCESSING_REQUIRED_FIELDS, f"Metadata preprocessing config {path}")
    _validate_field_specs(config["fields"])
    return config


def _encode_numeric(raw_value, missing_value: float) -> float:
    if raw_value is None or raw_value == "":
        return float(missing_value)
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return float(missing_value)


def _encode_categorical(raw_value, category_to_index: dict[str, int], vocabulary_size: int) -> list[float]:
    """One-hot via a precomputed category -> index lookup (built once per field in
    build_metadata_transform, not re-scanned on every call -- this runs once per sample) plus one
    trailing slot for whatever didn't match. A null value and an out-of-vocabulary value are
    deliberately not distinguished, both land in that trailing slot, since either way this
    config's vocabulary doesn't already account for it."""
    index = category_to_index.get(raw_value, vocabulary_size - 1)
    one_hot = [0.0] * vocabulary_size
    one_hot[index] = 1.0
    return one_hot


def _resolve_raw_value(field: dict, raw_row: dict, dataset_key) -> object:
    """Look up field's value on raw_row, reconciling cross-dataset column-name/value
    differences first (docs/decisions/009-pin-metadata-feature-config-per-training-run.md's
    Decision section). `source_columns.get(dataset_key, field["name"])` falls back to the
    canonical name when this dataset_key isn't listed (including when raw_row carries no
    "dataset_key" at all, e.g. a single-dataset config/export, matching dataset_key=None to
    nothing) -- so a field with no reconciliation needed behaves exactly as it did before this
    existed. `value_map` is applied after that column lookup, mapping this dataset_key's raw
    value onto the config's canonical vocabulary; a value not present in the map (or no map for
    this dataset_key) passes through unchanged.
    """
    source_column = field.get("source_columns", {}).get(dataset_key, field["name"])
    raw_value = raw_row.get(source_column)

    value_map = field.get("value_map", {}).get(dataset_key)
    if value_map is not None and raw_value in value_map:
        raw_value = value_map[raw_value]

    return raw_value


def build_metadata_transform(metadata_preprocessing_config: dict) -> Callable[[dict], list[float]]:
    """Resolve a loaded metadata preprocessing config into a callable: raw metadata row (a plain
    dict, e.g. one row of an `export_source_metadata_csv` CSV) -> a fixed-length feature vector,
    fields concatenated in the config's own listed order. A field absent from
    metadata_preprocessing_config["fields"] is implicitly dropped -- this function reads only the
    fields the config actually lists, never every column the raw row happens to carry.

    Deterministic in both content and length given the config alone -- the same guarantee
    `ml.preprocessing.build_transforms` gives for images, so a trained (image, metadata) model's
    reported metrics are reproducible from `metadata_preprocessing_version` alone, not from
    whatever ad hoc column selection happened to be applied locally that day.

    raw_row's optional "dataset_key" entry (present on every row `export_source_metadata_csv`
    writes) drives per-field `source_columns`/`value_map` reconciliation when a release combines
    more than one dataset_key whose raw columns/values for the "same" conceptual field don't
    already agree -- see docs/decisions/009-pin-metadata-feature-config-per-training-run.md.
    Absent entirely for a single-dataset config, exactly as before that reconciliation existed.
    """
    require_fields(
        metadata_preprocessing_config, METADATA_PREPROCESSING_REQUIRED_FIELDS, "metadata_preprocessing_config"
    )
    fields = metadata_preprocessing_config["fields"]
    _validate_field_specs(fields)

    # Built once per training run (this function is called once, not per sample) so the
    # per-sample closure below does an O(1) dict lookup instead of re-scanning each field's
    # categories list on every call.
    category_index_by_field_name = {
        field["name"]: {category: index for index, category in enumerate(field["categories"])}
        for field in fields
        if field["kind"] == "categorical"
    }

    def transform(raw_row: dict) -> list[float]:
        dataset_key = raw_row.get("dataset_key")
        vector: list[float] = []
        for field in fields:
            raw_value = _resolve_raw_value(field, raw_row, dataset_key)
            if field["kind"] == "numeric":
                vector.append(_encode_numeric(raw_value, field["missing_value"]))
            else:
                category_to_index = category_index_by_field_name[field["name"]]
                vector.extend(_encode_categorical(raw_value, category_to_index, len(field["categories"]) + 1))
        return vector

    return transform


def metadata_feature_dim(metadata_preprocessing_config: dict) -> int:
    """The fixed output length build_metadata_transform's callable always returns for this
    config -- 1 per numeric field, len(categories) + 1 per categorical field. A multimodal
    model's metadata input layer is sized from this, not inferred from a batch at runtime."""
    require_fields(
        metadata_preprocessing_config, METADATA_PREPROCESSING_REQUIRED_FIELDS, "metadata_preprocessing_config"
    )
    return sum(
        1 if field["kind"] == "numeric" else len(field["categories"]) + 1
        for field in metadata_preprocessing_config["fields"]
    )
