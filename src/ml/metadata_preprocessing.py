"""Turns a metadata preprocessing config (config/metadata_preprocessing/<name>.yaml) into a feature
transform for tabular metadata. The config is the versioned record of which metadata fields a model
uses and how they're encoded, so that choice is never an untracked edit to a downloaded CSV (ADR
009). Like ml.preprocessing for images, it's applied at load time, never baked into stored files.
Pure Python (tests/test_ml_metadata_preprocessing.py).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from data_platform.dataset_layout import load_yaml_config
from ml.preprocessing import require_fields

METADATA_PREPROCESSING_REQUIRED_FIELDS = ("metadata_preprocessing_version", "fields")

# The keys each field kind needs. "numeric" needs a fill value for a missing or unparseable
# value; "categorical" needs its vocabulary (whose order fixes the one-hot positions) and a
# bucket for missing or unknown values. So the vector's shape never depends on the data.
_REQUIRED_FIELD_SPEC_KEYS_BY_KIND = {
    "numeric": ("name", "kind", "missing_value"),
    "categorical": ("name", "kind", "categories", "unknown_category"),
}


# Optional per-field keys for releases that combine datasets whose column names or values
# differ for the same field (ADR 009). Both are keyed by dataset_key; a dataset not listed
# uses the field as it is.
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
    """Read `field` from raw_row, reconciling dataset differences first (ADR 009). The column name
    comes from `source_columns[dataset_key]`, falling back to the field's own name; then
    `value_map[dataset_key]`, if any, maps the raw value onto the config's vocabulary. Unmapped
    values pass through unchanged.
    """
    source_column = field.get("source_columns", {}).get(dataset_key, field["name"])
    raw_value = raw_row.get(source_column)

    value_map = field.get("value_map", {}).get(dataset_key)
    if value_map is not None and raw_value in value_map:
        raw_value = value_map[raw_value]

    return raw_value


def build_metadata_transform(metadata_preprocessing_config: dict) -> Callable[[dict], list[float]]:
    """Build a callable that turns a raw metadata row (a dict, e.g. one row of
    export_source_metadata_csv's CSV) into a fixed-length feature vector. Only the fields the config
    lists are used, in the config's order, so the output depends on the config alone.

    The row's optional "dataset_key" selects each field's per-dataset `source_columns`/`value_map`,
    for releases that combine datasets (ADR 009).
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
