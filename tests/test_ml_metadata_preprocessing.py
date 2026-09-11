from ml.metadata_preprocessing import build_metadata_transform, load_metadata_preprocessing_config, metadata_feature_dim

_VALID_CONFIG = {
    "metadata_preprocessing_version": "fixture-v1",
    "fields": [
        {"name": "age_approx", "kind": "numeric", "missing_value": -1.0},
        {"name": "sex", "kind": "categorical", "categories": ["male", "female"], "unknown_category": "unknown"},
    ],
}


def test_build_metadata_transform_encodes_numeric_and_categorical_fields():
    transform = build_metadata_transform(_VALID_CONFIG)

    vector = transform({"age_approx": "45", "sex": "female"})

    assert vector == [45.0, 0.0, 1.0, 0.0]  # age, [male, female, unknown] one-hot


def test_build_metadata_transform_uses_missing_value_for_null_or_unparseable_numeric():
    transform = build_metadata_transform(_VALID_CONFIG)

    assert transform({"age_approx": None, "sex": "male"})[0] == -1.0
    assert transform({"age_approx": "", "sex": "male"})[0] == -1.0
    assert transform({"age_approx": "not_a_number", "sex": "male"})[0] == -1.0


def test_build_metadata_transform_buckets_null_or_out_of_vocabulary_categorical_as_unknown():
    transform = build_metadata_transform(_VALID_CONFIG)

    assert transform({"age_approx": "10", "sex": None})[1:] == [0.0, 0.0, 1.0]
    assert transform({"age_approx": "10", "sex": "nonbinary"})[1:] == [0.0, 0.0, 1.0]


def test_build_metadata_transform_ignores_fields_not_listed_in_config():
    transform = build_metadata_transform(_VALID_CONFIG)

    vector = transform({"age_approx": "10", "sex": "male", "anatom_site_1": "head/neck", "extra": "ignored"})

    assert len(vector) == metadata_feature_dim(_VALID_CONFIG)


def test_metadata_feature_dim_counts_one_per_numeric_and_vocab_plus_one_per_categorical():
    assert metadata_feature_dim(_VALID_CONFIG) == 1 + (2 + 1)


def test_build_metadata_transform_raises_on_unknown_field_kind():
    config = {
        "metadata_preprocessing_version": "fixture-v1",
        "fields": [{"name": "age_approx", "kind": "not_a_real_kind"}],
    }

    try:
        build_metadata_transform(config)
    except ValueError as error:
        assert "not_a_real_kind" in str(error)
    else:
        raise AssertionError("Expected unknown field kind to raise")


def test_build_metadata_transform_raises_on_missing_kind_specific_key():
    config = {"metadata_preprocessing_version": "fixture-v1", "fields": [{"name": "sex", "kind": "categorical"}]}

    try:
        build_metadata_transform(config)
    except ValueError as error:
        assert "categories" in str(error)
        assert "unknown_category" in str(error)
    else:
        raise AssertionError("Expected missing categorical keys to raise")


def test_build_metadata_transform_raises_on_duplicate_field_name():
    config = {
        "metadata_preprocessing_version": "fixture-v1",
        "fields": [
            {"name": "sex", "kind": "categorical", "categories": ["male"], "unknown_category": "unknown"},
            {"name": "sex", "kind": "categorical", "categories": ["female"], "unknown_category": "unknown"},
        ],
    }

    try:
        build_metadata_transform(config)
    except ValueError as error:
        assert "sex" in str(error)
    else:
        raise AssertionError("Expected duplicate field name to raise")


def test_load_metadata_preprocessing_config_round_trips_a_real_file(tmp_path):
    metadata_dir = tmp_path / "metadata_preprocessing"
    metadata_dir.mkdir()
    (metadata_dir / "fixture-v1.yaml").write_text(
        "metadata_preprocessing_version: fixture-v1\n"
        "fields:\n"
        "  - name: age_approx\n"
        "    kind: numeric\n"
        "    missing_value: -1.0\n"
    )

    config = load_metadata_preprocessing_config(tmp_path, "fixture-v1")

    assert config["metadata_preprocessing_version"] == "fixture-v1"
    assert config["fields"][0]["name"] == "age_approx"


def test_build_metadata_transform_resolves_source_columns_per_dataset_key():
    config = {
        "metadata_preprocessing_version": "fixture-v1",
        "fields": [
            {
                "name": "sex",
                "kind": "categorical",
                "categories": ["male", "female"],
                "unknown_category": "unknown",
                "source_columns": {"milk10k": "patient_sex"},
            }
        ],
    }
    transform = build_metadata_transform(config)

    # isic_2019 (not listed in source_columns) falls back to the canonical column name "sex".
    assert transform({"dataset_key": "isic_2019", "sex": "male"}) == [1.0, 0.0, 0.0]
    # milk10k's raw column is actually "patient_sex" -- "sex" itself is absent/irrelevant here.
    assert transform({"dataset_key": "milk10k", "patient_sex": "female", "sex": "IGNORED"}) == [0.0, 1.0, 0.0]


def test_build_metadata_transform_resolves_value_map_per_dataset_key():
    config = {
        "metadata_preprocessing_version": "fixture-v1",
        "fields": [
            {
                "name": "sex",
                "kind": "categorical",
                "categories": ["male", "female"],
                "unknown_category": "unknown",
                "value_map": {"milk10k": {"M": "male", "F": "female"}},
            }
        ],
    }
    transform = build_metadata_transform(config)

    assert transform({"dataset_key": "milk10k", "sex": "M"}) == [1.0, 0.0, 0.0]
    assert transform({"dataset_key": "milk10k", "sex": "F"}) == [0.0, 1.0, 0.0]
    # isic_2019 has no value_map entry, so its raw value passes through unchanged.
    assert transform({"dataset_key": "isic_2019", "sex": "male"}) == [1.0, 0.0, 0.0]
    # A milk10k value not covered by its value_map passes through unchanged -- lands in unknown.
    assert transform({"dataset_key": "milk10k", "sex": "not_mapped"}) == [0.0, 0.0, 1.0]


def test_build_metadata_transform_without_dataset_key_still_uses_canonical_column():
    # A raw_row with no "dataset_key" at all (a single-dataset config/export) must behave
    # exactly as it did before source_columns/value_map existed.
    config = {
        "metadata_preprocessing_version": "fixture-v1",
        "fields": [
            {
                "name": "sex",
                "kind": "categorical",
                "categories": ["male", "female"],
                "unknown_category": "unknown",
                "source_columns": {"milk10k": "patient_sex"},
            }
        ],
    }
    transform = build_metadata_transform(config)

    assert transform({"sex": "male"}) == [1.0, 0.0, 0.0]


def test_build_metadata_transform_raises_on_non_mapping_source_columns():
    config = {
        "metadata_preprocessing_version": "fixture-v1",
        "fields": [
            {
                "name": "sex",
                "kind": "categorical",
                "categories": ["male"],
                "unknown_category": "unknown",
                "source_columns": "not_a_mapping",
            }
        ],
    }

    try:
        build_metadata_transform(config)
    except ValueError as error:
        assert "source_columns" in str(error)
    else:
        raise AssertionError("Expected non-mapping source_columns to raise")


def test_load_metadata_preprocessing_config_raises_on_missing_top_level_field(tmp_path):
    metadata_dir = tmp_path / "metadata_preprocessing"
    metadata_dir.mkdir()
    (metadata_dir / "fixture-v1.yaml").write_text("metadata_preprocessing_version: fixture-v1\n")

    try:
        load_metadata_preprocessing_config(tmp_path, "fixture-v1")
    except ValueError as error:
        assert "fields" in str(error)
    else:
        raise AssertionError("Expected missing 'fields' to raise")
