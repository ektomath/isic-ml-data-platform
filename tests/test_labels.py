from data_platform.labels import (
    MALIGNANCY_VALUES,
    SILVER_LABEL_COLUMN_DDL,
    deepest_available,
    normalize_diagnosis_labels,
    normalize_malignancy_prefix,
)


def test_malignancy_values_is_the_expected_canonical_set():
    assert MALIGNANCY_VALUES == ("benign", "malignant", "indeterminate")


def test_silver_label_column_ddl_has_the_two_shared_label_columns():
    assert set(SILVER_LABEL_COLUMN_DDL) == {"malignancy", "specific_diagnosis"}


def test_normalize_malignancy_prefix_is_case_and_whitespace_insensitive():
    assert normalize_malignancy_prefix("  MALIGNANT  ") == "malignant"
    assert normalize_malignancy_prefix("benign") == "benign"


def test_normalize_malignancy_prefix_returns_none_for_unrecognized_or_missing_value():
    assert normalize_malignancy_prefix("Unknown") is None
    assert normalize_malignancy_prefix(None) is None
    assert normalize_malignancy_prefix("") is None


def test_deepest_available_prefers_the_last_non_empty_value():
    assert deepest_available("Benign", "Benign melanocytic proliferations", "Nevus") == "Nevus"
    assert deepest_available("Malignant", None, None) == "Malignant"
    assert deepest_available("Benign", "", None) == "Benign"


def test_deepest_available_returns_none_when_all_missing():
    assert deepest_available(None, None, None) is None
    assert deepest_available("", "", "") is None


def test_normalize_diagnosis_labels_is_arity_agnostic():
    # 3 levels (isic_2019's shape)
    assert normalize_diagnosis_labels("Benign", "Benign melanocytic proliferations", "Nevus") == {
        "malignancy": "benign",
        "specific_diagnosis": "Nevus",
    }
    # 4 levels (milk10k's shape) — the real metadata.csv sample row (ISIC_0051817)
    assert normalize_diagnosis_labels(
        "Malignant",
        "Malignant epidermal proliferations",
        "Squamous cell carcinoma, Invasive",
        None,
    ) == {
        "malignancy": "malignant",
        "specific_diagnosis": "Squamous cell carcinoma, Invasive",
    }


def test_normalize_diagnosis_labels_all_missing_returns_empty_dict():
    assert normalize_diagnosis_labels(None, None, None) == {}
    assert normalize_diagnosis_labels() == {}


def test_normalize_diagnosis_labels_unrecognized_malignancy_still_resolves_specific_diagnosis():
    assert normalize_diagnosis_labels("Unknown", "Some category", "Some specific type") == {
        "specific_diagnosis": "Some specific type",
    }
