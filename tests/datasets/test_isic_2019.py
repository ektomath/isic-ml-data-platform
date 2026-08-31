from data_platform.datasets.isic_2019 import normalize_labels


def test_normalize_labels_resolves_both_keys_when_hierarchy_is_complete():
    assert normalize_labels("Benign", "Benign melanocytic proliferations", "Nevus") == {
        "malignancy": "benign",
        "specific_diagnosis": "Nevus",
    }


def test_normalize_labels_is_case_and_whitespace_insensitive_for_malignancy():
    assert normalize_labels("  MALIGNANT  ", None, None) == {
        "malignancy": "malignant",
        "specific_diagnosis": "MALIGNANT",
    }


def test_normalize_labels_specific_diagnosis_falls_back_to_diagnosis_1_when_no_deeper_level():
    result = normalize_labels("Malignant", None, None)
    assert result["malignancy"] == "malignant"
    assert result["specific_diagnosis"] == "Malignant"


def test_normalize_labels_unrecognized_malignancy_value_still_resolves_specific_diagnosis():
    assert normalize_labels("Unknown", "Some category", "Some specific type") == {
        "specific_diagnosis": "Some specific type",
    }


def test_normalize_labels_all_missing_returns_empty_dict():
    assert normalize_labels(None, None, None) == {}
    assert normalize_labels("", "", "") == {}


def test_normalize_labels_ignores_blank_deeper_levels():
    assert normalize_labels("Benign", "", None) == {
        "malignancy": "benign",
        "specific_diagnosis": "Benign",
    }
