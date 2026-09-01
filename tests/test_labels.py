from data_platform.labels import MALIGNANCY_VALUES


def test_malignancy_values_is_the_expected_canonical_set():
    assert MALIGNANCY_VALUES == ("benign", "malignant", "indeterminate")
