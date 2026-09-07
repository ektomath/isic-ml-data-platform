from ml.metrics import compute_classification_metrics


def test_compute_classification_metrics_perfect_predictions():
    label_values = ["benign", "malignant", "indeterminate"]
    y_true = [0, 0, 1, 1, 2, 2]
    y_pred = [0, 0, 1, 1, 2, 2]

    metrics = compute_classification_metrics(y_true, y_pred, label_values)

    assert metrics["balanced_accuracy"] == 1.0
    assert metrics["per_class_recall"] == {"benign": 1.0, "malignant": 1.0, "indeterminate": 1.0}
    assert metrics["confusion_matrix"] == [[2, 0, 0], [0, 2, 0], [0, 0, 2]]
    assert "benign" in metrics["classification_report"]


def test_compute_classification_metrics_with_mistakes():
    label_values = ["benign", "malignant"]
    y_true = [0, 0, 1, 1]
    y_pred = [0, 1, 1, 1]  # one benign misclassified as malignant, malignant recall stays perfect

    metrics = compute_classification_metrics(y_true, y_pred, label_values)

    assert metrics["per_class_recall"] == {"benign": 0.5, "malignant": 1.0}
    assert metrics["balanced_accuracy"] == 0.75
    assert metrics["confusion_matrix"] == [[1, 1], [0, 2]]


def test_compute_classification_metrics_handles_class_with_no_true_samples():
    # zero_division=0 -- a label never present in y_true shouldn't raise, its recall is 0.0
    label_values = ["benign", "malignant", "indeterminate"]
    y_true = [0, 0, 1, 1]
    y_pred = [0, 1, 1, 1]

    metrics = compute_classification_metrics(y_true, y_pred, label_values)

    assert metrics["per_class_recall"]["indeterminate"] == 0.0
