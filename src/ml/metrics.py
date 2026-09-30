"""Classification metrics for baseline model evaluation.

Pure Python (plus scikit-learn), no Spark/torch dependency -- unit-tested locally
(see tests/test_ml_metrics.py) against small hand-verified integer arrays.
"""

from __future__ import annotations

from collections.abc import Sequence

from sklearn.metrics import balanced_accuracy_score, classification_report, confusion_matrix, recall_score


def compute_classification_metrics(y_true: Sequence[int], y_pred: Sequence[int], label_values: list[str]) -> dict:
    """Compute the baseline classifier's acceptance metrics (matching the original project
    plan's ML-001 task): balanced accuracy, per-class recall, a confusion matrix, and a text
    classification report. `y_true`/`y_pred` are class indices (see
    ml.dataset.label_to_index_map); `label_values` gives their names in index order.
    """
    labels = list(range(len(label_values)))

    per_class_recall = recall_score(y_true, y_pred, labels=labels, average=None, zero_division=0)

    return {
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "per_class_recall": {
            label: float(recall) for label, recall in zip(label_values, per_class_recall, strict=True)
        },
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=labels).tolist(),
        "classification_report": classification_report(
            y_true, y_pred, labels=labels, target_names=label_values, zero_division=0
        ),
    }
