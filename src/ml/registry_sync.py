"""Reads MLflow runs into plain row dicts for gold.training_run_registry.

Pure Python plus the `mlflow` client -- no Spark session needed, unit-tested locally against a
real local-file MLflow tracking store (see tests/test_ml_registry_sync.py). The Spark write
itself (`MERGE INTO gold.training_run_registry`) lives in
`data_platform.spark_io.write_training_run_registry_rows` instead -- kept apart the same way
`data_platform.shard_export` (no Spark) is kept apart from `data_platform.spark_io` (needs
Spark), so `spark_io.py`'s own imports never gain a hard `mlflow` dependency just for this.

On-demand utility, not scheduled -- called explicitly from notebooks/40_train_baseline_classifier.ipynb
after a training run, same "exists, callable, not wired into a default flow" posture as
`data_platform.spark_io.remove_expired_exports`.
"""

from __future__ import annotations

REQUIRED_RUN_TAGS = ("training_run_name", "dataset_version", "preprocessing_version")


def list_training_run_rows(mlflow_client, experiment_name: str) -> list[dict]:
    """Search every run in experiment_name and return one row dict per run that carries this
    project's own training_run_name/dataset_version/preprocessing_version tags (set by
    src.ml.train.run_training). A run missing any of those tags is skipped, not raised on --
    this registry is provenance for runs this project's own training code produced, not a
    general MLflow audit tool, so a stray/unrelated run in the same experiment is simply not
    this project's concern.

    Returns [{mlflow_run_id, training_run_name, dataset_version, preprocessing_version,
    mlflow_experiment_id}, ...] -- ready for data_platform.spark_io.write_training_run_registry_rows.
    """
    experiment = mlflow_client.get_experiment_by_name(experiment_name)
    if experiment is None:
        return []

    rows = []
    for run in mlflow_client.search_runs(experiment_ids=[experiment.experiment_id]):
        tags = run.data.tags
        if not all(tag in tags for tag in REQUIRED_RUN_TAGS):
            continue
        rows.append(
            {
                "mlflow_run_id": run.info.run_id,
                "training_run_name": tags["training_run_name"],
                "dataset_version": tags["dataset_version"],
                "preprocessing_version": tags["preprocessing_version"],
                "mlflow_experiment_id": experiment.experiment_id,
            }
        )
    return rows
