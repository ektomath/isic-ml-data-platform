from mlflow import MlflowClient

from ml.registry_sync import list_training_run_rows


def _real_local_client(tmp_path, monkeypatch) -> MlflowClient:
    # A real local file:// MLflow tracking store -- fully offline, no Databricks credentials
    # needed, same "real thing, not a mock" convention as every other test in this project.
    # mlflow-skinny==3.15.2 deprecated file:// by default ("maintenance mode", pushing users
    # toward a sqlite:///... backend) -- but sqlite:/// itself needs sqlalchemy, which is part
    # of full mlflow, not mlflow-skinny (see docs/decisions/011-baseline-training-framework-and-registry-sync.md
    # for why this project deliberately stays on mlflow-skinny). MLFLOW_ALLOW_FILE_STORE opts
    # back into file:// without pulling in that extra dependency -- test-only, doesn't affect
    # production code, which always uses tracking_uri="databricks"
    # (ml.mlflow_utils.configure_mlflow_tracking), never a local file store.
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    return MlflowClient(tracking_uri=f"file:{tmp_path / 'mlruns'}")


def test_list_training_run_rows_returns_only_tagged_runs(tmp_path, monkeypatch):
    client = _real_local_client(tmp_path, monkeypatch)
    experiment_id = client.create_experiment("baseline_training")

    tagged_run = client.create_run(
        experiment_id,
        tags={
            "training_run_name": "sample-v1-resnet18",
            "dataset_version": "sample-v1",
            "preprocessing_version": "sample-v1",
        },
    )
    client.create_run(experiment_id)  # a run with no tags -- should be skipped, not raised on

    rows = list_training_run_rows(client, "baseline_training")

    assert len(rows) == 1
    assert rows[0] == {
        "mlflow_run_id": tagged_run.info.run_id,
        "training_run_name": "sample-v1-resnet18",
        "dataset_version": "sample-v1",
        "preprocessing_version": "sample-v1",
        "mlflow_experiment_id": experiment_id,
    }


def test_list_training_run_rows_returns_empty_list_for_unknown_experiment(tmp_path, monkeypatch):
    client = _real_local_client(tmp_path, monkeypatch)

    rows = list_training_run_rows(client, "does-not-exist")

    assert rows == []


def test_list_training_run_rows_skips_run_missing_some_tags(tmp_path, monkeypatch):
    client = _real_local_client(tmp_path, monkeypatch)
    experiment_id = client.create_experiment("baseline_training")
    client.create_run(experiment_id, tags={"training_run_name": "partial-run"})  # missing the other two tags

    rows = list_training_run_rows(client, "baseline_training")

    assert rows == []
