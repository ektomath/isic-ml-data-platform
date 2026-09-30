import zipfile

import mlflow
from mlflow import MlflowClient

from archive_fixtures import checksum as _checksum
from archive_fixtures import jpeg_bytes as _jpeg_bytes
from data_platform.shard_export import write_gold_shards_for_splits
from ml.train import run_training, shard_digest

# Same Windows urlparse-drive-letter workaround as tests/test_shard_export.py and
# tests/test_ml_dataset.py -- every test here chdirs into tmp_path and passes relative shard
# dirs so MDSWriter/StreamingDataset never see an absolute C:\... path.

_TRAINING_RUN_YAML = """
training_run_name: smoke-run
dataset_version: smoke-v1
preprocessing_version: smoke-v1
label_values: [benign, malignant]
architecture: resnet18
pretrained: false
batch_size: 2
num_epochs: 1
learning_rate: 0.001
optimizer: adam
"""

_PREPROCESSING_YAML = """
preprocessing_version: smoke-v1
image_size: 64
normalization:
  mean: [0.5, 0.5, 0.5]
  std: [0.5, 0.5, 0.5]
resize_policy: shorter_side_to_256
crop_policy:
  train: random_crop_224
  eval: center_crop_224
augmentation:
  train:
    random_horizontal_flip: true
  eval: []
random_seed: 7
"""


def _build_config_root(tmp_path):
    config_root = tmp_path / "config"
    (config_root / "gold" / "training_runs").mkdir(parents=True)
    (config_root / "preprocessing").mkdir(parents=True)
    (config_root / "gold" / "training_runs" / "smoke-run.yaml").write_text(_TRAINING_RUN_YAML)
    (config_root / "preprocessing" / "smoke-v1.yaml").write_text(_PREPROCESSING_YAML)
    config_root.joinpath("storage.yaml").write_text(
        "landing_root: /unused\nstorage_root: /unused\nmanifest_table: gold.manifest_rows\n"
    )
    return config_root


def _build_shards(tmp_path):
    # Every split needs genuinely distinct images (distinct archive members) -- real Gold
    # splits are leakage-group-disjoint, and write_gold_shards_for_splits keys its candidate
    # resolution by archive_member_path across all splits at once, so reusing the same member
    # in two splits would silently drop one of them (only the last-seen split for that member
    # survives), not duplicate it.
    benign, malignant = _jpeg_bytes((255, 0, 0), size=96), _jpeg_bytes((0, 255, 0), size=96)
    members = {
        "train_a.jpg": benign,
        "train_b.jpg": benign,
        "train_c.jpg": malignant,
        "train_d.jpg": malignant,
        "val_a.jpg": benign,
        "val_b.jpg": malignant,
        "test_a.jpg": benign,
        "test_b.jpg": malignant,
    }
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)

    def _row(image_id, member, label, group):
        return {
            "image_id": image_id,
            "dataset_key": "isic_2019",
            "bronze_uri": f"archive:dbfs:/landing/archive.zip#{member}",
            "source_checksum": _checksum(members[member]),
            "label": label,
            "group_id": group,
        }

    rows_by_split = {
        "train": [
            _row("train_a", "train_a.jpg", "benign", "g1"),
            _row("train_b", "train_b.jpg", "benign", "g2"),
            _row("train_c", "train_c.jpg", "malignant", "g3"),
            _row("train_d", "train_d.jpg", "malignant", "g4"),
        ],
        "validation": [
            _row("val_a", "val_a.jpg", "benign", "g5"),
            _row("val_b", "val_b.jpg", "malignant", "g6"),
        ],
        "test": [
            _row("test_a", "test_a.jpg", "benign", "g7"),
            _row("test_b", "test_b.jpg", "malignant", "g8"),
        ],
    }

    write_gold_shards_for_splits(
        rows_by_split,
        staged_path_by_archive_uri={"dbfs:/landing/archive.zip": archive_path},
        shard_dir_by_split={"train": "shards/train", "validation": "shards/validation", "test": "shards/test"},
        size_limit_bytes=1 << 26,
    )
    return "shards"


def test_run_training_end_to_end_wiring(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")

    config_root = _build_config_root(tmp_path)
    shards_root = _build_shards(tmp_path)
    mlflow_tracking_uri = f"file:{tmp_path / 'mlruns'}"

    run_id = run_training(
        "smoke-run",
        config_root=config_root,
        shards_root=shards_root,
        mlflow_experiment="smoke_test",
        mlflow_tracking_uri=mlflow_tracking_uri,
        device="cpu",
        num_workers=0,
    )

    assert run_id

    client = MlflowClient(tracking_uri=mlflow_tracking_uri)
    run = client.get_run(run_id)

    assert run.data.tags["training_run_name"] == "smoke-run"
    assert run.data.tags["dataset_version"] == "smoke-v1"
    assert run.data.tags["preprocessing_version"] == "smoke-v1"
    assert run.data.params["architecture"] == "resnet18"
    assert run.data.params["pretrained"] == "False"
    assert "test_balanced_accuracy" in run.data.metrics
    assert "test_recall_benign" in run.data.metrics
    assert "test_recall_malignant" in run.data.metrics
    assert "train_loss" in run.data.metrics
    assert "validation_loss" in run.data.metrics
    inputs_by_name = {dataset_input.dataset.name: dataset_input.dataset for dataset_input in run.inputs.dataset_inputs}
    assert set(inputs_by_name) == {"gold.manifest_rows@smoke-v1", "shards@smoke-v1"}
    assert inputs_by_name["shards@smoke-v1"].digest == shard_digest(shards_root)
    assert mlflow.models.get_model_info(f"runs:/{run_id}/model").signature is not None
    logged_recipe = mlflow.artifacts.load_dict(f"{run.info.artifact_uri}/preprocessing_config.json")
    assert logged_recipe["preprocessing_version"] == "smoke-v1"
    assert logged_recipe["image_size"] == 64


def test_run_training_registers_model_when_registered_model_name_is_set(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")

    config_root = _build_config_root(tmp_path)
    shards_root = _build_shards(tmp_path)
    mlflow_tracking_uri = f"file:{tmp_path / 'mlruns'}"

    run_id = run_training(
        "smoke-run",
        config_root=config_root,
        shards_root=shards_root,
        mlflow_experiment="smoke_test",
        mlflow_tracking_uri=mlflow_tracking_uri,
        registered_model_name="smoke_registered_model",
        registry_uri=mlflow_tracking_uri,  # same local file store as tracking, for this test only
        device="cpu",
        num_workers=0,
    )

    client = MlflowClient(tracking_uri=mlflow_tracking_uri, registry_uri=mlflow_tracking_uri)
    versions = client.search_model_versions("name='smoke_registered_model'")

    assert len(versions) == 1
    assert versions[0].run_id == run_id
    assert versions[0].tags["dataset_version"] == "smoke-v1"
    assert versions[0].tags["preprocessing_version"] == "smoke-v1"
    assert versions[0].tags["git_commit"]
