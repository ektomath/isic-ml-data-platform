from ml.training_run import load_training_run_config, resolve_training_run

_PREPROCESSING_YAML = """
preprocessing_version: fixture-v1
image_size: 224
normalization:
  mean: [0.485, 0.456, 0.406]
  std: [0.229, 0.224, 0.225]
resize_shorter_side: 256
augmentation:
  train:
    random_horizontal_flip: true
  eval: []
random_seed: 7
"""

_TRAINING_RUN_YAML = """
training_run_name: fixture-run
dataset_version: fixture-v1
preprocessing_version: fixture-v1
label_values: [benign, malignant]
architecture: resnet18
pretrained: false
batch_size: 4
num_epochs: 1
learning_rate: 0.001
optimizer: adam
"""


def _build_config_root(tmp_path, training_run_yaml=_TRAINING_RUN_YAML, preprocessing_yaml=_PREPROCESSING_YAML):
    (tmp_path / "gold" / "training_runs").mkdir(parents=True)
    (tmp_path / "preprocessing").mkdir(parents=True)
    (tmp_path / "gold" / "training_runs" / "fixture-run.yaml").write_text(training_run_yaml)
    (tmp_path / "preprocessing" / "fixture-v1.yaml").write_text(preprocessing_yaml)
    return tmp_path


def test_load_training_run_config_reads_yaml(tmp_path):
    config_root = _build_config_root(tmp_path)

    config = load_training_run_config(config_root, "fixture-run")

    assert config["dataset_version"] == "fixture-v1"
    assert config["preprocessing_version"] == "fixture-v1"
    assert config["label_values"] == ["benign", "malignant"]


def test_load_training_run_config_raises_on_missing_field(tmp_path):
    incomplete_yaml = "training_run_name: fixture-run\ndataset_version: fixture-v1\n"
    config_root = _build_config_root(tmp_path, training_run_yaml=incomplete_yaml)

    try:
        load_training_run_config(config_root, "fixture-run")
    except ValueError as error:
        assert "preprocessing_version" in str(error)
        assert "label_values" in str(error)
    else:
        raise AssertionError("Expected missing required fields to raise")


def test_resolve_training_run_loads_both_configs(tmp_path):
    config_root = _build_config_root(tmp_path)

    spec = resolve_training_run(config_root, "fixture-run")

    assert spec.training_run_name == "fixture-run"
    assert spec.dataset_version == "fixture-v1"
    assert spec.preprocessing_version == "fixture-v1"
    assert spec.label_values == ["benign", "malignant"]
    assert spec.architecture == "resnet18"
    assert spec.pretrained is False
    assert spec.batch_size == 4
    assert spec.num_epochs == 1
    assert spec.learning_rate == 0.001
    assert spec.optimizer == "adam"
    assert spec.mlflow_experiment is None
    assert spec.registered_model_name is None
    assert spec.preprocessing_config["random_seed"] == 7


def test_resolve_training_run_reads_registered_model_name_when_set(tmp_path):
    training_run_yaml = _TRAINING_RUN_YAML + "registered_model_name: fixture_catalog.gold.fixture_classifier\n"
    config_root = _build_config_root(tmp_path, training_run_yaml=training_run_yaml)

    spec = resolve_training_run(config_root, "fixture-run")

    assert spec.registered_model_name == "fixture_catalog.gold.fixture_classifier"


def test_resolve_training_run_raises_on_malformed_preprocessing_config(tmp_path):
    incomplete_preprocessing_yaml = "preprocessing_version: fixture-v1\nimage_size: 224\n"
    config_root = _build_config_root(tmp_path, preprocessing_yaml=incomplete_preprocessing_yaml)

    try:
        resolve_training_run(config_root, "fixture-run")
    except ValueError as error:
        assert "normalization" in str(error)
    else:
        raise AssertionError("Expected malformed preprocessing config to raise")
