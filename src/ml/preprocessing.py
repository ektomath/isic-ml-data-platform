"""Preprocessing-config loading and resolution into runtime transforms.

Pure Python (plus torchvision), no Spark dependency -- unit-tested locally like
`data_platform.files`/`validate`/`labels`/`sampling` (see tests/test_ml_preprocessing.py).

A preprocessing config (`config/preprocessing/<name>.yaml`) is never applied to stored bytes --
Gold shards hold raw, undecoded image bytes only (docs/decisions/004-stream-archives-no-blob-storage.md).
This module is the one place a config dict turns into an actual `torchvision.transforms.Compose`,
applied at training/inference load time only (docs/decisions/001-preprocessing-at-runtime.md).
"""

from __future__ import annotations

from pathlib import Path

from torchvision import transforms

from data_platform.dataset_layout import load_yaml_config

PREPROCESSING_REQUIRED_FIELDS = (
    "preprocessing_version",
    "image_size",
    "normalization",
    "resize_policy",
    "crop_policy",
    "augmentation",
    "random_seed",
)

_RESIZE_POLICIES = {
    "shorter_side_to_256": 256,
}


def require_fields(config: dict, required_fields: tuple[str, ...], context: str) -> None:
    """Raise ValueError, listing every missing field at once, if config lacks any of
    required_fields -- same "fail loud, all at once" posture as assert_controlled_vocabularies.
    Shared by every config loader/validator in this package (ml.preprocessing, ml.training_run).
    """
    missing_fields = [field for field in required_fields if field not in config]
    if missing_fields:
        raise ValueError(f"{context} is missing required field(s): {missing_fields}")


def load_preprocessing_config(config_root: str | Path, preprocessing_version: str) -> dict:
    """Load config/preprocessing/<preprocessing_version>.yaml and validate it carries every
    field the config/preprocessing/<name>.yaml contract requires (docs/data_contract.md)."""
    path = Path(config_root) / "preprocessing" / f"{preprocessing_version}.yaml"
    config = load_yaml_config(path)
    require_fields(config, PREPROCESSING_REQUIRED_FIELDS, f"Preprocessing config {path}")
    return config


def build_transforms(preprocessing_config: dict, split: str) -> transforms.Compose:
    """Resolve a loaded preprocessing config into a torchvision.transforms.Compose for one
    split. `split == "train"` applies preprocessing_config["augmentation"]["train"]; every other
    split (validation/test/inference) applies preprocessing_config["augmentation"]["eval"] (this
    config's eval augmentation is empty, i.e. resize+crop+normalize only, no augmentation).

    image_size comes from the config, not the crop_policy string, so a config's crop_policy
    (e.g. "random_crop_224") and image_size (224) must agree -- both are recorded so a reader
    doesn't have to parse the policy string to know the actual pixel size; this function trusts
    image_size as the source of truth for the crop dimension.
    """
    require_fields(preprocessing_config, PREPROCESSING_REQUIRED_FIELDS, "preprocessing_config")

    image_size = preprocessing_config["image_size"]
    resize_policy = preprocessing_config["resize_policy"]
    if resize_policy not in _RESIZE_POLICIES:
        raise ValueError(f"Unknown resize_policy {resize_policy!r}; known policies: {sorted(_RESIZE_POLICIES)}")
    resize_target = _RESIZE_POLICIES[resize_policy]

    normalization = preprocessing_config["normalization"]
    mean, std = normalization["mean"], normalization["std"]

    steps: list = [transforms.Resize(resize_target)]

    if split == "train":
        steps.append(transforms.RandomCrop(image_size))
        augmentation = preprocessing_config["augmentation"].get("train") or {}
        if augmentation.get("random_horizontal_flip"):
            steps.append(transforms.RandomHorizontalFlip())
        rotation_degrees = augmentation.get("random_rotation_degrees")
        if rotation_degrees:
            steps.append(transforms.RandomRotation(rotation_degrees))
    else:
        steps.append(transforms.CenterCrop(image_size))

    steps.append(transforms.ToTensor())
    steps.append(transforms.Normalize(mean=mean, std=std))

    return transforms.Compose(steps)
