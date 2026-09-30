"""Turns a preprocessing config (config/preprocessing/<name>.yaml) into torchvision transforms.
Applied when images are loaded for training or inference, never to stored bytes: shards hold the
original encoded images (ADR 001, ADR 004).
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
    """Build the transform for one split. "train" uses the config's train augmentation; every other
    split uses its eval augmentation. The crop size comes from `image_size`, which must agree with
    the size in crop_policy (e.g. random_crop_224).
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
