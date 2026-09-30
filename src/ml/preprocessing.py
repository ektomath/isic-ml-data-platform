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
    "resize_shorter_side",
    "augmentation",
    "random_seed",
)

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
    """Build the transform for one split: resize the shorter side to `resize_shorter_side`, crop to
    `image_size` (a random crop plus the config's train augmentation for "train", a center crop for
    every other split), then convert to a normalized tensor.
    """
    require_fields(preprocessing_config, PREPROCESSING_REQUIRED_FIELDS, "preprocessing_config")

    image_size = preprocessing_config["image_size"]

    normalization = preprocessing_config["normalization"]
    mean, std = normalization["mean"], normalization["std"]

    steps: list = [transforms.Resize(preprocessing_config["resize_shorter_side"])]

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
