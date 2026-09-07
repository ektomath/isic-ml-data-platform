import torch
from PIL import Image

from ml.preprocessing import build_transforms, load_preprocessing_config

_VALID_CONFIG = {
    "preprocessing_version": "fixture-v1",
    "image_size": 224,
    "normalization": {"mean": [0.485, 0.456, 0.406], "std": [0.229, 0.224, 0.225]},
    "resize_policy": "shorter_side_to_256",
    "crop_policy": {"train": "random_crop_224", "eval": "center_crop_224"},
    "augmentation": {"train": {"random_horizontal_flip": True, "random_rotation_degrees": 15}, "eval": []},
    "random_seed": 7,
}


def _varied_image(width: int = 300, height: int = 300) -> Image.Image:
    image = Image.new("RGB", (width, height))
    pixels = image.load()
    for x in range(width):
        for y in range(height):
            pixels[x, y] = (x % 256, y % 256, (x + y) % 256)
    return image


def test_build_transforms_output_shape_and_dtype():
    transform = build_transforms(_VALID_CONFIG, "eval")

    output = transform(_varied_image())

    assert isinstance(output, torch.Tensor)
    assert output.shape == (3, 224, 224)
    assert output.dtype == torch.float32


def test_build_transforms_eval_split_is_deterministic():
    transform = build_transforms(_VALID_CONFIG, "eval")
    image = _varied_image()

    first = transform(image)
    second = transform(image)

    assert torch.equal(first, second)


def test_build_transforms_train_split_applies_augmentation():
    transform = build_transforms(_VALID_CONFIG, "train")
    image = _varied_image()

    outputs = [transform(image) for _ in range(8)]

    # Random crop position / flip / rotation should not produce identical output every time.
    assert not all(torch.equal(outputs[0], other) for other in outputs[1:])


def test_build_transforms_raises_on_missing_fields():
    incomplete_config = {"preprocessing_version": "fixture-v1", "image_size": 224}

    try:
        build_transforms(incomplete_config, "eval")
    except ValueError as error:
        assert "normalization" in str(error)
        assert "random_seed" in str(error)
    else:
        raise AssertionError("Expected missing required fields to raise")


def test_build_transforms_raises_on_unknown_resize_policy():
    config = {**_VALID_CONFIG, "resize_policy": "not_a_real_policy"}

    try:
        build_transforms(config, "eval")
    except ValueError as error:
        assert "not_a_real_policy" in str(error)
    else:
        raise AssertionError("Expected unknown resize_policy to raise")


def test_load_preprocessing_config_round_trips_a_real_file(tmp_path):
    preprocessing_dir = tmp_path / "preprocessing"
    preprocessing_dir.mkdir()
    (preprocessing_dir / "fixture-v1.yaml").write_text(
        "preprocessing_version: fixture-v1\n"
        "image_size: 224\n"
        "normalization:\n  mean: [0.485, 0.456, 0.406]\n  std: [0.229, 0.224, 0.225]\n"
        "resize_policy: shorter_side_to_256\n"
        "crop_policy:\n  train: random_crop_224\n  eval: center_crop_224\n"
        "augmentation:\n  train: {}\n  eval: []\n"
        "random_seed: 7\n"
    )

    config = load_preprocessing_config(tmp_path, "fixture-v1")

    assert config["preprocessing_version"] == "fixture-v1"
    assert config["random_seed"] == 7


def test_load_preprocessing_config_raises_on_missing_field(tmp_path):
    preprocessing_dir = tmp_path / "preprocessing"
    preprocessing_dir.mkdir()
    (preprocessing_dir / "fixture-v1.yaml").write_text("preprocessing_version: fixture-v1\nimage_size: 224\n")

    try:
        load_preprocessing_config(tmp_path, "fixture-v1")
    except ValueError as error:
        assert "normalization" in str(error)
    else:
        raise AssertionError("Expected missing required fields to raise")
