from torchvision.transforms.functional import pil_to_tensor

from archive_fixtures import build_archive as _build_archive
from archive_fixtures import checksum as _checksum
from archive_fixtures import jpeg_bytes as _jpeg_bytes
from data_platform.shard_export import write_gold_shards_for_splits
from ml.dataset import GoldShardDataset, build_dataloader, label_to_index_map

# Same Windows urlparse-drive-letter workaround already documented in tests/test_shard_export.py
# -- MDSWriter (used to build these fixture shards) and StreamingDataset (used to read them
# back, which is what GoldShardDataset subclasses) both parse local paths the same way.


def _build_train_shard(tmp_path):
    image_benign, image_malignant = _jpeg_bytes((255, 0, 0)), _jpeg_bytes((0, 255, 0))
    archive_path = _build_archive(tmp_path, {"a.jpg": image_benign, "b.jpg": image_malignant})

    rows_by_split = {
        "train": [
            {
                "image_id": "a",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#a.jpg",
                "source_checksum": _checksum(image_benign),
                "label": "benign",
                "group_id": "g1",
            },
            {
                "image_id": "b",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#b.jpg",
                "source_checksum": _checksum(image_malignant),
                "label": "malignant",
                "group_id": "g2",
            },
        ],
    }

    write_gold_shards_for_splits(
        rows_by_split,
        staged_path_by_archive_uri={"dbfs:/landing/archive.zip": archive_path},
        shard_dir_by_split={"train": "shards/train"},
        size_limit_bytes=1 << 26,
    )
    return "shards/train"


def test_label_to_index_map_preserves_order():
    assert label_to_index_map(["benign", "malignant", "indeterminate"]) == {
        "benign": 0,
        "malignant": 1,
        "indeterminate": 2,
    }


def test_gold_shard_dataset_decodes_and_maps_labels(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shard_dir = _build_train_shard(tmp_path)
    label_to_index = label_to_index_map(["benign", "malignant"])
    identity_transform = pil_to_tensor

    dataset = GoldShardDataset(local=shard_dir, transform=identity_transform, label_to_index=label_to_index)

    assert len(dataset) == 2
    samples = {dataset[i][1] for i in range(len(dataset))}
    assert samples == {0, 1}  # benign -> 0, malignant -> 1, in some order


def test_gold_shard_dataset_raises_on_out_of_vocabulary_label(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shard_dir = _build_train_shard(tmp_path)
    # "indeterminate" is never in this shard, but leaving it out of label_to_index simulates
    # the real mismatch case: a label present in the shard but missing from the pinned vocabulary.
    label_to_index = {"benign": 0}
    identity_transform = lambda image: image  # noqa: E731

    dataset = GoldShardDataset(local=shard_dir, transform=identity_transform, label_to_index=label_to_index)

    try:
        for i in range(len(dataset)):
            dataset[i]
    except KeyError:
        pass
    else:
        raise AssertionError("Expected an out-of-vocabulary label to raise KeyError")


def test_build_dataloader_yields_batches(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shard_dir = _build_train_shard(tmp_path)
    label_to_index = label_to_index_map(["benign", "malignant"])
    tensor_transform = lambda image: pil_to_tensor(image.resize((4, 4)))  # noqa: E731

    dataloader = build_dataloader(shard_dir, tensor_transform, label_to_index, batch_size=2, shuffle=False)
    batches = list(dataloader)

    assert len(batches) == 1
    images, labels = batches[0]
    assert images.shape[0] == 2
    assert set(labels.tolist()) == {0, 1}
