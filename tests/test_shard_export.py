from archive_fixtures import build_archive as _build_archive
from archive_fixtures import checksum as _checksum
from streaming import StreamingDataset

from data_platform.shard_export import write_gold_shards_for_splits

# mosaicml-streaming's MDSWriter parses `out` with urllib.parse.urlparse to tell a local
# path from a cloud URL, and on Windows that misreads an absolute `C:\...` path's drive
# letter as a URL scheme ("c"), raising "Invalid Cloud provider prefix". A relative path
# parses with an empty (valid, local) scheme regardless of OS, so every test here chdirs
# into tmp_path (via monkeypatch, auto-restored) and passes relative shard dirs --
# real callers pass Databricks Volume paths (/Volumes/...), which never hit this at all.


def test_write_gold_shards_for_splits_writes_matching_samples(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    image_a, image_b, image_c = b"image-a-bytes", b"image-b-bytes", b"image-c-bytes"
    archive_path = _build_archive(tmp_path, {"a.jpg": image_a, "b.jpg": image_b, "c.jpg": image_c})

    rows_by_split = {
        "train": [
            {
                "image_id": "a",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#a.jpg",
                "source_checksum": _checksum(image_a),
                "label": "benign",
                "group_id": "g1",
            },
            {
                "image_id": "c",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#c.jpg",
                "source_checksum": _checksum(image_c),
                "label": "benign",
                "group_id": "g3",
            },
        ],
        "test": [
            {
                "image_id": "b",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#b.jpg",
                "source_checksum": _checksum(image_b),
                "label": "malignant",
                "group_id": "g2",
            },
        ],
    }

    result = write_gold_shards_for_splits(
        rows_by_split,
        staged_path_by_archive_uri={"dbfs:/landing/archive.zip": archive_path},
        shard_dir_by_split={"train": "shards/train", "test": "shards/test"},
        size_limit_bytes=1 << 26,
    )

    assert result == {
        "train": {"sample_count": 2, "shard_dir": "shards/train"},
        "test": {"sample_count": 1, "shard_dir": "shards/test"},
    }

    train_dataset = StreamingDataset(local="shards/train", remote=None, batch_size=1)
    train_samples = {sample["image_id"]: sample for sample in train_dataset}
    assert set(train_samples) == {"a", "c"}
    assert train_samples["a"]["image"] == image_a
    assert train_samples["a"]["label"] == "benign"
    assert train_samples["a"]["dataset_key"] == "isic_2019"
    assert train_samples["a"]["group_id"] == "g1"

    test_dataset = StreamingDataset(local="shards/test", remote=None, batch_size=1)
    test_samples = {sample["image_id"]: sample for sample in test_dataset}
    assert set(test_samples) == {"b"}
    assert test_samples["b"]["image"] == image_b
    assert test_samples["b"]["label"] == "malignant"


def test_write_gold_shards_for_splits_raises_on_checksum_mismatch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    image_a = b"image-a-bytes"
    archive_path = _build_archive(tmp_path, {"a.jpg": image_a})

    rows_by_split = {
        "train": [
            {
                "image_id": "a",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#a.jpg",
                "source_checksum": _checksum(b"different-bytes-entirely"),
                "label": "benign",
                "group_id": "g1",
            },
        ],
    }

    try:
        write_gold_shards_for_splits(
            rows_by_split,
            staged_path_by_archive_uri={"dbfs:/landing/archive.zip": archive_path},
            shard_dir_by_split={"train": "shards/train"},
            size_limit_bytes=1 << 26,
        )
    except RuntimeError as error:
        assert "no longer match the checksum" in str(error)
    else:
        raise AssertionError("Expected a checksum mismatch to raise")


def test_write_gold_shards_for_splits_raises_on_missing_image(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    archive_path = _build_archive(tmp_path, {"a.jpg": b"image-a-bytes"})

    rows_by_split = {
        "train": [
            {
                "image_id": "missing",
                "dataset_key": "isic_2019",
                "bronze_uri": "archive:dbfs:/landing/archive.zip#does-not-exist.jpg",
                "source_checksum": _checksum(b"whatever"),
                "label": "benign",
                "group_id": "g1",
            },
        ],
    }

    try:
        write_gold_shards_for_splits(
            rows_by_split,
            staged_path_by_archive_uri={"dbfs:/landing/archive.zip": archive_path},
            shard_dir_by_split={"train": "shards/train"},
            size_limit_bytes=1 << 26,
        )
    except RuntimeError as error:
        assert "missing from their source archive" in str(error)
    else:
        raise AssertionError("Expected a missing image to raise")


def test_failed_export_keeps_the_previous_export_and_leaves_no_partial_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    image_a = b"image-a-bytes"
    archive_path = _build_archive(tmp_path, {"a.jpg": image_a})
    good_row = {
        "image_id": "a",
        "dataset_key": "isic_2019",
        "bronze_uri": "archive:dbfs:/landing/archive.zip#a.jpg",
        "source_checksum": _checksum(image_a),
        "label": "benign",
        "group_id": "g1",
    }
    export_args = {
        "staged_path_by_archive_uri": {"dbfs:/landing/archive.zip": archive_path},
        "shard_dir_by_split": {"train": "shards/train"},
        "size_limit_bytes": 1 << 26,
    }
    write_gold_shards_for_splits({"train": [good_row]}, **export_args)
    previous_index = (tmp_path / "shards" / "train" / "index.json").read_bytes()

    bad_row = {**good_row, "source_checksum": _checksum(b"different-bytes-entirely")}
    try:
        write_gold_shards_for_splits({"train": [bad_row]}, **export_args)
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected a checksum mismatch to raise")

    assert (tmp_path / "shards" / "train" / "index.json").read_bytes() == previous_index
    assert not (tmp_path / "shards" / "train.partial").exists()
