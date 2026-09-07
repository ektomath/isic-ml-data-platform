from ml.train import check_shards_exist


def _write_index_json(shard_dir):
    shard_dir.mkdir(parents=True)
    (shard_dir / "index.json").write_text("{}")


def test_check_shards_exist_passes_when_every_split_has_an_index(tmp_path):
    base_shards_root = str(tmp_path)
    for split in ("train", "validation", "test"):
        _write_index_json(tmp_path / split)

    check_shards_exist(base_shards_root)  # should not raise


def test_check_shards_exist_raises_listing_every_missing_split(tmp_path):
    base_shards_root = str(tmp_path)
    _write_index_json(tmp_path / "train")
    # validation and test splits are deliberately left unexported

    try:
        check_shards_exist(base_shards_root)
    except FileNotFoundError as error:
        assert "validation" in str(error)
        assert "test" in str(error)
        assert "train" not in str(error).split("under")[0]  # the exported split isn't reported missing
    else:
        raise AssertionError("Expected missing shard splits to raise")


def test_check_shards_exist_treats_a_directory_with_no_index_json_as_missing(tmp_path):
    base_shards_root = str(tmp_path)
    for split in ("train", "validation", "test"):
        (tmp_path / split).mkdir(parents=True)  # directory exists, but export never completed

    try:
        check_shards_exist(base_shards_root)
    except FileNotFoundError as error:
        assert "train" in str(error) and "validation" in str(error) and "test" in str(error)
    else:
        raise AssertionError("Expected a directory with no index.json to be treated as missing")
