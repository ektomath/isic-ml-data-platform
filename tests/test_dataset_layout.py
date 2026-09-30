from pathlib import Path

import pytest

from data_platform.dataset_layout import load_dataset, load_storage_config

CONFIG_ROOT = Path(__file__).resolve().parents[1] / "config"


@pytest.mark.parametrize("dataset_key", ["isic_2019", "milk10k"])
def test_load_dataset_names_bronze_after_the_dataset_and_shares_silver(dataset_key):
    layout = load_dataset(CONFIG_ROOT, dataset_key)

    assert layout["bronze_paths"]["root"].endswith(f"/{dataset_key}")
    assert layout["bronze_paths"]["metadata"].endswith(f"/{dataset_key}/metadata")
    assert layout["landing_paths"]["dataset_archive"].endswith(f"/archives/{dataset_key}")
    assert layout["bronze_tables"] == {
        "source_metadata": f"bronze.{dataset_key}_source_metadata",
        "image_index": f"bronze.{dataset_key}_image_index",
        "ingestion_runs": "bronze.ingestion_runs",
    }
    assert layout["silver_tables"] == {
        "image_inventory": "silver.image_inventory",
        "leakage_groups": "silver.leakage_groups",
        "rejected_records": "silver.rejected_records",
    }


def test_load_dataset_places_each_archive_and_its_metadata():
    dataset = load_dataset(CONFIG_ROOT, "isic_2019")

    assert dataset["config"]["dataset_key"] == "isic_2019"
    assert dataset["config"]["storage_root"] == load_storage_config(CONFIG_ROOT)["storage_root"]
    train = next(archive for archive in dataset["archives"] if archive["source_split"] == "train")
    assert train["archive_dbfs_path"].endswith("/archives/isic_2019/ISIC-2019-train-images.zip")
    assert train["metadata_target_path"].endswith("/isic_2019/metadata/train/metadata.csv")
    assert train["archive_local_path"] == Path(train["archive_dbfs_path"])


def test_load_storage_config_accepts_the_real_config():
    config = load_storage_config(CONFIG_ROOT)

    assert config["landing_root"].startswith(f"/Volumes/{config['catalog']}/")


def test_load_storage_config_rejects_a_path_in_another_catalog(tmp_path):
    (tmp_path / "storage.yaml").write_text(
        "catalog: new_catalog\n"
        "landing_root: /Volumes/old_catalog/bronze/landing\n"
        "storage_root: /Volumes/new_catalog/bronze/files\n"
    )

    with pytest.raises(ValueError, match="landing_root"):
        load_storage_config(tmp_path)
