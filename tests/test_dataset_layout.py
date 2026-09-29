from pathlib import Path

import pytest

from data_platform.dataset_layout import (
    build_layout,
    load_dataset_config,
    load_storage_config,
    load_yaml_config,
    resolve_archive_paths,
)

CONFIG_ROOT = Path(__file__).resolve().parents[1] / "config"


def _layout_for(dataset_key):
    # The real dataset configs merged with the real storage roots, the same way every
    # dataset's 05_setup_tables_and_folders notebook does it.
    config = load_dataset_config(CONFIG_ROOT / "bronze" / "datasets" / f"{dataset_key}.yaml")
    config.update(load_yaml_config(CONFIG_ROOT / "storage.yaml"))
    return config, build_layout(config)


@pytest.mark.parametrize("dataset_key", ["isic_2019", "milk10k"])
def test_build_layout_names_bronze_after_the_dataset_and_shares_silver(dataset_key):
    _, layout = _layout_for(dataset_key)

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


def test_resolve_archive_paths_places_each_archive_and_its_metadata():
    config, layout = _layout_for("isic_2019")

    archives = resolve_archive_paths(config["archives"], layout["landing_paths"], layout["bronze_paths"])

    train = next(archive for archive in archives if archive["source_split"] == "train")
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
