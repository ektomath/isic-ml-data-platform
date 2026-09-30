"""Paths and table names for one source dataset, built from its
config/bronze/datasets/<dataset>.yaml and config/storage.yaml. Gold isn't included: a release spans
several datasets, so the Gold notebooks name their tables themselves.
"""

from __future__ import annotations

from pathlib import Path

import yaml


def load_yaml_config(path: str | Path) -> dict:
    """Load a YAML config file."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_storage_config(config_root: str | Path) -> dict:
    """Load config/storage.yaml and check that its Volume paths and manifest table all sit in
    the catalog it names, so the catalog can be renamed by editing that one file."""
    config = load_yaml_config(Path(config_root) / "storage.yaml")
    catalog = config["catalog"]
    mismatched = [
        key
        for key, prefix in (("landing_root", f"/Volumes/{catalog}/"), ("storage_root", f"/Volumes/{catalog}/"),
                            ("manifest_table", f"{catalog}."))
        if key in config and not str(config[key]).startswith(prefix)
    ]
    if mismatched:
        raise ValueError(f"config/storage.yaml: {mismatched} don't use catalog {catalog!r}")
    return config


def join_storage_path(storage_root: str, relative_path: str) -> str:
    """Join a governed storage root and a relative medallion path."""
    return f"{storage_root.rstrip('/')}/{relative_path.lstrip('/')}"


def resolve_archive_paths(archives: list[dict], landing_paths: dict, bronze_paths: dict) -> list[dict]:
    """Return a copy of a dataset config's `archives` entries, each with its resolved paths added:

    - `archive_dbfs_path`: the archive's landing Volume path.
    - `archive_local_path`: the same path, as a Path.
    - `metadata_dir_path`: the split's Bronze metadata folder.
    - `metadata_local_root`: the same folder, as a Path.
    - `metadata_target_path`: where the extracted metadata file ends up.

    The Path versions are for plain Python file I/O in data_platform.files (Volumes are readable as
    local files on the driver); the strings are for dbutils.fs and spark.read.
    """
    resolved = []
    for archive in archives:
        archive = dict(archive)
        source_split = archive["source_split"]
        archive["archive_dbfs_path"] = join_storage_path(landing_paths["dataset_archive"], archive["archive_filename"])
        archive["archive_local_path"] = Path(archive["archive_dbfs_path"])
        archive["metadata_dir_path"] = join_storage_path(bronze_paths["metadata"], source_split)
        archive["metadata_local_root"] = Path(archive["metadata_dir_path"])
        archive["metadata_target_path"] = join_storage_path(archive["metadata_dir_path"], archive["metadata_filename"])
        resolved.append(archive)
    return resolved


def build_layout(dataset_config: dict) -> dict:
    """Build the paths and table names one dataset's Bronze and Silver notebooks use, from its
    config merged with config/storage.yaml's roots. Returns:

    - `landing_paths`: {root, archives, dataset_archive}, where the archives land.
    - `bronze_paths`: {root, metadata}, the dataset's Bronze folder and its metadata folder.
    - `bronze_tables`: {source_metadata, image_index, ingestion_runs}; the last is shared by all
      datasets.
    - `silver_tables`: {image_inventory, leakage_groups, rejected_records}, shared by all datasets.
      Silver has no folders.
    """
    dataset_key = dataset_config["dataset_key"]
    landing_root = dataset_config["landing_root"]
    storage_root = dataset_config["storage_root"]

    return {
        "landing_paths": {
            "root": landing_root.rstrip("/"),
            "archives": join_storage_path(landing_root, "archives"),
            "dataset_archive": join_storage_path(landing_root, f"archives/{dataset_key}"),
        },
        "bronze_paths": {
            "root": join_storage_path(storage_root, dataset_key),
            "metadata": join_storage_path(storage_root, f"{dataset_key}/metadata"),
        },
        "bronze_tables": {
            "source_metadata": f"bronze.{dataset_key}_source_metadata",
            "image_index": f"bronze.{dataset_key}_image_index",
            "ingestion_runs": "bronze.ingestion_runs",
        },
        "silver_tables": {
            "image_inventory": "silver.image_inventory",
            "leakage_groups": "silver.leakage_groups",
            "rejected_records": "silver.rejected_records",
        },
    }


def load_dataset(config_root: str | Path, dataset_key: str) -> dict:
    """Everything a dataset's notebooks need, from its config and config/storage.yaml:
    build_layout's paths and tables, plus `config` (the dataset config with the storage roots
    merged in) and `archives` (resolve_archive_paths' output).
    """
    config_root = Path(config_root)
    config = load_yaml_config(config_root / "bronze" / "datasets" / f"{dataset_key}.yaml")
    storage_config = load_storage_config(config_root)
    config["landing_root"] = storage_config["landing_root"]
    config["storage_root"] = storage_config["storage_root"]
    layout = build_layout(config)
    archives = resolve_archive_paths(config["archives"], layout["landing_paths"], layout["bronze_paths"])
    return {**layout, "config": config, "archives": archives}
