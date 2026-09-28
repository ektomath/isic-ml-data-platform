"""Shared dataset layout helpers for Databricks notebooks and local scripts.

A "layout" is the dict `build_layout()` returns below: every Volume path and
table name one source dataset's Bronze/Silver notebooks need, all derived from
that dataset's `config/bronze/datasets/<dataset>.yaml` plus the shared
`config/storage.yaml` roots. It is deliberately scoped to *one dataset* — Gold
is not part of it (see `build_layout`'s docstring for why) and there is no
`GoldLayout`-equivalent here; `notebooks/30_create_gold_manifest.ipynb` and
`notebooks/31_export_gold_shards.ipynb` build their own small `GOLD_TABLES`
dict inline instead, since one Gold release spans however many source
datasets it includes, not one.
"""

from __future__ import annotations

from pathlib import Path

import yaml


def load_yaml_config(path: str | Path) -> dict:
    """Load a YAML config file. Generic -- used for every config file in this
    project (dataset configs, Gold manifest/export configs, storage.yaml), not
    just dataset configs; see load_dataset_config for the dataset-specific case.
    """
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_dataset_config(path: str | Path) -> dict:
    """Load one dataset's config file (config/bronze/datasets/<dataset>.yaml).

    Mechanically identical to load_yaml_config -- this exists as its own name
    purely so a call site (`load_dataset_config(CONFIG_ROOT / "bronze" / "datasets" / "isic_2019.yaml")`)
    reads as "this YAML is a dataset config" without the reader having to check
    the path argument, the same way every dataset notebook already names its
    loaded dict `DATASET_CONFIG` rather than a generic `config`.
    """
    return load_yaml_config(path)


def join_storage_path(storage_root: str, relative_path: str) -> str:
    """Join a governed storage root and a relative medallion path."""
    return f"{storage_root.rstrip('/')}/{relative_path.lstrip('/')}"


def resolve_archive_paths(archives: list[dict], landing_paths: dict, bronze_paths: dict) -> list[dict]:
    """Return a copy of `archives` (each dict from a dataset config's `archives`
    list -- `source_split`, `archive_filename`, `metadata_filename`) with five
    resolved-path fields added to each entry:

    - `archive_dbfs_path` (str) -- this archive's full landing Volume path, for
      `dbutils.fs` / display.
    - `archive_local_path` (Path) -- identical path, as a `Path`, for
      `data_platform.files.check_archives_exist`/`stage_archive_locally`, which
      read it with plain Python file I/O.
    - `metadata_dir_path` (str) -- this split's Bronze metadata folder.
    - `metadata_local_root` (Path) -- identical path, as a `Path`, for
      `stage_archives_and_extract_metadata`'s extraction target.
    - `metadata_target_path` (str) -- the specific metadata file's full path
      once extracted there.

    `archive_local_path`/`metadata_local_root` are `Path` because they are
    handed to plain-Python file I/O in `data_platform.files` (Unity Catalog
    Volume paths are directly readable on the driver's local filesystem);
    every other path stays a plain string for use with `dbutils.fs` and
    `spark.read`.
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
    """Build every Volume path and table name one source dataset's Bronze/Silver
    notebooks need, from that dataset's config (`config/bronze/datasets/<dataset>.yaml`,
    already merged with `config/storage.yaml`'s `landing_root`/`storage_root` by the
    caller -- see any `05_setup_tables_and_folders.ipynb` for the pattern). Returns:

    - `landing_paths` -- `None` if `dataset_config` has no `landing_root` set (a
      dataset config loaded without merging in `config/storage.yaml` first), else
      `{root, archives, dataset_archive}`: where this dataset's source archives
      land.
    - `bronze_paths` -- `{root, metadata}`: this dataset's Bronze folder, named after
      its `dataset_key`, and the metadata folder inside it.
    - `bronze_tables` -- `{source_metadata, image_index, ingestion_runs}`, this
      dataset's two Bronze tables plus the shared `bronze.ingestion_runs`. Always
      present.
    - `silver_tables` -- `{image_inventory, leakage_groups, rejected_records}`, the
      three shared, not dataset-prefixed, Silver tables (the same for every
      dataset). There is no `silver_paths` -- Silver is table-only, no Volume
      folders (`docs/architecture.md`).

    Gold is not part of this layout at all, on purpose: a Gold release
    (`gold.manifest_rows`, and the ephemeral MosaicML shard export built from
    it) spans however many source datasets a given release includes, not one,
    so it can never be derived from a single dataset's config the way
    everything above can. `notebooks/30_create_gold_manifest.ipynb` and
    `notebooks/31_export_gold_shards.ipynb` build their own small
    `GOLD_TABLES = {"manifest_rows": "gold.manifest_rows"}` dict inline instead
    of calling this function for it.
    """
    dataset_key = dataset_config["dataset_key"]
    landing_root = dataset_config.get("landing_root")
    storage_root = dataset_config["storage_root"]

    return {
        "landing_paths": None
        if landing_root is None
        else {
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
