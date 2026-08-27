"""Shared dataset layout helpers for Databricks notebooks and local scripts."""

from __future__ import annotations

from pathlib import Path

import yaml


def load_yaml_config(path: str | Path) -> dict:
    """Load a YAML config file."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_dataset_config(path: str | Path) -> dict:
    """Load a dataset config from YAML."""
    return load_yaml_config(path)


def join_storage_path(storage_root: str, relative_path: str) -> str:
    """Join a governed storage root and a relative medallion path."""
    return f"{storage_root.rstrip('/')}/{relative_path.lstrip('/')}"


def build_layout(dataset_config: dict) -> dict:
    """Build path and table names from a dataset config."""
    dataset_key = dataset_config["dataset_key"]
    landing_root = dataset_config.get("landing_root")
    storage_root = dataset_config["storage_root"]
    bronze_prefix = dataset_config["bronze_prefix"]
    silver_prefix = dataset_config.get("silver_prefix")
    gold_prefix = dataset_config.get("gold_prefix")

    return {
        "landing_paths": None
        if landing_root is None
        else {
            "root": landing_root.rstrip("/"),
            "archives": join_storage_path(landing_root, "archives"),
            "dataset_archive": join_storage_path(landing_root, f"archives/{dataset_key}"),
        },
        "bronze_paths": {
            "root": join_storage_path(storage_root, bronze_prefix),
            "metadata": join_storage_path(storage_root, f"{bronze_prefix}/metadata"),
        },
        "bronze_tables": {
            "source_metadata": f"bronze.{dataset_key}_source_metadata",
            "image_blobs": f"bronze.{dataset_key}_image_blobs",
            "ingestion_runs": "bronze.ingestion_runs",
        },
        "silver_paths": None
        if silver_prefix is None
        else {
            "root": join_storage_path(storage_root, silver_prefix),
            "image_inventory": join_storage_path(storage_root, f"{silver_prefix}/image_inventory"),
            "labels": join_storage_path(storage_root, f"{silver_prefix}/labels"),
            "leakage_groups": join_storage_path(storage_root, f"{silver_prefix}/leakage_groups"),
            "rejected_records": join_storage_path(storage_root, f"{silver_prefix}/rejected_records"),
        },
        "silver_tables": None
        if silver_prefix is None
        else {
            "image_inventory": "silver.image_inventory",
            "leakage_groups": "silver.leakage_groups",
            "rejected_records": "silver.rejected_records",
        },
        "gold_paths": None
        if gold_prefix is None
        else {
            "root": join_storage_path(storage_root, gold_prefix),
            "manifest": join_storage_path(storage_root, f"{gold_prefix}/manifest.parquet"),
            "preprocessing": join_storage_path(storage_root, f"{gold_prefix}/preprocessing.yaml"),
            "dataset_card": join_storage_path(storage_root, f"{gold_prefix}/dataset_card.md"),
        },
        "gold_tables": None
        if gold_prefix is None
        else {
            "classification_manifest": "gold.classification_manifest",
        },
    }
