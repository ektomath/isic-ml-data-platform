"""Shared dataset layout helpers for Databricks notebooks and local scripts."""

from __future__ import annotations

from pathlib import Path

import yaml

BRONZE_INGESTION_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS bronze.ingestion_runs (
  ingestion_run_id STRING,
  dataset_name STRING,
  source_version STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status STRING,
  records_seen INT,
  records_written INT
)
USING DELTA
"""

ISIC_2019_SOURCE_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS bronze.isic_2019_source_metadata (
  image_id STRING,
  source_uri STRING,
  patient_id STRING,
  lesion_id STRING,
  label_raw STRING,
  label_source STRING,
  label_normalized STRING,
  acquisition_date STRING,
  source_checksum STRING,
  ingestion_run_id STRING,
  ingested_at TIMESTAMP
)
USING DELTA
"""


DEFAULT_STORAGE_CONTAINER = "isic-data"


def load_dataset_config(path: str | Path) -> dict:
    """Load a dataset config from YAML."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def build_layout(dataset_config: dict) -> dict:
    """Build path and table names from a dataset config."""
    dataset_key = dataset_config["dataset_key"]
    bronze_prefix = dataset_config["bronze_prefix"]
    silver_prefix = dataset_config.get("silver_prefix")
    gold_prefix = dataset_config.get("gold_prefix")

    return {
        "bronze_paths": {
            "root": bronze_prefix,
            "images": f"{bronze_prefix}/images",
            "metadata": f"{bronze_prefix}/metadata",
            "ingestion_runs": f"{bronze_prefix}/ingestion_runs",
        },
        "bronze_tables": {
            "source_metadata": f"bronze.{dataset_key}_source_metadata",
            "ingestion_runs": "bronze.ingestion_runs",
        },
        "silver_paths": None
        if silver_prefix is None
        else {
            "root": silver_prefix,
            "image_inventory": f"{silver_prefix}/image_inventory",
            "labels": f"{silver_prefix}/labels",
            "leakage_groups": f"{silver_prefix}/leakage_groups",
            "rejected_records": f"{silver_prefix}/rejected_records",
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
            "root": gold_prefix,
            "manifest": f"{gold_prefix}/manifest.parquet",
            "preprocessing": f"{gold_prefix}/preprocessing.yaml",
            "dataset_card": f"{gold_prefix}/dataset_card.md",
        },
        "gold_tables": None
        if gold_prefix is None
        else {
            "classification_manifest": "gold.classification_manifest",
        },
    }


def ensure_bronze_tables(spark) -> None:
    """Create the Bronze schema and Bronze tables if needed."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql(BRONZE_INGESTION_RUNS_DDL)
    spark.sql(ISIC_2019_SOURCE_METADATA_DDL)
