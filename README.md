# ISIC ML Data Platform

> A portfolio project demonstrating Azure, Databricks, and reproducible computer-vision data engineering.

An Azure and Databricks-based ISIC 2019 data platform that turns raw images and metadata into validated Bronze, Silver, and Gold data products for a reproducible classifier workflow.

## What this repository contains

- `src/`: pipeline implementation for ingest, validate, publish, and train
- `src/data_platform/`: shared, dataset-agnostic helpers — `files.py` (filesystem/archive handling), `layout.py` (dataset layout/config), `validate.py` (image validation), `labels.py` (canonical label vocabulary and diagnosis-hierarchy normalization, shared by every onboarded dataset so far), `gold.py` (pure-Python Gold sampling and leakage-aware split assignment), `spark_io.py` (Bronze/Silver/Gold Spark pipeline functions), `datasets/` (a landing spot for a *future* dataset whose label logic doesn't fit `labels.py`'s shape — empty until one needs it)
- `config/`: versioned pipeline, preprocessing, and shared storage-root settings — `config/datasets/<dataset>.yaml` per onboarded dataset, `config/gold/manifests/<name>.yaml` per Gold manifest release
- `tests/`: fixture-backed checks that do not require the full ISIC dataset
- `docs/`: architecture, data contract, Silver validation rules, ADRs (`docs/decisions/`), project checklist, and lessons learned
- `AGENT.md`: canonical repo-local agent instructions and working context
- `notebooks/`: Databricks setup, Bronze ingestion, Silver validation, and Gold publishing notebooks

## Architecture

The platform keeps source archives in a landing volume, stores raw image bytes in a Bronze Delta table, validates and normalizes records in Silver, and publishes a versioned training manifest in Gold.

See [docs/architecture.md](docs/architecture.md) for the system layout, Unity Catalog storage structure, and Databricks execution model.

## Data contract

The Gold manifest is the training contract. It is built from Bronze source metadata, Silver validation results, leakage-control groups, and versioned preprocessing settings.

See [docs/data_contract.md](docs/data_contract.md) for the canonical table schemas and file outputs.

## Local setup

Prerequisites:

- Git
- Python 3.11
- `uv`
- PyCharm or another IDE

Installation:

```powershell
uv venv --python 3.11 .venv
.venv\Scripts\Activate.ps1
uv sync --extra dev
```

Run the tests:

```powershell
uv run pytest
```

## Source data

The project uses the [ISIC Archive](https://www.isic-archive.com/) and the [ISIC Archive API](https://api.isic-archive.com/api/docs/swagger/). Follow the applicable dataset terms, licences, and citation requirements before downloading or redistributing any data.

## Development approach


This project is built with heavy use of AI tools for iterative code and documentation generation. I review, edit, and validate the output manually, and I keep architectural decisions, implementation quality, and final responsibility under my own control.

The goal is to use AI as an accelerator/amplifier while still maintaining correctness, clarity, and maintainability. 

In general my workflow is to generate smaller sections of code in chunks rather than prompt the AI to "build data engineering pipeline, no mistakes please."

## Notebook flow

- `notebooks/_setup_env.ipynb`: shared Databricks notebook environment setup for imports
- `notebooks/00_setup_storage_and_shared_tables.ipynb`: shared medallion schemas, storage conventions, and every shared table (`bronze.ingestion_runs`, all of Silver, all of Gold) — created once here since it's identical regardless of dataset
- `notebooks/isic_2019/05_setup_tables_and_folders.ipynb`: ISIC 2019 Bronze table and volume folder setup
- `notebooks/isic_2019/10_bronze_ingest.ipynb`: end-to-end Bronze ingestion for ISIC 2019
- `notebooks/isic_2019/20_silver_validate.ipynb`: label normalization, image validation, and leakage-control grouping for ISIC 2019
- `notebooks/milk10k/05_setup_tables_and_folders.ipynb`, `10_bronze_ingest.ipynb`, `20_silver_validate.ipynb`: the same flow for MILK10k, the second onboarded dataset
- `notebooks/30_create_gold_manifest.ipynb`: publishes a `gold.manifest_rows` release spanning every included dataset in one run (top-level, not under a dataset folder). Currently a `sample-v1` release (~100 images per dataset, `malignancy` label) built to iterate on the Gold pipeline cheaply, not the full-scale release. `gold.manifest_registry` (a view) gives an overview of every release published so far

