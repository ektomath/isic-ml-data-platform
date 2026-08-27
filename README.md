# ISIC ML Data Platform

> A portfolio project demonstrating Azure, Databricks, and reproducible computer-vision data engineering.

An Azure and Databricks-based ISIC 2019 data platform that turns raw images and metadata into validated Bronze, Silver, and Gold data products for a reproducible classifier workflow.

## What this repository contains

- `src/`: pipeline implementation for ingest, validate, publish, and train
- `src/data_platform/setup.py`: shared dataset layout helpers
- `config/`: versioned pipeline, preprocessing, and shared storage-root settings
- `tests/`: fixture-backed checks that do not require the full ISIC dataset
- `docs/`: architecture, data contract, and project checklist
- `AGENT.md`: canonical repo-local agent instructions and working context
- `notebooks/`: setup, data-quality, and results notebooks

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
- `notebooks/00_setup_storage.ipynb`: shared medallion schemas and storage conventions
- `notebooks/isic_2019/05_create_bronze_tables_and_folders.ipynb`: ISIC 2019 Bronze table and volume folder setup
- `notebooks/isic_2019/10_bronze_ingest.ipynb`: end-to-end Bronze ingestion for ISIC 2019
- `notebooks/30_data_quality_summary.ipynb`: data-quality exploration
- `notebooks/40_baseline_results.ipynb`: baseline model results


## Licence

-
