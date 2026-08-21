# ISIC ML Data Platform

> A portfolio project demonstrating Azure, Databricks, and reproducible computer-vision data engineering.

An Azure and Databricks-based ISIC 2019 data platform that turns raw images and metadata into validated Bronze, Silver, and Gold data products for a reproducible classifier workflow.

## What this repository contains

- `src/`: pipeline implementation for ingest, validate, publish, and train
- `src/isic_pipeline/bootstrap.py`: shared dataset layout helpers
- `config/`: versioned pipeline and preprocessing settings
- `tests/`: fixture-backed checks that do not require the full ISIC dataset
- `docs/`: architecture, data contract, and project checklist
- `notebooks/`: bootstrap, data-quality, and results notebooks

## Architecture

The platform keeps raw JPEGs in Bronze, validates and normalizes records in Silver, and publishes a versioned training manifest in Gold.

See [docs/architecture.md](docs/architecture.md) for the system layout, Azure storage structure, and Databricks execution model.

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

The goal is to use AI as an accelerator while still maintaining correctness, clarity, and maintainability. 

## Notebook flow

- `notebooks/00_bootstrap_storage.ipynb`: reusable layout helper pattern
- `notebooks/isic_2019/10_bronze_setup.ipynb`: concrete Bronze setup for ISIC 2019
- `notebooks/isic_2019/20_bronze_ingest_sample.ipynb`: Bronze sample ingest and layout check
- `notebooks/30_data_quality_summary.ipynb`: data-quality exploration
- `notebooks/40_baseline_results.ipynb`: baseline model results


## Licence

-
