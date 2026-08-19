# Project Checklist

Status key: `🟩` done, `🟨` in progress, `⬜` not started

## Foundation

- 🟩 Define the repository structure
- 🟩 Pin the local Python version
- 🟩 Set up `uv` for dependency management
- 🟩 Intentionally leave the source code unlicensed for showcase use
- ⬜ Confirm the Databricks workspace and deployment model

## Bronze

- ⬜ Define Bronze storage paths
- ⬜ Ingest ISIC 2019 metadata
- ⬜ Ingest ISIC 2019 labels
- ⬜ Copy source JPEGs into Bronze
- ⬜ Record ingestion runs and checksums

## Silver

- ⬜ Build the canonical image inventory
- ⬜ Validate image readability and dimensions
- ⬜ Normalize metadata and labels
- ⬜ Create leakage-control groups
- ⬜ Write rejected-record outputs

## Gold

- ⬜ Publish the versioned classification manifest
- ⬜ Freeze preprocessing configuration
- ⬜ Generate the dataset card
- ⬜ Record manifest checksum and version metadata

## Training

- ⬜ Implement the baseline classifier
- ⬜ Add deterministic train/validation/test splitting
- ⬜ Record experiment tracking with MLflow
- ⬜ Capture baseline metrics and confusion matrix

## Automation

- ⬜ Add fixture-backed tests
- ⬜ Wire GitHub Actions CI
- ⬜ Add notebook summaries for data quality and results
- ⬜ Add Databricks job deployment configuration when ready

## Release

- ⬜ Review documentation for the first public release
- ⬜ Validate that no real ISIC images are committed
- ⬜ Publish the portfolio-ready version
