# Project Checklist

Status key: `🟩` done, `🟨` in progress, `⬜` not started

## Foundation

- 🟩 Define the repository structure
- 🟩 Set up `uv` for dependency management
- 🟩 Create notebooks for setting up storage and other resources

## Bronze

- 🟩 Define Bronze storage paths
- 🟩 Create general helper functions that load dataset config files and then create schemas and folders from them
- 🟩 Save images as blob rows in `bronze.isic_2019_image_blobs`
- 🟩 Ingest ISIC 2019 metadata (includes diagnosis labels from `metadata.csv`)
- 🟩 Record ingestion runs and checksums
- ⬜ Add secondary dataset

Verified end-to-end on Databricks with the real ISIC 2019 train/test archives. Rerun/merge/dedup behavior is covered by unit tests against fixture archives (`tests/test_files.py`) and by code review, but has not been re-verified against a second real-data run — re-running Bronze ingestion against the real archives is expensive (see `docs/lessons-learned.md`). See Automation section for a follow-up item to cover this with mock data instead.

## Silver

- 🟨 Build the canonical image inventory
- 🟨 Validate image readability and dimensions
- 🟨 Normalize metadata and labels
- 🟨 Create leakage-control groups
- 🟨 Write rejected-record outputs

Implementation-complete but not yet run: `notebooks/isic_2019/20_silver_validate.ipynb`, `data_platform.datasets.isic_2019.normalize_labels`, and `data_platform.validate.decode_image` are written and covered by local unit tests, but the notebook itself hasn't been run against real Bronze data on Databricks yet. Mark these done once that run succeeds and the accepted/rejected/group counts look right — same standard applied to Bronze. See `docs/decisions/003-silver-label-columns-not-map.md` for the label schema design.

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

- 🟨 Add fixture-backed tests (`data_platform.files`, `data_platform.validate`, `data_platform.datasets.isic_2019` covered; `ingest`/`publish` still placeholders)
- ⬜ Wire GitHub Actions CI
- ⬜ Add notebook summaries for data quality and results
- ⬜ Add Databricks job deployment configuration when ready
- ⬜ Add a mock-data test for Bronze rerun behavior (merge/dedup/staging reuse on a second run), so this can be verified without re-running against real, costly ISIC archives — not urgent, defer until Silver/Gold work settles
- ⬜ Orchestrate pipeline stages as an automated data pipeline (e.g. Databricks Workflows/Jobs) instead of manual notebook runs, so Bronze/Silver/Gold can run and chain automatically

## Release

- ⬜ Review documentation for the first public release
- ⬜ Validate that no real ISIC images are committed
- ⬜ Publish the portfolio-ready version
