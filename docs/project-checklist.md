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

- 🟩 Build the canonical image inventory
- 🟩 Validate image readability and dimensions
- 🟩 Normalize metadata and labels
- 🟩 Create leakage-control groups
- 🟩 Write rejected-record outputs
- 🟩 Standardize label vocabulary for `malignancy` — canonical values live in `data_platform.labels.MALIGNANCY_VALUES` (`benign`/`malignant`/`indeterminate`, fixed) and are enforced at Silver-run time by `apply_label_normalization`'s `controlled_vocabularies` param: a dataset's `normalize_labels` producing a value outside the canonical set fails the Silver run loudly instead of silently drifting the shared column. `specific_diagnosis` is deliberately left uncanonicalized — free text by design; a crosswalk across datasets is deferred until a real second dataset shows what the actual mapping problem looks like, rather than speculatively designing against taxonomies that don't exist yet.

Verified end-to-end on Databricks: `notebooks/isic_2019/20_silver_validate.ipynb` ran successfully against real Bronze data on serverless compute, with sane accepted/rejected/leakage-group counts (32,413 accepted, 1,156 rejected for unresolvable label, 16,800 leakage-control groups). Running this also surfaced and fixed a real bug — `.cache()`/`.persist()` throughout `spark_io.py` is unsupported on Databricks serverless compute; see `AGENT.md`'s "Databricks serverless compute" section and `data_platform.spark_io.materialize()`. See `docs/decisions/003-silver-label-columns-not-map.md` for the label schema design.

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
- ⬜ Review test coverage across the whole codebase and fill gaps for anything important (not just the modules already covered) — a deliberate pass, not incidental coverage from writing feature tests
- 🟩 Wire GitHub Actions CI
- ⬜ Add notebook summaries for data quality and results
- ⬜ Add Databricks job deployment configuration when ready
- ⬜ Add a mock-data test for Bronze rerun behavior (merge/dedup/staging reuse on a second run), so this can be verified without re-running against real, costly ISIC archives — not urgent, defer until Silver/Gold work settles
- ⬜ Orchestrate pipeline stages as an automated data pipeline (e.g. Databricks Workflows/Jobs) instead of manual notebook runs, so Bronze/Silver/Gold can run and chain automatically

## Release

- ⬜ Review documentation for the first public release
- ⬜ Validate that no real ISIC images are committed
- ⬜ Publish the portfolio-ready version
