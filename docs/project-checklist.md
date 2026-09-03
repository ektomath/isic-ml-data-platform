# Project Checklist

Status key: `🟩` done, `🟨` in progress, `⬜` not started

## Foundation

- 🟩 Define the repository structure
- 🟩 Set up `uv` for dependency management
- 🟩 Create notebooks for setting up storage and other resources

## Bronze

- 🟩 Define Bronze storage paths
- 🟩 Create general helper functions that load dataset config files and then create schemas and folders from them
- 🟨 Index images (checksum, byte_length, archive locator — no image bytes) in `bronze.isic_2019_image_index`. Bronze no longer stores image bytes at all, not even as Delta rows — the source archives are the sole permanent store of image bytes; see `docs/decisions/006-stream-archives-no-blob-storage.md` (supersedes `docs/decisions/002-store-bronze-images-as-delta-blobs.md`). This is implementation-complete but **not yet run** — ISIC 2019 Bronze was already verified once under the old blob-storing design (see the verification note below) and needs to be rerun under this new design; see `AGENT.md`'s "Current next step."
- 🟩 Ingest ISIC 2019 metadata (includes diagnosis labels from `metadata.csv`)
- 🟩 Record ingestion runs and checksums
- 🟨 Add secondary dataset — MILK10k (ISIC Archive), single release with no train/test split. `config/bronze/datasets/milk10k.yaml`, the shared `data_platform.labels.normalize_diagnosis_labels` (unit-tested against the real `metadata.csv` header/sample row, `tests/test_labels.py`), the `bronze.milk10k_source_metadata` schema, and all three `notebooks/milk10k/*.ipynb` are written. Not yet run against Databricks — blocked on the archive download/extraction finishing. See `AGENT.md`'s "Current next step" for what's left.

Previously verified end-to-end on Databricks with the real ISIC 2019 train/test archives under the old blob-storing design (32,413 accepted downstream in Silver — see the Silver section below); that run's counts are the regression baseline for rerunning under the new streaming-index design. Rerun/merge/dedup behavior is covered by unit tests against fixture archives (`tests/test_files.py`) and by code review, but has not been re-verified against a second real-data run — re-running Bronze ingestion against the real archives is expensive (see `docs/lessons-learned.md`). See Automation section for a follow-up item to cover this with mock data instead.

## Silver

- 🟩 Build the canonical image inventory
- 🟩 Validate image readability and dimensions
- 🟩 Normalize metadata and labels
- 🟩 Create leakage-control groups
- 🟩 Write rejected-record outputs
- 🟩 Standardize label vocabulary for `malignancy` — canonical values live in `data_platform.labels.MALIGNANCY_VALUES` (`benign`/`malignant`/`indeterminate`, fixed) and are enforced at Silver-run time by `apply_label_normalization`'s `controlled_vocabularies` param: a dataset's `normalize_labels` producing a value outside the canonical set fails the Silver run loudly instead of silently drifting the shared column. `specific_diagnosis` is deliberately left uncanonicalized — free text by design; a crosswalk across datasets is deferred until a real second dataset shows what the actual mapping problem looks like, rather than speculatively designing against taxonomies that don't exist yet.

Previously verified end-to-end on Databricks under the old Bronze-blob-table design: `notebooks/isic_2019/20_silver_validate.ipynb` ran successfully against real Bronze data on serverless compute, with sane accepted/rejected/leakage-group counts (32,413 accepted, 1,156 rejected for unresolvable label, 16,800 leakage-control groups). Running this also surfaced and fixed a real bug — `.cache()`/`.persist()` throughout `spark_io.py` is unsupported on Databricks serverless compute; see `AGENT.md`'s "Databricks serverless compute" section and `data_platform.spark_io.materialize()`. See `docs/decisions/003-silver-label-columns-not-map.md` for the label schema design.

`validate_images` is rewritten (`docs/decisions/006-stream-archives-no-blob-storage.md`) to stream candidate images directly from their source archives instead of joining a Bronze blob table — implementation-complete but **not yet rerun** against real data; the above counts are the regression baseline once it is.

## Gold

- 🟨 Publish the versioned classification manifest — `sample-v1` release (~100 images from `isic_2019` and `milk10k` each, `malignancy` label only), defined by `config/gold/manifests/sample-v1.yaml` and built by `notebooks/30_create_gold_manifest.ipynb` (`data_platform.sampling`, the Gold pipeline functions in `data_platform.spark_io`), to iterate on the Gold pipeline cheaply, not as the real release. Each manifest is its own config file — same convention as `config/bronze/datasets/<dataset>.yaml` — so a new manifest is a new YAML file, not a new notebook. Not yet run against Databricks. The full-scale release (all accepted rows, not a sample) is separate future work.
- 🟩 Freeze preprocessing configuration — `config/preprocessing.yaml` versioned (`version: sample-v1`), referenced by `gold.manifest_rows.preprocessing_version`. One field short of the full `docs/data_contract.md` spec (framework-specific runtime notes) — deliberately, since no training framework is chosen yet; documented in the file itself, not filled with a placeholder.
- ⬜ Generate the dataset card — deferred for `sample-v1` (a 200-image iteration sample doesn't need one); build for the full-scale release.
- 🟨 Record manifest checksum and version metadata — `manifest_row_hash` (per-row) and `dataset_version`/`sample_seed`/`split_seed`/`preprocessing_version` (release-level) are in the schema and populated; `sample_seed` and `split_seed` are deliberately separate columns so two releases can be verified (via `gold.manifest_registry`) to share the same image pool even if their split or preprocessing differs.
- 🟨 Export a training-consumable format from a published manifest — `notebooks/31_export_gold_shards.ipynb` streams a `dataset_version`'s images from their source archives into a derived, per-split MosaicML shard export (`streaming.MDSWriter`/`streaming.StreamingDataset`, `config/gold/exports/sample-v1.yaml`), for both local-machine and Databricks ML-cluster training. Written but **not yet run against Databricks**. See `docs/decisions/006-stream-archives-no-blob-storage.md`. Shard retention is deliberately undecided (`docs/decisions/009-gold-shard-retention-undecided.md`) — no cleanup runs by default; `remove_expired_exports` exists as a general-purpose utility for whenever a retention policy does get decided.
- ⬜ Detect cross-dataset duplicate images before combining datasets into one manifest — leakage-group assignment is `dataset_key`-scoped by design (`docs/decisions/004-cross-dataset-leakage-not-checked.md`), so if the same real image exists in two onboarded datasets (plausible for ISIC-Archive-family datasets like `isic_2019`/`milk10k`), it gets unrelated `group_id`s in each and could land in different splits (`train` in one dataset's copy, `test` in the other's) when a Gold manifest combines both — real train/test leakage, not just a theoretical risk. `source_checksum` equality across datasets is the sound signal to catch this (unlike `patient_id`/`lesion_id`, which are dataset-issued and unsound to compare across datasets), and would naturally belong in `data_platform.spark_io.build_gold_candidate_groups` — checked only when a manifest actually spans more than one `dataset_key`, since that's the only time it matters. Not built yet; revisit once a manifest actually combines multiple datasets for real.

## Training

- ⬜ Implement the baseline classifier
- ⬜ Add deterministic train/validation/test splitting
- ⬜ Record experiment tracking with MLflow — the tracking server built into the Databricks workspace, reachable from a local machine via its REST client (no Databricks Connect/Spark session needed just to log a run); see `docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`
- ⬜ Pin data+preprocessing identity per training run — `config/gold/training_runs/<name>.yaml` pairs one `dataset_version` with one `preprocessing_version` (same versioned-config convention as `config/gold/manifests/` and `config/gold/exports/`); the training entrypoint accepts only that one name, not two free-standing parameters, so there's no code path to train against an unpinned pairing. Design decided, implementation deliberately deferred until this point — see `docs/decisions/010-...md`
- ⬜ Record training-run provenance in `gold.training_run_registry` (`dataset_version`, `preprocessing_version`, MLflow `run_id`) — same audit-trail pattern as `bronze.ingestion_runs`/`gold.manifest_registry`, populated from MLflow runs rather than written to directly by local training code
- ⬜ Capture baseline metrics and confusion matrix

## Automation

- 🟨 Add fixture-backed tests (`data_platform.files`, `data_platform.validate`, `data_platform.labels`, `data_platform.sampling` covered; `ingest`/`publish` still placeholders; `data_platform.dataset_layout` is pure Python too but currently has no tests — a real gap)
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
