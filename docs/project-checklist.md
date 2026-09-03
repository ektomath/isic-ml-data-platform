# Project Checklist

Status key: `🟩` done, `🟨` in progress, `⬜` not started

## Foundation

- 🟩 Define the repository structure
- 🟩 Set up `uv` for dependency management
- 🟩 Create notebooks for setting up storage and other resources

## Bronze

- 🟩 Define Bronze storage paths
- 🟩 Create general helper functions that load dataset config files and then create schemas and folders from them
- 🟩 Index images (checksum, byte_length, archive locator — no image bytes) in `bronze.isic_2019_image_index`. 
- 🟩 Ingest ISIC 2019 metadata 
- 🟩 Record ingestion runs and checksums
- 🟩 Add secondary dataset — MILK10k (ISIC Archive)

Verified end-to-end on Databricks under the current archive-streaming/index-only design (`docs/decisions/006-stream-archives-no-blob-storage.md`) for both `isic_2019` and `milk10k`. Rerun/merge/dedup behavior is additionally covered by unit tests against fixture archives (`tests/test_files.py`) — re-running Bronze ingestion against the real archives is expensive (see `docs/lessons-learned.md`), so unit tests are the ongoing regression check for that behavior rather than repeated real-data reruns. See Automation section for a follow-up item to cover this with mock data too.

## Silver

- 🟩 Build the canonical image inventory
- 🟩 Validate image readability and dimensions
- 🟩 Normalize metadata and labels
- 🟩 Create leakage-control groups
- 🟩 Write rejected-record outputs
- 🟩 Standardize label vocabulary for `malignancy` — canonical values live in `data_platform.labels.MALIGNANCY_VALUES` (`benign`/`malignant`/`indeterminate`, fixed) and are enforced at Silver-run time by `apply_label_normalization`'s `controlled_vocabularies` param: a dataset's `normalize_labels` producing a value outside the canonical set fails the Silver run loudly instead of silently drifting the shared column. `specific_diagnosis` is deliberately left uncanonicalized — free text by design; a crosswalk across datasets is deferred until a real second dataset shows what the actual mapping problem looks like, rather than speculatively designing against taxonomies that don't exist yet.

Verified end-to-end on Databricks under the current streaming-validation design for both `isic_2019` and `milk10k`: `validate_images` streams candidate images directly from their source archives (`docs/decisions/006-stream-archives-no-blob-storage.md`) rather than joining a Bronze blob table. An earlier run under the old blob-table design surfaced and fixed a real bug — `.cache()`/`.persist()` throughout `spark_io.py` is unsupported on Databricks serverless compute; see `AGENT.md`'s "Databricks serverless compute" section and `data_platform.spark_io.materialize()`. See `docs/decisions/003-silver-label-columns-not-map.md` for the label schema design.

## Gold

- 🟨 Publish the versioned classification manifest — `sample-v1` release (~100 images from `isic_2019` and `milk10k` each, `malignancy` label only), defined by `config/gold/manifests/sample-v1.yaml` and built by `notebooks/30_create_gold_manifest.ipynb` (`data_platform.sampling`, the Gold pipeline functions in `data_platform.spark_io`), to iterate on the Gold pipeline cheaply, not as the real release. Each manifest is its own config file — same convention as `config/bronze/datasets/<dataset>.yaml` — so a new manifest is a new YAML file, not a new notebook. Not yet run against Databricks. The full-scale release (all accepted rows, not a sample) is separate future work. **A new manifest/`dataset_version` is for a new image selection, not a new `preprocessing_version`** — Gold shard export writes raw bytes regardless of `preprocessing_version`, so a manifest created solely to vary that field and then exported would just duplicate identical shard bytes; see `docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`.
- ⬜ Freeze preprocessing configuration — `gold.manifest_rows.preprocessing_version` is populated (currently an opaque string tag, e.g. `"sample-v1"`), but there's no versioned config file backing it yet — `config/preprocessing.yaml` was removed as unused clutter (nothing loaded it, since preprocessing only happens at training runtime per `docs/decisions/001-preprocessing-at-runtime.md`, which doesn't exist yet). Build a real one (per the `gold.preprocessing.yaml` contract in `docs/data_contract.md`) once training code exists to consume it — same "don't build ahead of a real consumer" reasoning as `config/gold/training_runs/`, see `docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`.
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
