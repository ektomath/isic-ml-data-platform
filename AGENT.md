# AGENT

Last updated: 2026-09-03. Bronze and Silver have been rerun and verified against real Databricks
data (ISIC 2019 + MILK10k) under the archive-streaming design (`docs/decisions/006-stream-archives-no-blob-storage.md`).
Gold (`sample-v1` manifest + shard export) has not been run yet — see "Current next step." Baseline
training (`notebooks/40_train_baseline_classifier.ipynb`, `src/ml/`) is implemented — a PyTorch/torchvision
ResNet-18 classifier, trainable identically locally or on Databricks, logging to MLflow and syncing
provenance into `gold.training_run_registry` (`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`,
`docs/decisions/011-baseline-training-framework-and-registry-sync.md`) — but not yet run against
Databricks, since Gold hasn't published a manifest to train against yet.

This is the canonical repo-local instruction file for project working state and conventions.
Ask the agent to reread this file after long gaps, after conversation compaction, or when project conventions change.

## Current branch

- `feature_branch_silver`

## Current focus

- Notebook-first Databricks workflow.
- Bronze and Silver are implemented and verified for both onboarded datasets (`isic_2019`, `milk10k`) under the current archive-streaming design.
- Gold (`notebooks/30_create_gold_manifest.ipynb`, `notebooks/31_export_gold_shards.ipynb`) is implementation-complete but not yet run — see "Current next step."
- Baseline training (`notebooks/40_train_baseline_classifier.ipynb`, `src/ml/train.py`'s `run_training`) is implementation-complete, unit-tested (`tests/test_ml_*.py`, including a real end-to-end wiring smoke test), but not yet run against Databricks — depends on Gold having published a manifest and export first.

## Repository shape

- `src/` contains shared Python code
- `notebooks/` contains Databricks notebooks
- `docs/` contains architecture, contracts, checklist, and supporting context

## Notebook flow

1. `notebooks/_setup_env.ipynb`
2. `notebooks/00_setup_storage_and_shared_tables.ipynb`
3. `notebooks/isic_2019/05_setup_tables_and_folders.ipynb`
4. `notebooks/isic_2019/10_bronze_ingest.ipynb`
5. `notebooks/isic_2019/20_silver_validate.ipynb`
6. `notebooks/milk10k/05_setup_tables_and_folders.ipynb`
7. `notebooks/milk10k/10_bronze_ingest.ipynb`
8. `notebooks/milk10k/20_silver_validate.ipynb`
9. `notebooks/30_create_gold_manifest.ipynb`
10. `notebooks/31_export_gold_shards.ipynb`
11. `notebooks/40_train_baseline_classifier.ipynb`

MILK10k's three notebooks mirror ISIC 2019's exactly — the only real differences are `config/bronze/datasets/milk10k.yaml` and the Bronze source-metadata schema/DDL (MILK10k has 4 diagnosis levels and 2 anatomic-site levels instead of ISIC 2019's 5, no `patient_id`, one archive/no train-test split).

`30_create_gold_manifest.ipynb` and `31_export_gold_shards.ipynb` are top-level (not under a dataset folder), unlike Bronze/Silver notebooks — a Gold release spans every included dataset in one run. `30` publishes `gold.manifest_rows` for a `dataset_version` (currently just `sample-v1`, ~100 images per dataset, built to iterate on the Gold pipeline cheaply — see `docs/project-checklist.md`). `31` reads an already-published `dataset_version` and packs it into a derived, per-split MosaicML shard export (`config/gold/exports/<name>.yaml`) — no table of its own, a Volume artifact fully rebuildable from the manifest and archives.

`40_train_baseline_classifier.ipynb` is also top-level, same reasoning — training reads a `31` export, not a per-dataset artifact. It trains a baseline classifier against one `config/gold/training_runs/<name>.yaml` (currently just `sample-v1-resnet18`), delegating everything training-specific to `src/ml/train.py`'s `run_training` (identical whether called from this notebook or a local `python -m ml.train` invocation), then syncs the resulting MLflow run's provenance into `gold.training_run_registry`. See the ML/Training contract section below and `docs/decisions/010`/`011`.

## Notebook organization

- Use stage-based numbering with gaps. Use `00` for global setup (shared schemas, shared tables — `bronze.ingestion_runs`, all of Silver, all of Gold), `05` for dataset-specific object/folder setup (Bronze DDL/folders for that dataset only — neither Silver nor Gold, both entirely `00`'s job), `10` for Bronze ingestion, `20` for Silver processing, `30` for Gold manifest publishing, `31` for the Gold shard export (a distinct, later sub-stage of Gold that reads an already-published manifest — the gap between `30` and `40` is exactly for insertions like this), `40`+ for later stages.
- Group dataset-specific notebooks under a dataset folder; a notebook whose single run spans every included dataset (Gold publishing) stays top-level instead
- Keep Bronze ingestion separate from quality and results notebooks
- Use `notebooks/_setup_env.ipynb` as the shared Databricks import/bootstrap notebook — it also sets `CATALOG_NAME` and runs `USE CATALOG` itself (idempotent), rather than relying on `00_setup_storage_and_shared_tables.ipynb` having run earlier in the same session. `USE CATALOG` is session-scoped, not persistent across notebook runs or a fresh session attach — every notebook re-issuing it here (via `%run ./_setup_env`) is what makes an unqualified table name like `gold.manifest_rows` resolve correctly regardless of session history. This was a real bug found in this project: notebook 30 hit `TABLE_OR_VIEW_NOT_FOUND` after a session reset because only `00` used to set the catalog.
- `00_setup_storage_and_shared_tables.ipynb` stays the one place that runs `CREATE CATALOG IF NOT EXISTS` — `_setup_env.ipynb` only selects the catalog (`USE CATALOG`), it doesn't create it. Works on a fresh workspace too: `_setup_env`'s `USE CATALOG` is wrapped in a try/except that prints a warning and continues if the catalog doesn't exist yet, rather than failing the notebook outright — so `00` can still reach its own cell, which creates the catalog and re-selects it before creating schemas/volumes. Every other notebook assumes `00` has already run at least once.
- Use `00_setup_storage_and_shared_tables.ipynb` for shared schemas, medallion conventions, and every shared table (`bronze.ingestion_runs`, all of Silver, all of Gold) — anything with no dataset-specific parameter at all belongs here, its DDL written directly in the notebook (this notebook's whole purpose is to show what gets created) rather than duplicated across dataset setup notebooks
- All Python imports go in the first or second code cell (first is `%run ../_setup_env`, second is config/imports) — never scattered into later cells, even ones that introduce a new dependency
- `31_export_gold_shards.ipynb` needs `mosaicml-streaming==0.13.0` (pinned in `pyproject.toml` — keep both in sync if this ever gets bumped; not preinstalled on Databricks, and not picked up from `pyproject.toml` automatically, since that file only governs local `uv sync`, not a notebook's own Python environment). Declared as a dependency on that notebook's serverless Environment (the notebook UI's environment/session panel), not installed via a `%pip install` cell — avoids a slow reinstall on every fresh serverless session. A `%pip install mosaicml-streaming==0.13.0` + `dbutils.library.restartPython()` cell (before `%run ./_setup_env`, since restart wipes Python process state) is the fallback if the Environment approach isn't available in a given workspace.

## Shared code

Full schemas/contracts live in `docs/data_contract.md`; this section is what each module does and why it's organized that way, not a schema reference.

- `data_platform.dataset_layout` — `build_layout(dataset_config)` (returns `landing_paths`/`bronze_paths`/`bronze_tables`/`silver_tables` for one dataset; deliberately no Gold fields — a Gold release spans multiple datasets, so `30`/`31` build their own small `GOLD_TABLES` dict inline instead), `resolve_archive_paths`, `load_dataset_config`, `load_yaml_config`. Pure Python, no Spark — but unlike `files.py`/`validate.py`/`sampling.py`/`labels.py`, no `tests/test_dataset_layout.py` yet; a real gap (see `docs/project-checklist.md`'s Automation section).
- `data_platform.files` — archive staging, `iter_archive_image_rows` (the one shared low-level primitive every archive-streaming consumer uses — Bronze index build, Silver validation, Gold shard export — each deciding how long to keep the bytes; takes an optional `member_predicate` to skip members a caller doesn't want), `format_bronze_uri`/`parse_bronze_uri` (build/parse the `archive:<uri>#<path>` pointer — the only way to locate an image's bytes, see `docs/decisions/006-stream-archives-no-blob-storage.md`), `iter_archive_matches` (shared archive-matching + checksum-verification generator used by both Silver validation and Gold shard export), plus archive extraction/detection/counting helpers. Pure Python, unit-tested, scoped to **archive-based ingestion** — a non-archive-sourced dataset needs its own equivalents.
- `data_platform.validate.decode_image`/`decode_batch` — dataset-agnostic image readability/dimension check, reused by every dataset's Silver notebook; bounds overridable per call (see `docs/silver_validation_rules.md` for what each dataset uses). `decode_batch` needs only `pandas`, not `pyspark` — its Spark output schema (`DECODE_OUTPUT_SCHEMA`) lives in `spark_io.py` instead.
- `data_platform.labels.MALIGNANCY_VALUES` — canonical `malignancy` vocabulary, enforced (not just documented) via `apply_label_normalization`'s `controlled_vocabularies` param. `normalize_diagnosis_labels(*diagnosis_levels)` is the arity-agnostic ISIC-Archive-style diagnosis-hierarchy mapping both onboarded datasets pass directly as `normalize_fn` — no per-dataset wrapper module exists since both turned out to need zero dataset-specific logic (the wrappers were deleted once that was clear, rather than kept for symmetry). `data_platform/datasets/` still exists as the landing spot for a future dataset whose label logic doesn't fit this shape.
- `data_platform.sampling.select_sample_and_splits` (renamed from `gold.py`, pure Python, `tests/test_sampling.py`) — takes small driver-side per-leakage-group summaries and returns `{group_id: split}`, working in whole groups only so a group can never be split across splits or partially sampled. `sample_seed`/`split_seed` are separate on purpose — `sample_seed` alone determines which images are in the manifest, `split_seed` alone how they're divided, so a caller can hold one fixed while varying the other.
- `data_platform.spark_io` — Bronze/Silver/Gold/Gold-export Spark plumbing, requires a live Spark session (not unit-tested locally, unlike the pure-Python modules above). One file, internally grouped with `# ===` section comments — briefly split into a `spark_io/` package and merged back (see the file's own docstring for why); `shard_export.py` stayed split out, since it needs no Spark session at all. Function groups, in call order per pipeline:
  - **Shared**: `merge_into`, `bronze_image_uri` (Spark-Column form of `format_bronze_uri`), `reject_rows`, `assert_controlled_vocabularies`, `materialize` (see "Databricks serverless compute" below).
  - **Bronze**: `write_image_index_table` (streams archives, writes checksum/locator rows only — never `image_bytes`), `load_combined_metadata`, `write_ingestion_run`, `print_bronze_outputs`.
  - **Silver**: `reconcile_bronze_records`, `apply_label_normalization`, `validate_images` (streams each label-valid candidate directly from its source archive via `iter_archive_matches`, decodes, discards bytes; raises immediately — fails the whole run — on any checksum mismatch against Bronze's recorded value, see `docs/decisions/008-immutable-source-archives-checksum-verified.md`), `build_accepted_rows`, `assign_leakage_groups_and_write_inventory`, `print_silver_outputs`.
  - **Gold**: `build_gold_candidate_groups`, `assert_no_cross_dataset_duplicate_checksums` (raises if the same `source_checksum` appears under more than one `dataset_key` among the datasets a manifest combines — a no-op for a single-dataset manifest; call before sampling, not after), `write_gold_manifest_rows`, `print_gold_outputs` (also asserts no `group_id` spans more than one `split`).
  - **Gold export**: `load_manifest_rows_for_export`, `print_export_outputs`, `remove_expired_exports` (general-purpose, not called by default — retention is undecided, `docs/decisions/009-gold-shard-retention-undecided.md`).
  - **Training registry**: `write_training_run_registry_rows(spark, gold_tables, rows)` — `MERGE INTO gold.training_run_registry` keyed on `mlflow_run_id`, called on demand from `40_train_baseline_classifier.ipynb`; never imports `mlflow` itself, see `ml.registry_sync` below.
  - `spark`, `dbutils`, `display` are always explicit parameters, never assumed globals.
- `data_platform.shard_export.write_gold_shards_for_splits(rows_by_split, staged_path_by_archive_uri, shard_dir_by_split, size_limit_bytes)` — streams every split's images in one pass (a source archive commonly feeds more than one split) via `iter_archive_matches`, writing matches into one `streaming.MDSWriter(..., exist_ok=True)` per split. Pure Python (no `spark`/`dbutils` param), unit-tested locally with real fixture archives (`tests/test_shard_export.py`). Kept out of `files.py` too — `mosaicml-streaming` is a heavy dependency (pulls in `torch`/`torchvision`/`transformers`) only Gold export needs.
- `src/ml/` — the baseline-training package, sibling to `data_platform`, all pure Python plus `torch`/`torchvision`/`mlflow`/`scikit-learn` (no Spark import anywhere in this package). See the ML/Training contract section below for the design; module map:
  - `ml.preprocessing` — `load_preprocessing_config`/`build_transforms(preprocessing_config, split)`, turning a `config/preprocessing/<name>.yaml` into a `torchvision.transforms.Compose` (train gets augmentation, everything else doesn't). Unit-tested (`tests/test_ml_preprocessing.py`) against a real in-memory PIL image.
  - `ml.training_run` — `TrainingRunSpec`/`resolve_training_run(config_root, training_run_name)`, the single place a `config/gold/training_runs/<name>.yaml` name turns into everything training needs (loads + validates both that config and its referenced preprocessing config). Unit-tested (`tests/test_ml_training_run.py`) against real small YAML fixtures.
  - `ml.dataset` — `GoldShardDataset` (subclasses `streaming.StreamingDataset`, decodes each sample's bytes via PIL, applies the split's transform, maps its label through the pinned `label_values` vocabulary — raises `KeyError` on an out-of-vocabulary label rather than silently coercing), `build_dataloader`. Unit-tested (`tests/test_ml_dataset.py`) against real tiny MDS shards, same fixture convention as `tests/test_shard_export.py` (including the Windows drive-letter workaround — `StreamingDataset` shares `MDSWriter`'s local-path quirk).
  - `ml.metrics.compute_classification_metrics(y_true, y_pred, label_values)` — thin `scikit-learn` wrapper (balanced accuracy, per-class recall, confusion matrix, classification report), the original plan's ML-001 acceptance metrics. Unit-tested (`tests/test_ml_metrics.py`) against small hand-verified arrays.
  - `ml.mlflow_utils` — `configure_mlflow_tracking(experiment_name, tracking_uri=None)` (no-op on Databricks; `mlflow.set_tracking_uri("databricks")` off it — credentials resolved by `databricks-sdk` from `~/.databrickscfg`/env vars, this project's own code never handles them), `running_on_databricks()`, `current_git_commit()` (best-effort, never raises).
  - `ml.registry_sync.list_training_run_rows(mlflow_client, experiment_name)` — queries MLflow (no Spark), returns rows carrying this project's own `training_run_name`/`dataset_version`/`preprocessing_version` tags (skips anything missing them). Unit-tested (`tests/test_ml_registry_sync.py`) against a real local MLflow tracking store.
  - `ml.train` — the entrypoint. `run_training(training_run_name, config_root=None, shards_root=None, mlflow_experiment=None, mlflow_tracking_uri=None, device=None, num_workers=0) -> str` (MLflow `run_id`) is the one orchestrator both `40_train_baseline_classifier.ipynb` and a local `python -m ml.train --training-run-name <name> --shards-root <dir>` CLI call. Unit-tested end-to-end (`tests/test_ml_train_smoke.py`) against real tiny shards + a real local MLflow store + `pretrained=False` (no network download in tests).

### Databricks serverless compute

All Spark/SQL code in this project must run on Databricks serverless compute. Concretely:

- **Never call `.cache()` / `.persist()` / `.unpersist()`.** `DataFrame.cache()` compiles to `PERSIST TABLE`, which serverless rejects outright at runtime — a hard failure, not a missed optimization.
- Where a DataFrame is genuinely expensive to recompute **and** feeds more than one downstream action, use `data_platform.spark_io.materialize(spark, df, table_name)` — writes to a scratch Delta table and reads it back. `validate_images` is the reference example (materializes the image-bytes join and the decoded-image output). Don't materialize something just because it's convenient.
- Before introducing a new Spark pattern (RDD API, thread pools, a new caching mechanism), check it's actually serverless-supported first — don't assume classic-cluster idioms carry over.
- Several `spark_io`/`files` helpers assume archive-based ingestion and say so in their docstrings. Don't force an API-ingested dataset's notebook to call them for consistency — write that dataset's own ingestion path and reuse only what applies.

## Bronze contract

- Shared table: `bronze.ingestion_runs`. Dataset-specific: `bronze.<dataset>_source_metadata`, `bronze.<dataset>_image_index` (checksum/byte_length/archive locator only — no image bytes, ever; see `docs/data_contract.md` for the exact schema and the Guardrails section below for why).
- MILK10k is a single release with no train/test split — its one archive uses `source_split: all` rather than `train`/`test`; nothing in shared code special-cases those two values, `source_split` is just a free label.

## Silver contract

- Shared tables (not dataset-prefixed): `silver.image_inventory`, `silver.leakage_groups`, `silver.rejected_records` — see `docs/data_contract.md` for schema. Every one has a `dataset_key` column, and every `MERGE INTO`/query filters or keys on it — a natural key (`image_id`, `group_id`) is only unique within one dataset, not globally.
- Label columns (`malignancy`, `specific_diagnosis`) are real, named columns on `silver.image_inventory`, not a `MAP` — see `docs/decisions/003-silver-label-columns-not-map.md`. `malignancy`'s vocabulary is enforced via `apply_label_normalization`'s `controlled_vocabularies`; a value outside it fails the whole run (a bug in `normalize_fn` itself, not per-row data variance), unlike an unresolved label, which is rejected per-row.
- Validation criteria (`MIN_DIMENSION`/`MAX_DIMENSION`) can legitimately differ by dataset — defaults live in `data_platform/validate.py`, actual per-dataset values and why are tracked in `docs/silver_validation_rules.md`; keep that updated when they change.
- Normalize before validate: compute label columns first (cheap) and reject unlabeled rows before decoding any image bytes. This ordering is a guardrail, not incidental — don't reorder without a reason.
- Leakage-group priority: exact duplicate by `source_checksum`, then `lesion_id`, then `patient_id`, otherwise singleton — scoped per `dataset_key`, implemented once in `assign_leakage_groups_and_write_inventory`. Groups themselves are never merged across `dataset_key`s (unsound for `lesion_id`/`patient_id`, since they're dataset-issued IDs, not a shared namespace) — but a separate check, `assert_no_cross_dataset_duplicate_checksums` (Gold contract below), does catch the one sound cross-dataset signal (`source_checksum` equality) before sampling. See `docs/decisions/004-cross-dataset-leakage-not-checked.md`.
- A dataset's Silver notebook is mostly configuration (its `dataset_key`, label input/output columns, validation overrides, its own staged `archives` list) calling `spark_io`'s pipeline functions in sequence — see either onboarded dataset's `20_silver_validate.ipynb` as the reference shape.

## Gold contract

- Shared table: `gold.manifest_rows` — row-level, one row per image per release, versioned by `dataset_version` (multiple releases coexist; `MERGE INTO` is keyed on `(dataset_version, dataset_key, image_id)`). `gold.manifest_registry` is a **view**, not a table, over it — fully derivable, so it can't drift out of sync.
- Each manifest is its own config file, `config/gold/manifests/<name>.yaml` (same convention as `config/bronze/datasets/<dataset>.yaml`) — a new manifest is a new YAML file, not a new notebook.
- Sampling/splitting is leakage-aware by construction (`data_platform.sampling` works in whole groups), and `print_gold_outputs` still asserts it as a real invariant rather than only trusting the construction. `assert_no_cross_dataset_duplicate_checksums` covers the cross-dataset case that whole-group construction alone can't — the same real image existing under two different `dataset_key`s, which is the reason `sample-v1` (already `[isic_2019, milk10k]`) needs this before it ever gets sampled.
- `preprocessing_version` is an opaque string tag today — no config file backs it, and Gold shard export writes raw bytes regardless of its value. Never fork a `dataset_version`/manifest solely to vary this field and then export shards for both — that duplicates identical bytes for nothing. A new `dataset_version` is for a new image selection; see `docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`.
- `gold.dataset_card` is deferred, not silently dropped — not needed for a small iteration sample, build for the full-scale release.

## ML/Training contract

- `config/preprocessing/<name>.yaml` — a named preprocessing recipe (image size, normalization, resize/crop policy, augmentation, random seed), decoupled from any one `dataset_version` so multiple training runs can reuse it. Matches the `gold.preprocessing.yaml` contract in `docs/data_contract.md`. Never applied to stored bytes — resolved into `torchvision` transforms at training/inference load time only (`docs/decisions/001-preprocessing-at-runtime.md`).
- `config/gold/training_runs/<name>.yaml` — pins exactly one `dataset_version` to exactly one `preprocessing_version`, plus `label_values` (fixes the class-index mapping deterministically, not derived from a shard scan) and full hyperparameters (`architecture`/`batch_size`/`num_epochs`/`learning_rate`/`optimizer`). `ml.train`'s entrypoint accepts only this one name — never free-standing `dataset_version`/`preprocessing_version` parameters — so there's no code path to train against an unpinned pairing. See `docs/decisions/010-pin-data-and-preprocessing-per-training-run.md` and `docs/decisions/011-baseline-training-framework-and-registry-sync.md`.
- `gold.training_run_registry` — one row per actual MLflow training run (`mlflow_run_id`, `training_run_name`, `dataset_version`, `preprocessing_version`, `mlflow_experiment_id`, `created_at`), same audit-trail pattern as `bronze.ingestion_runs`/`gold.manifest_registry`. Populated *from* MLflow runs by `data_platform.spark_io.write_training_run_registry_rows`, called on demand from `40_train_baseline_classifier.ipynb`'s sync cell — never written to directly by local training code, since a local run has no Spark session. Running the training cell without the sync cell leaves that run invisible to the registry until someone runs it later — accepted, same "exists, not automatic" posture as `remove_expired_exports`.
- MLflow is the local/Databricks bridge, not the source of truth for data identity: `ml.mlflow_utils.configure_mlflow_tracking` is a no-op tracking-URI-wise on Databricks (the runtime already configures one); off Databricks it calls `mlflow.set_tracking_uri("databricks")`, resolving credentials from `~/.databrickscfg` or `DATABRICKS_HOST`/`DATABRICKS_TOKEN` via `databricks-sdk` (an `mlflow-skinny` dependency) — this project's own code never reads or handles those credentials. One shared, project-wide experiment (`ml.mlflow_utils.DEFAULT_MLFLOW_EXPERIMENT`, overridable per training-run config), not one per release — the `training_run_name`/`dataset_version`/`preprocessing_version` **tags** (not params) set on every run give the per-run filtering axis within it.
- `mlflow-skinny`, not full `mlflow` — this project only ever talks to the Databricks-hosted tracking server over REST, never runs its own tracking server. Two version-specific quirks worth knowing (both already worked around in `src/ml/train.py` and `tests/test_ml_*.py`): (1) it deprecated the plain `file://` tracking backend by default ("maintenance mode") — `MLFLOW_ALLOW_FILE_STORE=true` opts back in for local/test use, since `sqlite:///...` (the suggested replacement) needs `sqlalchemy`, part of full `mlflow`, not `mlflow-skinny`; (2) `mlflow.pytorch.log_model`'s default `serialization_format` is now `"pt2"` (a traced-graph format requiring a real `input_example`) — `run_training` passes `serialization_format="pickle"` explicitly instead, simpler and needs no example input.
- Local vs Databricks shard consumption: `ml.train.resolve_shards_root` takes an explicit `--shards-root` override (a `databricks fs cp -r`'d local directory, per `docs/data_contract.md`'s Gold shard export section); if omitted, defaults to the same `<storage_root>/gold/<dataset_version>/shards` path `31_export_gold_shards.ipynb` already writes to — resolves for free on Databricks (mounted Volume). No environment auto-detection beyond this.
- `run_training` is ordered deliberately to fail fast: `ml.train.check_shards_exist` checks every split's `index.json` up front (a clear `FileNotFoundError` naming which splits are missing, rather than a cryptic `StreamingDataset` error deep inside dataloader construction if `31` hasn't run yet or `--shards-root` is wrong), then `configure_mlflow_tracking` runs (so an MLflow permission problem surfaces before the expensive part), and only then does it build the three dataloaders (real I/O — each reads a shard index).
- If `device` isn't explicitly passed and no CUDA device is found, `run_training` prints a loud warning rather than silently training on CPU. This matters concretely: a plain `pip install torch`/`uv add torch` (no CUDA-specific index) resolves to a CPU-only wheel by default — verified locally (`torch.version.cuda` is `None` on this pinned version) — so a GPU-cluster run could silently fall back to CPU with zero error if the Environment installs torch the same naive way. Check `torch.cuda.is_available()`/`torch.version.cuda` directly on the target compute before trusting a real run's timing.
- Baseline architecture: PyTorch + torchvision `resnet18` (`ResNet18_Weights.IMAGENET1K_V1`, `fc` replaced for the run's label count) — `pretrained: true` needs outbound network access to download ImageNet weights on first use; confirm the target compute's network policy before relying on it for a real run.

## Table strategy

- Use one catalog unless there is a real isolation requirement
- Keep shared operational tables shared, for example `bronze.ingestion_runs`
- Keep Bronze source tables dataset-specific when source schemas differ or may evolve
- Normalize across datasets only when there is a real common Silver or Gold contract

## Configuration rules

- See the top-level `README.md`'s repository-contents section for the directory-level map.
- Keep the governed shared landing/medallion roots in `config/storage.yaml`; dataset archive filenames/metadata filenames in `config/bronze/datasets/<dataset>.yaml`; each Gold manifest's params in `config/gold/manifests/<name>.yaml`; each Gold export's params in `config/gold/exports/<name>.yaml` (`dataset_keys`/`preprocessing_version` deliberately not repeated there — `31` reads them from `gold.manifest_registry` at run time).
- Do not redefine shared dataset config dicts inside notebooks.
- Silver and Gold tables' DDL is written directly, inline, in `00_setup_storage_and_shared_tables.ipynb` — not behind a `spark_io` wrapper function, not in each dataset's `05_setup_tables_and_folders.ipynb`. (Corrected twice during implementation — first from per-dataset notebooks, then from a `spark_io` wrapper function — since this notebook's whole purpose is to show what gets created; recorded here rather than relying on chat history.)
- Create the shared landing folder in `00_setup_storage_and_shared_tables.ipynb`; dataset setup notebooks create their own folders on demand via `build_layout`.

## Guardrails

- Notebook code may load config into a local variable, but must not become a second source of truth.
- Do not duplicate setup logic or Bronze table DDL across notebooks.
- **Never store image bytes in a table, at any layer, and never extract an individual image to a Volume as its own object.** The landing-volume source archives are the sole permanent store; every layer that needs actual bytes streams them from the archive instead (`docs/decisions/006-stream-archives-no-blob-storage.md`). The one scoped exception is the Gold shard export — a derived, fully rebuildable cache, never a second source of truth; how long it's kept around is a separate, undecided question (`docs/decisions/009-gold-shard-retention-undecided.md`).
- More generally: prefer streaming/on-demand computation over persisting a new copy of data, at any layer. When persisting is genuinely unavoidable, persist as late as possible and treat it as derived/rebuildable unless there's a concrete, current reason it must be authoritative. See `docs/decisions/007-defer-disk-writes-until-unavoidable.md`.
- Source archives must never change after Bronze ingestion. Checked, not just assumed: `validate_images` and `write_gold_shards_for_splits` both compare each freshly-streamed image's checksum against its recorded `source_checksum` and raise immediately on any mismatch (`docs/decisions/008-immutable-source-archives-checksum-verified.md`). A genuine source update needs a new `source_version` and a fresh ingestion run, never an in-place archive edit.
- If a convention was corrected during implementation, record the corrected rule here instead of relying on chat history.

## Current next step

Bronze and Silver are done — both onboarded datasets (`isic_2019`, `milk10k`) have been rerun and verified against real Databricks data under the current archive-streaming design. What's left, in order:

1. Run `notebooks/30_create_gold_manifest.ipynb` for the `sample-v1` release against the freshly-verified Silver data. Confirm the leakage invariant holds (`print_gold_outputs`) and that reruns are idempotent (same `manifest_row_hash`, not duplicate rows).
2. Run `notebooks/31_export_gold_shards.ipynb` for the same release. New core dependency: `mosaicml-streaming` (pulls in `torch`/`torchvision`/`transformers` transitively — accepted cost of using `MDSWriter`/`StreamingDataset` rather than a hand-rolled format).
3. Run `notebooks/40_train_baseline_classifier.ipynb` for `sample-v1-resnet18` — implementation-complete and unit-tested (see the ML/Training contract section above), but this is its first real run. Confirm the training cell completes, MLflow shows the run with correct tags/params/metrics/artifacts, and the sync cell populates `gold.training_run_registry`. That notebook's serverless Environment needs `torch`/`torchvision`/`mlflow-skinny`/`scikit-learn` added (same mechanism as `mosaicml-streaming` on `31`).
4. Record actual accepted/rejected/leakage-group counts, manifest/shard counts, and baseline classifier metrics (balanced accuracy, per-class recall) here and in `docs/project-checklist.md` once all three have run.

Everything else deferred (full-scale Gold release, dataset card, cross-dataset dedup) is tracked in `docs/project-checklist.md`, not here.

## Update rule

- Update this file when notebook naming, branch usage, config location, execution flow, or project guardrails change
