# AGENT

Last updated: 2026-09-01 (MILK10k second-dataset code and notebooks written, not yet run — blocked on archive download)

This is the canonical repo-local instruction file for project working state and conventions.
Ask the agent to reread this file after long gaps, after conversation compaction, or when project conventions change.

## Current branch

- `feature_branch_silver`

## Current focus

- Notebook-first Databricks workflow
- Bronze is done for ISIC 2019 (shared medallion setup plus end-to-end ingestion, verified on real data)
- Silver is done for ISIC 2019 (end-to-end run verified on Databricks serverless compute, see Current next step)
- Onboarding MILK10k as the second dataset (`config/datasets/milk10k.yaml`, `notebooks/milk10k/`) — code and notebooks are written, not yet run against real data (see Current next step)
- Shared layout/config code in `src/data_platform/layout.py`

## Repository shape

- `src/` contains shared Python code
- `notebooks/` contains Databricks notebooks
- `docs/` contains architecture, contracts, checklist, and supporting context

## Notebook flow

1. `notebooks/_setup_env.ipynb`
2. `notebooks/00_setup_storage.ipynb`
3. `notebooks/isic_2019/05_setup_tables_and_folders.ipynb`
4. `notebooks/isic_2019/10_bronze_ingest.ipynb`
5. `notebooks/isic_2019/20_silver_validate.ipynb`
6. `notebooks/milk10k/05_setup_tables_and_folders.ipynb`
7. `notebooks/milk10k/10_bronze_ingest.ipynb`
8. `notebooks/milk10k/20_silver_validate.ipynb`
9. `notebooks/30_data_quality_summary.ipynb`
10. `notebooks/40_baseline_results.ipynb`

MILK10k's three notebooks mirror ISIC 2019's exactly (same shape, same shared `data_platform.spark_io`/`data_platform.labels` calls) — the only real differences are `config/datasets/milk10k.yaml`, the `DIAGNOSIS_INPUT_COLUMNS` list passed to the shared `normalize_diagnosis_labels`, and the Bronze source-metadata schema/DDL (MILK10k has 4 diagnosis levels and 2 anatomic-site levels instead of ISIC 2019's 5, no `patient_id`, and one archive/no train-test split instead of two).

## Notebook organization

- Use stage-based numbering with gaps. Use `00` for global setup, `05` for dataset-specific object/folder setup (covers Bronze, Silver, and Gold DDL/folders for that dataset in one notebook), `10` for Bronze ingestion, `20` for Silver processing, `30` for Gold publishing, `40`+ for later stages.
- Group dataset-specific notebooks under a dataset folder
- Keep Bronze ingestion separate from quality and results notebooks
- Use `notebooks/_setup_env.ipynb` as the shared Databricks import/bootstrap notebook
- Use `00_setup_storage.ipynb` only for shared schemas and medallion conventions
- All Python imports go in the first or second code cell (first is `%run ../_setup_env`, second is config/imports) — never scattered into later cells, even ones that introduce a new dependency

## Shared code

- `build_layout(dataset_config)`, `resolve_archive_paths(archives, landing_paths, bronze_paths)`
- `load_dataset_config(path)`
- `data_platform.files` helpers for archive staging, image blob rows (with embedded checksums), archive extraction, image detection, and per-suffix member counting (`count_zip_members_by_suffix`, for zero-images diagnostics), plus `check_archives_exist(archives)` and `stage_archives_and_extract_metadata(archives, local_stage_root, overwrite=False)` — pure Python, unit-tested, but explicitly scoped to **archive-based ingestion** (each archive dict has `archive_local_path`/`metadata_filename`/etc. from `resolve_archive_paths`). A dataset ingested from an API or another non-archive source doesn't use these — write its own equivalents rather than forcing them to fit
- `data_platform.validate.decode_image(image_bytes, min_dimension=MIN_DIMENSION, max_dimension=MAX_DIMENSION)` — dataset-agnostic image readability and dimension-bounds check, reused by every dataset's Silver notebook. Bounds are overridable per call, defaulting to `MIN_DIMENSION`/`MAX_DIMENSION`; a dataset only overrides them if its images are a structurally different size range — see `docs/silver_validation_rules.md` for what each dataset actually uses and why. (A near-uniform-color rejection was tried and removed — not trusted as a reliable signal; readd only with real evidence it helps.)
- `data_platform.validate.decode_batch(batches)` — batches `decode_image` for Spark's `mapInPandas`; needs only `pandas`, not `pyspark`, so it's unit-tested locally like the rest of `validate.py`. Its Spark `StructType` output schema (`DECODE_OUTPUT_SCHEMA`) lives in `spark_io.py` instead, since that module already depends on `pyspark` — `validate.py` itself never has to import it
- `data_platform.labels.MALIGNANCY_VALUES` — the canonical `("benign", "malignant", "indeterminate")` vocabulary for the `malignancy` Silver label column; pure Python, no Spark dependency, unit-tested locally like `validate.py`. Enforced (not just documented) via `apply_label_normalization`'s `controlled_vocabularies` param — see the Silver pipeline bullet below. Only label axes meant to be cross-dataset comparable belong here (see `docs/decisions/003-silver-label-columns-not-map.md`); `specific_diagnosis` has no entry and isn't meant to, since it's deliberately open-ended per dataset.
  - `data_platform.labels.normalize_diagnosis_labels(*diagnosis_levels)` is the full ISIC-Archive-style diagnosis-hierarchy → `{malignancy, specific_diagnosis}` mapping, arity-agnostic (works for any number of `diagnosis_1..N` levels) — a dataset's Silver notebook passes this directly as `normalize_fn`, with its own `DIAGNOSIS_INPUT_COLUMNS`. `normalize_malignancy_prefix(diagnosis_1)` and `deepest_available(*values)` are its building blocks, also directly usable/testable on their own.
  - There used to be a per-dataset `data_platform.datasets.<dataset>.normalize_labels` wrapper module for each of isic_2019/milk10k, but once both wrappers turned out to be zero-logic (just pinning an arity and delegating), they were deleted rather than kept "for consistency" or "in case a future dataset needs one" — that's structure for a hypothetical, the same mistake avoided elsewhere in this project (see the Gold-crosswalk and label-vocabulary decisions). **If a future dataset's diagnosis signal doesn't fit `normalize_diagnosis_labels`'s shape** (a different word than benign/malignant/indeterminate, a numeric code, a differently-ordered hierarchy), that's when a real `data_platform/datasets/<dataset>.py` with actual custom logic gets created — the `data_platform/datasets/` package (with its `__init__.py`) still exists specifically as that landing spot, it's just empty until a dataset genuinely needs it.
- `data_platform.spark_io` — Bronze and Silver pipeline Spark plumbing, fully dataset-agnostic, requires a live Spark session so (unlike `files.py`/`layout.py`/`validate.py`) not unit-tested locally. Internally grouped with `# ===` section comments (low-level/shared, Bronze pipeline, Silver pipeline) since it holds both layers' functions in one file — see the file itself, not just this list, if it grows further:
  - `merge_into(spark, df, target_table, key_columns)`, `bronze_image_uri(table_name, source_split_col, image_id_col)`, `reject_rows(spark, df, target_table, dataset_key, reason, bronze_uri_col=..., description=...)`, `assert_controlled_vocabularies(df, controlled_vocabularies, context=...)` — low-level: upsert-via-MERGE, Bronze image URI format, the build-reject-row-and-MERGE-and-log pattern, and a single-aggregation check that every column named in `controlled_vocabularies` holds only values from its allowed set (raises, doesn't reject rows — see `apply_label_normalization` below for why)
  - Bronze pipeline: `write_image_blob_table(spark, archives, target_table, batch_max_bytes=..., batch_max_rows=...)` and `load_combined_metadata(spark, archives)` — like `files.py`'s two Bronze helpers, explicitly scoped to **archive-based ingestion**, say so in their docstrings. `write_ingestion_run(spark, bronze_tables, ingestion_run_id, dataset_name, source_version, started_at, finished_at, records_seen, records_written, status="success")` is the one Bronze piece that's ingestion-source-agnostic (archives, API, anything) — any future dataset still wants this one regardless of how it ingests
  - Silver pipeline: `reconcile_bronze_records(spark, bronze_tables, silver_tables, dataset_key)`, `apply_label_normalization(spark, source_metadata_df, normalize_fn, input_columns, label_columns, silver_tables, dataset_key, controlled_vocabularies=...)`, `validate_images(spark, label_valid_df, bronze_tables, silver_tables, dataset_key, min_dimension=..., max_dimension=...)`, `build_accepted_rows(label_valid_df, image_valid_df, label_columns, dataset_key)`, `assign_leakage_groups_and_write_inventory(spark, accepted_df, label_columns, silver_tables)` (reads `dataset_key` off `accepted_df`, which `build_accepted_rows` already stamped onto it; returns `(image_inventory_df, leakage_groups_df)`), `print_silver_outputs(spark, display, silver_tables, label_columns, dataset_key, image_inventory_df, leakage_groups_df)` — composed from the above; a dataset's Silver notebook supplies its `dataset_key` (from `DATASET_CONFIG["dataset_key"]`), `normalize_fn`, `input_columns`, and `label_columns`, and calls these six in sequence
  - `print_bronze_outputs(spark, dbutils, display, landing_paths, bronze_paths, bronze_tables, dataset_name)` — Bronze's own review-outputs summary, same naming convention (`print_<layer>_outputs`); `image_blobs`/`source_metadata` are already dataset-specific tables so need no filter, but `ingestion_runs` is shared across datasets so is filtered by `dataset_name` (from `DATASET_CONFIG["dataset_name"]` — note this is the *display* name like `isic/2019`, not `dataset_key`; `bronze.ingestion_runs` predates the `dataset_key` convention and was never migrated to it)
  - `spark`, `dbutils`, and `display` are always passed as explicit parameters, never assumed to be globals — all three are injected into a *notebook's* namespace by the Databricks runtime, not into arbitrary imported modules
  - `GROUP_SOURCE_BY_TYPE`/`GROUP_SOURCE_MAP`, `DECODE_OUTPUT_SCHEMA`, and `IMAGE_BLOB_SCHEMA` are module-level constants here too, since every value in them is a universal Bronze/Silver field name, not dataset-specific

### Databricks serverless compute

All Spark/SQL code in this project must run on Databricks serverless compute — assume that's the only compute type available, since that's what this project actually runs on. Concretely:

- **Never call `.cache()` / `.persist()` / `.unpersist()`.** `DataFrame.cache()` compiles to `PERSIST TABLE`, which serverless rejects outright at runtime: `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute`. This isn't a missed optimization to fix later — it's a hard failure the notebook can't get past.
- Where a DataFrame is genuinely expensive to recompute (holds raw image bytes, or is the output of a `mapInPandas` decode) **and** feeds more than one downstream action, use `data_platform.spark_io.materialize(spark, df, table_name)` instead — writes `df` to a real scratch Delta table and reads it back. `_scratch_table(silver_tables, name)` builds the scratch table name in the same catalog/schema as the real Silver tables; it's overwritten every run, nothing there is meant to persist between runs. `validate_images` is the reference example: it materializes the image-bytes join and the decoded-image output, since each feeds several downstream reads, but leaves everything else (metadata-only, single-consumer, or already reading from an already-materialized table) as plain lazy DataFrames — don't materialize something just because it's convenient, only where skipping it would mean redoing real work (a byte-heavy join or a `mapInPandas` pass) multiple times.
- Before introducing a new Spark pattern (RDD API, thread pools, a new caching mechanism, etc.), check whether Databricks serverless supports it — don't assume classic-cluster Spark idioms carry over.
  - Not every helper here applies to every future dataset — several assume archive-based ingestion and say so explicitly. Don't force an API-ingested dataset's notebook to call `write_image_blob_table`/`load_combined_metadata`/`stage_archives_and_extract_metadata`/`check_archives_exist` just for consistency; write that dataset's own ingestion path and reuse only what actually applies (`write_ingestion_run`, the Silver pipeline, `merge_into`/`bronze_image_uri`/`reject_rows`)

## Bronze contract

- Shared table: `bronze.ingestion_runs`
- `bronze.isic_2019_source_metadata` / `bronze.milk10k_source_metadata` — dataset-specific shapes, see `docs/data_contract.md` (MILK10k has fewer diagnosis/anatomic-site levels than ISIC 2019, no `patient_id`, one MILK10k-only column `image_manipulation`)
- Raw image bytes stay in `bronze.isic_2019_image_blobs` / `bronze.milk10k_image_blobs` — identical shape between datasets (`IMAGE_BLOB_SCHEMA` in `spark_io.py`)
- Raw metadata files stay in `isic_2019/metadata` / `milk10k/metadata`
- MILK10k is a single release with no train/test split — its one archive uses `source_split: all` rather than `train`/`test`; nothing in the shared code special-cases those two values, `source_split` is just a free label used for URI/grouping

## Silver contract

- Shared tables (not dataset-prefixed): `silver.image_inventory`, `silver.leakage_groups`, `silver.rejected_records` — see `docs/data_contract.md` for the full schema.
- Every shared Silver table has a `dataset_key` column identifying which dataset a row belongs to, and every `MERGE INTO` into these tables is keyed on `dataset_key` plus the table's natural key (`image_id`, or `group_id` which also embeds `dataset_key`) — never on the natural key alone, since it's only guaranteed unique within one dataset, not globally. Query/report code (`print_silver_outputs`) filters by `dataset_key` for the same reason.
- Label columns (`malignancy`, `specific_diagnosis` — populated by both ISIC 2019 and MILK10k so far) are real, named, commented columns on `silver.image_inventory`, not a `MAP` column and not a per-dataset table. A future dataset needing a different label axis adds new nullable columns via `ALTER TABLE ... ADD COLUMNS` rather than a schema redesign — see `docs/decisions/003-silver-label-columns-not-map.md` for why.
- `malignancy`'s vocabulary is enforced, not just documented: `data_platform.labels.MALIGNANCY_VALUES` is the fixed canonical set, checked by `apply_label_normalization` for every column named in its `controlled_vocabularies` argument. A `normalize_fn` producing a value outside it fails the Silver run — that's a bug in `normalize_fn` itself (every row sharing that source value would fail identically), not a per-row data-quality issue, so it isn't routed through `silver.rejected_records` like an unresolved label is. A future dataset adapts its own mapping onto this fixed set rather than the set changing to fit a new dataset. `specific_diagnosis` is deliberately not enforced this way.
- Image validation criteria (`MIN_DIMENSION`/`MAX_DIMENSION` and any future checks) can legitimately differ by dataset — a fixed bound tuned for one image domain can be wrong for another. Defaults live in `data_platform/validate.py`; what each onboarded dataset actually uses (and why) is tracked in `docs/silver_validation_rules.md` — keep that updated whenever a dataset's validation criteria are set or changed, so "how strict is dataset X" stays a lookup, not a code-reading exercise.
- Normalize before validate: compute label columns first (cheap, metadata-only) and reject unlabeled rows before decoding any image bytes; only label-valid rows get the expensive image-decode/dimension check. This ordering is a guardrail, not just an implementation detail — don't reorder it for a future dataset without a reason.
- Leakage-group priority: exact duplicate by `source_checksum` first, then `lesion_id`, then `patient_id`, otherwise a singleton group — scoped per `dataset_key`. Implemented once in `data_platform.spark_io.assign_leakage_groups_and_write_inventory` — do not reimplement this per dataset. `build_accepted_rows` fills `patient_id`/`lesion_id` with `NULL` if a dataset's Bronze metadata has no such column, so this doesn't crash for a dataset without that concept, but the `lesion_id`-then-`patient_id` priority order itself is still a fixed, dermatology-shaped assumption, not a per-dataset-configurable one — revisit this (e.g. a caller-supplied identity-column list, mirroring `label_columns`) if a future dataset needs a different leakage-grouping axis.
- A dataset's Silver notebook should be mostly configuration (its `dataset_key`, which raw columns feed label normalization, the resulting label column names, and any validation-bound overrides) calling `data_platform.spark_io`'s pipeline functions in sequence — see `notebooks/isic_2019/20_silver_validate.ipynb` or `notebooks/milk10k/20_silver_validate.ipynb` as reference shapes for a new dataset's Silver notebook; the two are nearly identical aside from `DATASET_CONFIG`/`DIAGNOSIS_INPUT_COLUMNS`. A dataset whose diagnosis hierarchy fits `data_platform.labels.normalize_diagnosis_labels`'s shape (both onboarded datasets do) just passes that function directly as `normalize_fn` — no per-dataset Python module needed; only write one if the dataset's label logic genuinely doesn't fit.

## Table strategy

- Use one catalog unless there is a real isolation requirement
- Keep shared operational tables shared, for example `bronze.ingestion_runs`
- Keep Bronze source tables dataset-specific when source schemas differ or may evolve
- Normalize across datasets only when there is a real common Silver or Gold contract

## Configuration rules

- Keep the governed shared landing and medallion roots in `config/storage.yaml`
- Load dataset config from `config/datasets/<dataset>.yaml` when the same config is shared across notebooks
- Keep dataset archive filenames and expected metadata filenames in `config/datasets/<dataset>.yaml`
- Do not redefine shared dataset config dicts inside notebooks
- Keep shared helpers in `src/data_platform/layout.py`
- Keep global operational tables (for example `bronze.ingestion_runs`) in `00_setup_storage.ipynb`
- Keep dataset-specific Bronze DDL in the dataset setup notebook (`05_setup_tables_and_folders.ipynb`), not in shared code
- Silver/Gold tables are shared (not dataset-prefixed) but their `CREATE TABLE IF NOT EXISTS` DDL lives in each dataset's `05_setup_tables_and_folders.ipynb` too, since that is where the dataset-specific object setup already happens — this is safe to run from more than one dataset's notebook because it is idempotent; do not also duplicate that DDL into `00_setup_storage.ipynb`
- Create the shared landing folder in `00_setup_storage.ipynb`; do not pre-create medallion-layer folders there — dataset setup notebooks create their own folders on demand via `build_layout`
- Keep dataset-specific folder paths in the dataset setup notebook

## Guardrails

- Notebook code may load config into a local variable, but must not become a second source of truth
- Do not duplicate setup logic across notebooks when it belongs in shared code
- Do not duplicate Bronze table DDL across notebooks
- If a convention was corrected during implementation, record the corrected rule here instead of relying on chat history

## Current next step

- Bronze is implementation-complete and verified: the end-to-end ingestion notebook has been run against the real ISIC 2019 train/test archives on Databricks.
- Rerun/merge/dedup behavior is covered by unit tests against fixture archives (`tests/test_files.py`), not by a second real-data run — re-running Bronze against the real archives is expensive, so this is deferred. A mock-data rerun test is a future automation item, not the current priority.
- Silver is implementation-complete and verified: `notebooks/isic_2019/20_silver_validate.ipynb` has been run end-to-end against real Bronze data on Databricks serverless compute, with sane accepted/rejected/leakage-group counts (32,413 accepted, 1,156 rejected for unresolvable label, 16,800 leakage-control groups — see `docs/project-checklist.md`). This run also surfaced a real bug, now fixed: `.cache()`/`.persist()` is not supported on serverless compute (see "Databricks serverless compute" below).
- Bronze's known gap — `write_image_blob_table` writes directly to the production table rather than staging-then-atomically-replacing, so a mid-run failure leaves it partially rewritten (recovery is a full rerun, accepted as a tradeoff for this project's current scale — see `docs/decisions/002-store-bronze-images-as-delta-blobs.md`) — is a documented, accepted risk, not an open task.
- MILK10k (second dataset) is implementation-complete but **not yet run**: `config/datasets/milk10k.yaml` and all three `notebooks/milk10k/*.ipynb` are written, built from the real `metadata.csv` header/sample row, but nothing has been run against Databricks yet — blocked on the archive download/extraction finishing. Onboarding MILK10k drove two rounds of promoting logic out of per-dataset modules into shared `data_platform.labels` once real evidence (not upfront assumption) showed it was genuinely reusable: first `normalize_malignancy_prefix`/`deepest_available`, then the whole `normalize_diagnosis_labels` function once both datasets' `normalize_labels` bodies turned out identical except for arity — at which point the now-zero-logic `data_platform/datasets/isic_2019.py`/`milk10k.py` wrapper modules (and their per-dataset tests) were deleted rather than kept for symmetry; both notebooks now pass `data_platform.labels.normalize_diagnosis_labels` directly as `normalize_fn`. `data_platform/datasets/` still exists as the landing spot for a future dataset whose label logic doesn't fit that shape. Next up once the download finishes: upload the archive, run `05_setup_tables_and_folders.ipynb` → `10_bronze_ingest.ipynb` → `20_silver_validate.ipynb`, and record real accepted/rejected/leakage-group counts here and in `docs/project-checklist.md`, same standard as ISIC 2019's verification.
- Otherwise: Gold (publish the classification manifest) is the next unstarted layer, per `docs/project-checklist.md`.

## Update rule

- Update this file when notebook naming, branch usage, config location, execution flow, or project guardrails change
