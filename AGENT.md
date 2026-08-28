# AGENT

Last updated: 2026-08-28 (Silver implementation added)

This is the canonical repo-local instruction file for project working state and conventions.
Ask the agent to reread this file after long gaps, after conversation compaction, or when project conventions change.

## Current branch

- `feature_branch_bronze`

## Current focus

- Notebook-first Databricks workflow
- Bronze is done (shared medallion setup plus end-to-end ISIC 2019 ingestion, verified on real data)
- Silver is implementation-complete, not yet verified on Databricks (see Current next step)
- Shared setup code in `src/data_platform/setup.py`

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
6. `notebooks/30_data_quality_summary.ipynb`
7. `notebooks/40_baseline_results.ipynb`

## Notebook organization

- Use stage-based numbering with gaps. Use `00` for global setup, `05` for dataset-specific object/folder setup (covers Bronze, Silver, and Gold DDL/folders for that dataset in one notebook), `10` for Bronze ingestion, `20` for Silver processing, `30` for Gold publishing, `40`+ for later stages.
- Group dataset-specific notebooks under a dataset folder
- Keep Bronze ingestion separate from quality and results notebooks
- Use `notebooks/_setup_env.ipynb` as the shared Databricks import/bootstrap notebook
- Use `00_setup_storage.ipynb` only for shared schemas and medallion conventions

## Shared code

- `build_layout(dataset_config)`, `resolve_archive_paths(archives, landing_paths, bronze_paths)`
- `load_dataset_config(path)`
- `data_platform.files` helpers for archive staging, image blob rows (with embedded checksums), archive extraction, image detection, and per-suffix member counting (`count_zip_members_by_suffix`, for zero-images diagnostics)
- `data_platform.validate.decode_image(image_bytes)` — dataset-agnostic image readability, dimension-bounds (`MIN_DIMENSION`/`MAX_DIMENSION`), and near-uniform-color check, reused by every dataset's Silver notebook
- `data_platform.datasets.<dataset>` — per-dataset label-mapping modules, NOT shared; each dataset gets its own (e.g. `data_platform.datasets.isic_2019.normalize_labels`)
- `data_platform.spark_io.merge_into(spark, df, target_table, key_columns)` and `data_platform.spark_io.bronze_image_uri(table_name, source_split_col, image_id_col)` — generic Spark plumbing (upsert-via-MERGE, Bronze image URI format) shared across Bronze/Silver notebooks; requires a live Spark session, so unlike `files.py`/`setup.py`/`validate.py` this module is not unit-tested locally

## Bronze contract

- Shared table: `bronze.ingestion_runs`
- `bronze.isic_2019_source_metadata`
- Raw image bytes stay in `bronze.isic_2019_image_blobs`
- Raw metadata files stay in `isic_2019/metadata`

## Silver contract

- Shared tables (not dataset-prefixed): `silver.image_inventory`, `silver.leakage_groups`, `silver.rejected_records` — see `docs/data_contract.md` for the full schema.
- Label columns (`malignancy`, `specific_diagnosis` for ISIC 2019) are real, named, commented columns on `silver.image_inventory`, not a `MAP` column and not a per-dataset table. A future dataset needing a different label axis adds new nullable columns via `ALTER TABLE ... ADD COLUMNS` rather than a schema redesign — see `docs/decisions/003-silver-label-columns-not-map.md` for why.
- Normalize before validate: compute label columns first (cheap, metadata-only) and reject unlabeled rows before decoding any image bytes; only label-valid rows get the expensive image-decode/dimension check. This ordering is a guardrail, not just an implementation detail — don't reorder it for a future dataset without a reason.
- Leakage-group priority: exact duplicate by `source_checksum` first, then `lesion_id`, then `patient_id`, otherwise a singleton group.

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
- Keep shared helpers in `src/data_platform/setup.py`
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
- Silver is implementation-complete but not yet run: `notebooks/isic_2019/20_silver_validate.ipynb`, `data_platform.datasets.isic_2019.normalize_labels`, and `data_platform.validate.decode_image` are written and unit-tested locally (`tests/datasets/test_isic_2019.py`, `tests/test_validate.py`), but the notebook itself needs a real run on Databricks against real Bronze data before Silver can be marked done in `docs/project-checklist.md`.
- Next up: run `20_silver_validate.ipynb` on Databricks, verify accepted/rejected counts and leakage groups look right, then mark Silver checklist items done.

## Update rule

- Update this file when notebook naming, branch usage, config location, execution flow, or project guardrails change
