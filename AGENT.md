# AGENT

Last updated: 2026-09-03. Bronze and Silver have been rerun and verified against real Databricks
data (ISIC 2019 + MILK10k) under the archive-streaming design (`docs/decisions/006-stream-archives-no-blob-storage.md`).
Gold (`sample-v1` manifest + shard export) has not been run yet — see "Current next step."

This is the canonical repo-local instruction file for project working state and conventions.
Ask the agent to reread this file after long gaps, after conversation compaction, or when project conventions change.

## Current branch

- `feature_branch_silver`

## Current focus

- Notebook-first Databricks workflow.
- Bronze and Silver are implemented and verified for both onboarded datasets (`isic_2019`, `milk10k`) under the current archive-streaming design.
- Gold (`notebooks/30_create_gold_manifest.ipynb`, `notebooks/31_export_gold_shards.ipynb`) is implementation-complete but not yet run — see "Current next step."

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
11. `notebooks/40_baseline_results.ipynb`

MILK10k's three notebooks mirror ISIC 2019's exactly — the only real differences are `config/bronze/datasets/milk10k.yaml` and the Bronze source-metadata schema/DDL (MILK10k has 4 diagnosis levels and 2 anatomic-site levels instead of ISIC 2019's 5, no `patient_id`, one archive/no train-test split).

`30_create_gold_manifest.ipynb` and `31_export_gold_shards.ipynb` are top-level (not under a dataset folder), unlike Bronze/Silver notebooks — a Gold release spans every included dataset in one run. `30` publishes `gold.manifest_rows` for a `dataset_version` (currently just `sample-v1`, ~100 images per dataset, built to iterate on the Gold pipeline cheaply — see `docs/project-checklist.md`). `31` reads an already-published `dataset_version` and packs it into a derived, per-split MosaicML shard export (`config/gold/exports/<name>.yaml`) — no table of its own, a Volume artifact fully rebuildable from the manifest and archives.

## Notebook organization

- Use stage-based numbering with gaps. Use `00` for global setup (shared schemas, shared tables — `bronze.ingestion_runs`, all of Silver, all of Gold), `05` for dataset-specific object/folder setup (Bronze DDL/folders for that dataset only — neither Silver nor Gold, both entirely `00`'s job), `10` for Bronze ingestion, `20` for Silver processing, `30` for Gold manifest publishing, `31` for the Gold shard export (a distinct, later sub-stage of Gold that reads an already-published manifest — the gap between `30` and `40` is exactly for insertions like this), `40`+ for later stages.
- Group dataset-specific notebooks under a dataset folder; a notebook whose single run spans every included dataset (Gold publishing) stays top-level instead
- Keep Bronze ingestion separate from quality and results notebooks
- Use `notebooks/_setup_env.ipynb` as the shared Databricks import/bootstrap notebook — it also sets `CATALOG_NAME` and runs `USE CATALOG` itself (idempotent), rather than relying on `00_setup_storage_and_shared_tables.ipynb` having run earlier in the same session. `USE CATALOG` is session-scoped, not persistent across notebook runs or a fresh session attach — every notebook re-issuing it here (via `%run ./_setup_env`) is what makes an unqualified table name like `gold.manifest_rows` resolve correctly regardless of session history. This was a real bug found in this project: notebook 30 hit `TABLE_OR_VIEW_NOT_FOUND` after a session reset because only `00` used to set the catalog.
- `00_setup_storage_and_shared_tables.ipynb` stays the one place that runs `CREATE CATALOG IF NOT EXISTS` — `_setup_env.ipynb` only selects the catalog (`USE CATALOG`), it doesn't create it. Works on a fresh workspace too: `_setup_env`'s `USE CATALOG` is wrapped in a try/except that prints a warning and continues if the catalog doesn't exist yet, rather than failing the notebook outright — so `00` can still reach its own cell, which creates the catalog and re-selects it before creating schemas/volumes. Every other notebook assumes `00` has already run at least once.
- Use `00_setup_storage_and_shared_tables.ipynb` for shared schemas, medallion conventions, and every shared table (`bronze.ingestion_runs`, all of Silver, all of Gold) — anything with no dataset-specific parameter at all belongs here, its DDL written directly in the notebook (this notebook's whole purpose is to show what gets created) rather than duplicated across dataset setup notebooks
- All Python imports go in the first or second code cell (first is `%run ../_setup_env`, second is config/imports) — never scattered into later cells, even ones that introduce a new dependency

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
  - **Gold**: `build_gold_candidate_groups`, `write_gold_manifest_rows`, `print_gold_outputs` (also asserts no `group_id` spans more than one `split`).
  - **Gold export**: `load_manifest_rows_for_export`, `print_export_outputs`, `remove_expired_exports` (general-purpose, not called by default — retention is undecided, `docs/decisions/009-gold-shard-retention-undecided.md`).
  - `spark`, `dbutils`, `display` are always explicit parameters, never assumed globals.
- `data_platform.shard_export.write_gold_shards_for_splits(rows_by_split, staged_path_by_archive_uri, shard_dir_by_split, size_limit_bytes)` — streams every split's images in one pass (a source archive commonly feeds more than one split) via `iter_archive_matches`, writing matches into one `streaming.MDSWriter(..., exist_ok=True)` per split. Pure Python (no `spark`/`dbutils` param), unit-tested locally with real fixture archives (`tests/test_shard_export.py`). Kept out of `files.py` too — `mosaicml-streaming` is a heavy dependency (pulls in `torch`/`torchvision`/`transformers`) only Gold export needs.

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
- Leakage-group priority: exact duplicate by `source_checksum`, then `lesion_id`, then `patient_id`, otherwise singleton — scoped per `dataset_key`, implemented once in `assign_leakage_groups_and_write_inventory`. Cross-dataset leakage is never checked; see `docs/decisions/004-cross-dataset-leakage-not-checked.md`.
- A dataset's Silver notebook is mostly configuration (its `dataset_key`, label input/output columns, validation overrides, its own staged `archives` list) calling `spark_io`'s pipeline functions in sequence — see either onboarded dataset's `20_silver_validate.ipynb` as the reference shape.

## Gold contract

- Shared table: `gold.manifest_rows` — row-level, one row per image per release, versioned by `dataset_version` (multiple releases coexist; `MERGE INTO` is keyed on `(dataset_version, dataset_key, image_id)`). `gold.manifest_registry` is a **view**, not a table, over it — fully derivable, so it can't drift out of sync.
- Each manifest is its own config file, `config/gold/manifests/<name>.yaml` (same convention as `config/bronze/datasets/<dataset>.yaml`) — a new manifest is a new YAML file, not a new notebook.
- Sampling/splitting is leakage-aware by construction (`data_platform.sampling` works in whole groups), and `print_gold_outputs` still asserts it as a real invariant rather than only trusting the construction.
- `preprocessing_version` is an opaque string tag today — no config file backs it, and Gold shard export writes raw bytes regardless of its value. Never fork a `dataset_version`/manifest solely to vary this field and then export shards for both — that duplicates identical bytes for nothing. A new `dataset_version` is for a new image selection; see `docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`.
- `gold.dataset_card` is deferred, not silently dropped — not needed for a small iteration sample, build for the full-scale release.

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
3. Record actual accepted/rejected/leakage-group and manifest/shard counts here and in `docs/project-checklist.md` once both have run.

Everything else deferred (full-scale Gold release, dataset card, cross-dataset dedup, training-run pinning) is tracked in `docs/project-checklist.md`, not here.

## Update rule

- Update this file when notebook naming, branch usage, config location, execution flow, or project guardrails change
