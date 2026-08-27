# AGENT

Last updated: 2026-08-26

This is the canonical repo-local instruction file for project working state and conventions.
Ask the agent to reread this file after long gaps, after conversation compaction, or when project conventions change.

## Current branch

- `feature_branch_bronze`

## Current focus

- Notebook-first Databricks workflow
- Shared medallion setup plus end-to-end Bronze ingestion for ISIC 2019
- Shared setup code in `src/data_platform/setup.py`

## Repository shape

- `src/` contains shared Python code
- `notebooks/` contains Databricks notebooks
- `docs/` contains architecture, contracts, checklist, and supporting context

## Notebook flow

1. `notebooks/_setup_env.ipynb`
2. `notebooks/00_setup_storage.ipynb`
3. `notebooks/isic_2019/05_create_bronze_tables_and_folders.ipynb`
4. `notebooks/isic_2019/10_bronze_ingest.ipynb`
5. `notebooks/30_data_quality_summary.ipynb`
6. `notebooks/40_baseline_results.ipynb`

## Notebook organization

- Use stage-based numbering with gaps. Use `00` for global setup, `05` for dataset-specific object/folder setup, `10` for Bronze ingestion, then `20`, `30`, `40` for later stages.
- Group dataset-specific notebooks under a dataset folder
- Keep Bronze ingestion separate from quality and results notebooks
- Use `notebooks/_setup_env.ipynb` as the shared Databricks import/bootstrap notebook
- Use `00_setup_storage.ipynb` only for shared schemas and medallion conventions

## Shared code

- `build_layout(dataset_config)`
- `load_dataset_config(path)`
- `data_platform.files` helpers for archive extraction, image detection, file listing, and checksums

## Bronze contract

- Shared table: `bronze.ingestion_runs`
- `bronze.isic_2019_source_metadata`
- Raw JPEGs stay in Bronze paths under `isic_2019/images`
- Raw metadata files stay in `isic_2019/metadata`

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
- Keep shared tables in `00_setup_storage.ipynb`
- Keep dataset-specific DDL in the dataset setup notebook, not in shared code
- Create the shared landing folder and medallion root folders in `00_setup_storage.ipynb`
- Keep dataset-specific folder paths in the dataset Bronze notebook

## Guardrails

- Notebook code may load config into a local variable, but must not become a second source of truth
- Do not duplicate setup logic across notebooks when it belongs in shared code
- Do not duplicate Bronze table DDL across notebooks
- If a convention was corrected during implementation, record the corrected rule here instead of relying on chat history

## Current next step

- Run the end-to-end Bronze ingestion notebook in Databricks
- Verify rerun behavior and inspect the landed archive, extracted files, and Bronze tables

## Update rule

- Update this file when notebook naming, branch usage, config location, execution flow, or project guardrails change
