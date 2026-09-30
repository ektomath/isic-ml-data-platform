# Running the pipeline

How to set the project up and run it, notebook by notebook or as Databricks jobs. For what each step does and why, see [architecture.md](architecture.md).

## On Databricks

### One-time setup

1. You need a workspace with Unity Catalog and serverless notebooks, and the source archives downloaded from the [ISIC Archive](https://www.isic-archive.com/) under their licence terms.
2. Import the repository as a **Databricks Git folder**. Every notebook finds the repo's code and configs from there, and Gold and training read the current commit from it.
3. **Create the catalog, if your workspace needs it.** The catalog's name is set once, in [`config/storage.yaml`](../config/storage.yaml): `isic_ml_data_platform`. Notebook 00 creates it automatically when the workspace has a metastore storage location. Many newer workspaces use Databricks **Default Storage** instead, and there a catalog can only be created from the UI; notebook 00 would otherwise stop with "Metastore storage root URL does not exist". In that case, create it by hand first:
   1. Open **Catalog** in the left sidebar, then **Create catalog**.
   2. Enter the name `isic_ml_data_platform`, exactly as in `config/storage.yaml`.
   3. Leave the storage location on **Default Storage** and create it.
4. Run [`00_setup_storage_and_shared_tables`](../notebooks/00_setup_storage_and_shared_tables.ipynb). It uses the catalog if it exists (and creates it if it doesn't and the workspace allows it), then creates the `bronze`, `silver` and `gold` schemas, the landing and files Volumes, and the tables shared by every dataset. It's safe to rerun.

Every path and table name comes from `config/storage.yaml` and the per-dataset configs, never from values typed into a notebook. To use a different catalog name, change it in `config/storage.yaml` (the Volume paths and table name there must match; this is checked when the notebooks load it) and in `registered_model_name` in the training-run config.

### Per dataset: upload the archives, then Bronze and Silver

Each dataset has three notebooks in `notebooks/<dataset>/`, run in order. The examples use `isic_2019`; `milk10k` is the same.

1. Run `05_setup_tables_and_folders`. It creates the dataset's Bronze tables and folders, and its last cell prints the landing folder for the archives.
2. **Upload the dataset's zip archives** to that folder, `/Volumes/isic_ml_data_platform/bronze/landing/archives/<dataset>/`, with exactly these file names:

   | Dataset | Archive to upload | Download from |
   |---|---|---|
   | `isic_2019` | `ISIC-2019-train-images.zip` | [ISIC 2019 training collection](https://api.isic-archive.com/collections/65/) |
   | `isic_2019` | `ISIC-2019-test-images.zip` | [ISIC 2019 test collection](https://api.isic-archive.com/collections/72/) |
   | `milk10k` | `milk10k.zip` | [MILK10k collection](https://api.isic-archive.com/collections/425/) |

   Rename each download to the file name in this table if it differs.

   The expected names are listed in `config/bronze/datasets/<dataset>.yaml`, and the next notebook stops with an error if one is missing.
3. Run `10_bronze_ingest`. It produces `bronze.<dataset>_image_index`, `bronze.<dataset>_source_metadata` and a row in `bronze.ingestion_runs`.
4. Run `20_silver_validate`. It produces this dataset's rows in `silver.image_inventory`, `silver.leakage_groups` and `silver.rejected_records`.

Each notebook ends with a review cell that prints row counts, rejection reasons and label distributions. Look at those before moving on.

### Gold: build a training release

| Notebook | Setting to change | What it produces |
|---|---|---|
| [`30_create_gold_manifest`](../notebooks/30_create_gold_manifest.ipynb) | `MANIFEST_NAME`, pointing at a file in [`config/gold/manifests/`](../config/gold/manifests/) | The release's rows in `gold.manifest_rows`, with splits and the git commit |
| [`31_export_gold_shards`](../notebooks/31_export_gold_shards.ipynb) | `EXPORT_NAME`, pointing at a file in [`config/gold/exports/`](../config/gold/exports/) | Shards under `gold/<dataset_version>/shards/<split>/`, plus per-dataset metadata CSVs if `export_metadata_csv: true` |

The manifest config sets the datasets, label column, sample size, seeds and split ratios. To build a different release, add a new YAML file there rather than editing the notebook. A published release can't be rebuilt by accident: notebook 30 stops if the `dataset_version` already exists, unless you set `OVERWRITE_EXISTING_RELEASE = True`. Changing a release's selection should normally mean a new `dataset_version`. Both notebooks fail loudly if a leakage group spans two splits or a shard count doesn't match the manifest.

### Training

| Notebook | Setting to change | What it produces |
|---|---|---|
| [`40_train_baseline_classifier`](../notebooks/40_train_baseline_classifier.ipynb) | `TRAINING_RUN_NAME`, pointing at a file in [`config/gold/training_runs/`](../config/gold/training_runs/) | An MLflow run and, if the config sets `registered_model_name`, a new model version in Unity Catalog |

Its first cell installs PyTorch and the other training packages, which takes a few minutes. To try other hyperparameters or another preprocessing recipe, add a new training-run config rather than editing values in place. To train on your own machine instead, see [Locally](#locally-tests-and-training-on-your-own-machine) below.

## As Databricks jobs

The same notebooks are also defined as jobs in [`databricks.yml`](../databricks.yml) and [`resources/jobs.yml`](../resources/jobs.yml), one per step you actually do ([ADR 012](decisions/012-jobs-by-lifecycle.md)). Nothing runs on a schedule; you start each job when you need it.

| Job | Runs | When | Parameters |
|---|---|---|---|
| `setup` | 00 | Once per workspace | |
| `ingest_isic_2019`, `ingest_milk10k` | 05, 10, 20 for that dataset | When you onboard a dataset or rebuild its Bronze and Silver | |
| `build_release` | 30, 31 | Whenever you want a new training set | `release_name` (default `sample-v1`), `overwrite_existing_release` (default `false`) |
| `train` | 40 | Whenever you want to train | `training_run_name` (default `sample-v1-resnet18`) |

With the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) logged in to your workspace, from a clean checkout of the repo:

```bash
databricks bundle deploy                  # upload the code, create or update the jobs
databricks bundle run setup               # once
databricks bundle run ingest_isic_2019    # after uploading its archives
databricks bundle run build_release --params release_name=sample-v1
databricks bundle run train --params training_run_name=sample-v1-resnet18
```

The catalog step and archive uploads above still apply. Jobs record the git commit that was deployed, so deploy from a checkout without uncommitted changes.

## Locally: tests, and training on your own machine

Requires Python 3.11 and [`uv`](https://docs.astral.sh/uv/). The first `uv sync` downloads PyTorch, so it takes a few minutes.

```bash
uv sync --extra dev   # --extra dev also installs pytest
uv run pytest
```

The tests build their own tiny archives and shards and never download ISIC data. `tests/test_ml_train_smoke.py` runs the full training path end to end on CPU.

To train locally on a real release, copy its shards down from Databricks and point the entrypoint at them. Run this from a git checkout: training refuses to start without a commit to record.

```bash
databricks fs cp -r dbfs:/Volumes/isic_ml_data_platform/bronze/files/gold/sample-v1/shards ./shards
uv run python -m ml.train --training-run-name sample-v1-resnet18 --shards-root ./shards
```

MLflow credentials come from `~/.databrickscfg`, or from `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

## Adding a dataset

See [datasets/README.md](datasets/README.md).
