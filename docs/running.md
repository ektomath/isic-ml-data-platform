# Running the pipeline

How to set the project up and run it, notebook by notebook. For what each step does and why, see [how-it-works.md](how-it-works.md).

## Locally: tests and training

Requires Python 3.11 and [`uv`](https://docs.astral.sh/uv/). The first `uv sync` downloads PyTorch, so it takes a few minutes.

```bash
uv sync --extra dev   # --extra dev also installs pytest
uv run pytest
```

The tests build their own tiny archives and shards and never download ISIC data. `tests/test_ml_train_smoke.py` runs the full training path end to end on CPU.

To train locally on a real release, copy its shards down from Databricks and point the entrypoint at them. Run this from a git checkout: training refuses to start without a commit to record.

```bash
databricks fs cp -r dbfs:/Volumes/derm_showcase_project/bronze/files/gold/sample-v1/shards ./shards
uv run python -m ml.train --training-run-name sample-v1-resnet18 --shards-root ./shards
```

MLflow credentials come from `~/.databrickscfg`, or from `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

## On Databricks

### One-time setup

1. You need a workspace with Unity Catalog and serverless notebooks, and the source archives downloaded from the [ISIC Archive](https://www.isic-archive.com/) under their licence terms.
2. Import the repository as a **Databricks Git folder**. Every notebook finds the repo's code and configs from there, and Gold and training read the current commit from it.
3. Run [`00_setup_storage_and_shared_tables`](../notebooks/00_setup_storage_and_shared_tables.ipynb). It creates the `derm_showcase_project` catalog, the `bronze`, `silver` and `gold` schemas, the landing and files Volumes, and the tables shared by every dataset. It's safe to rerun.

Paths and table names come from [`config/storage.yaml`](../config/storage.yaml) and the per-dataset configs, never from values typed into a notebook.

### Per dataset: Bronze and Silver

Each dataset has three notebooks in `notebooks/<dataset>/`, run in order. The examples use `isic_2019`; `milk10k` is the same.

| Notebook | Before running | What it produces |
|---|---|---|
| `05_setup_tables_and_folders` | Nothing | The dataset's Bronze tables and folders. Its last cell prints where to upload the archives |
| `10_bronze_ingest` | Upload the archives listed in `config/bronze/datasets/<dataset>.yaml` to `/Volumes/derm_showcase_project/bronze/landing/archives/<dataset>/` | `bronze.<dataset>_image_index`, `bronze.<dataset>_source_metadata` and a row in `bronze.ingestion_runs` |
| `20_silver_validate` | Bronze has run | This dataset's rows in `silver.image_inventory`, `silver.leakage_groups` and `silver.rejected_records` |

Each notebook ends with a review cell that prints row counts, rejection reasons and label distributions. Look at those before moving on.

### Gold: build a training release

| Notebook | Setting to change | What it produces |
|---|---|---|
| [`30_create_gold_manifest`](../notebooks/30_create_gold_manifest.ipynb) | `MANIFEST_NAME`, pointing at a file in [`config/gold/manifests/`](../config/gold/manifests/) | The release's rows in `gold.manifest_rows`, with splits and the git commit |
| [`31_export_gold_shards`](../notebooks/31_export_gold_shards.ipynb) | `EXPORT_NAME`, pointing at a file in [`config/gold/exports/`](../config/gold/exports/) | Shards under `gold/<dataset_version>/shards/<split>/`, plus per-dataset metadata CSVs if `export_metadata_csv: true` |

The manifest config sets the datasets, label column, sample size, seeds and split ratios. To build a different release, add a new YAML file there rather than editing the notebook. The export notebook's serverless environment needs `mosaicml-streaming==0.13.0`. Both notebooks fail loudly if a leakage group spans two splits or a shard count doesn't match the manifest.

### Training

| Notebook | Setting to change | What it produces |
|---|---|---|
| [`40_train_baseline_classifier`](../notebooks/40_train_baseline_classifier.ipynb) | `TRAINING_RUN_NAME`, pointing at a file in [`config/gold/training_runs/`](../config/gold/training_runs/) | An MLflow run and, if the config sets `registered_model_name`, a new model version in Unity Catalog |

Its serverless environment needs `torch`, `torchvision`, `mlflow-skinny` and `scikit-learn`. To try other hyperparameters or another preprocessing recipe, add a new training-run config rather than editing values in place.

## Adding a dataset

See [datasets/README.md](datasets/README.md).
