# How it works

A 10-minute tour of the platform for someone reviewing this repository: what each layer does, how an image travels from a zip archive to a trained model, the design decisions worth knowing about, and how to run it yourself.

The goal the whole design serves:

> A reviewer can trace a trained model back to an immutable Gold manifest, a validated Silver inventory, and the exact source-image checksums used to create it.

## Current status

| Stage | State |
|---|---|
| Bronze ingestion | Built, and run end to end on Databricks for both datasets (ISIC 2019 and MILK10k) |
| Silver validation | Built, and run end to end on Databricks serverless for both datasets |
| Gold manifest and shard export | Built, and run on Databricks for the small `sample-v1` release, producing its training shards |
| Baseline training | Built and unit-tested, including an end-to-end smoke test that trains on tiny real shards and logs to a local MLflow store; never run on Databricks or against the real shards |

## The problem

The source data is public dermoscopy images of skin lesions from the [ISIC Archive](https://www.isic-archive.com/): ISIC 2019 (about 33,000 images, split into train and test zip archives) and MILK10k (one archive). Each archive holds the images plus a metadata CSV with the diagnosis. Training a classifier straight from those files has three hidden traps:

1. **Unreproducible inputs.** If images are copied, resized, filtered or relabelled ad hoc, nobody can later say exactly what a model was trained on.
2. **Dirty data.** Some images may be unreadable or an unusable size, and some diagnoses do not map cleanly to a label.
3. **Leakage.** The same lesion or patient appears in several images. A random train/test split puts near-identical images on both sides and inflates test scores.

The platform turns the raw releases into versioned, validated training datasets that address all three, and pins every training run to exactly one of them.

## The one idea to know first

**Image bytes are stored in exactly one place: the original zip archives in the landing Volume.** No layer copies them into a table or extracts them as individual files. Every table carries a pointer instead:

```text
bronze_uri = archive:<source archive path>#<path of the image inside the zip>
```

plus the image's SHA-256 checksum, recorded once at ingestion. Any step that needs pixels (Silver validation, Gold shard export) streams them from the archive on demand, re-hashes them, and fails the run if the checksum no longer matches. So the archives are immutable in practice, not just by convention.

## The data pipeline

![Bronze/Silver/Gold data pipeline](assets/architecture.svg)

Everything lives in one Unity Catalog catalog (`derm_showcase_project`) with a schema per layer. Each dataset has its own config file in [`config/bronze/datasets/`](../config/bronze/datasets/) and its own short set of notebooks; the pipeline logic itself is shared.

### Bronze: record the source exactly as it arrived

Notebook: `notebooks/<dataset>/10_bronze_ingest.ipynb`

1. Each archive is copied **once** from the landing Volume to the cluster's local disk.
2. Every image in it is read locally, hashed and measured, then discarded. One row per image goes into `bronze.<dataset>_image_index`: id, split, archive path, byte length and checksum. No bytes.
3. The archive's metadata CSV is extracted for traceability and loaded, unmodified, into `bronze.<dataset>_source_metadata`.
4. The run is logged in the shared `bronze.ingestion_runs` table.

### Silver: decide what is trustworthy

Notebook: `notebooks/<dataset>/20_silver_validate.ipynb`. The notebook is mostly configuration; the logic lives in [`src/data_platform/spark_io.py`](../src/data_platform/spark_io.py) and runs in this order:

1. **Reconcile.** Any indexed image with no metadata row is rejected, so orphan images cannot slip through unnoticed.
2. **Normalize labels.** The source diagnosis fields are mapped to two shared label columns ([`labels.py`](../src/data_platform/labels.py)):
   - `malignancy`: `benign`, `malignant` or `indeterminate`. The vocabulary is enforced: a mapping that produces any other value fails the whole run instead of quietly writing bad labels.
   - `specific_diagnosis`: the most specific diagnosis available, kept as free text.

   Rows where neither label resolves are rejected with a reason.
3. **Validate images.** Each candidate is streamed from its archive, checksum-verified and decoded. Unreadable images and images outside 50 to 15,000 pixels per side are rejected ([validation thresholds](datasets/README.md#default-image-validation-thresholds)).
4. **Group for leakage control.** Every accepted image gets a `group_id`, chosen in priority order: identical bytes, then same lesion, then same patient, otherwise its own group. Splits are later made by group, never by image.
5. **Write.** Results are upserted into three tables shared by every dataset and keyed on `dataset_key`:
   - `silver.image_inventory`: one row per accepted image, with labels, dimensions and group.
   - `silver.leakage_groups`: one row per group, with its type and size.
   - `silver.rejected_records`: every excluded image and why.

### Gold: publish a versioned training dataset

Gold is two steps, each driven by a small YAML file rather than values in a notebook.

**Manifest** ([`30_create_gold_manifest`](../notebooks/30_create_gold_manifest.ipynb), config in [`config/gold/manifests/`](../config/gold/manifests/)). A manifest names a `dataset_version`, which datasets it draws from, the label column, sample size, seeds and split ratios. The notebook:

1. Summarizes each dataset's accepted Silver rows into one row per leakage group.
2. Checks that no image (by checksum) appears in two of the datasets being combined, since a shared image could otherwise land in train in one copy and test in the other.
3. Selects whole groups and assigns them to `train`, `validation` or `test` with pure, seeded Python ([`sampling.py`](../src/data_platform/sampling.py)), so the same config always gives the same split.
4. Upserts the rows into `gold.manifest_rows`, each with a row hash. Rerunning is idempotent.
5. Fails if any leakage group ends up in more than one split.

`gold.manifest_registry` is a view over the manifest rows, one line per release, so it cannot drift out of sync. The current release, `sample-v1`, is deliberately small (about 100 images from each dataset) so the pipeline can be iterated on cheaply.

**Shard export** ([`31_export_gold_shards`](../notebooks/31_export_gold_shards.ipynb), config in [`config/gold/exports/`](../config/gold/exports/)). Streams the release's images out of their archives in a single pass, re-verifies every checksum, and writes per-split [MosaicML](https://docs.mosaicml.com/projects/streaming/) shards to `gold/<dataset_version>/shards/<split>/`. Shards hold raw bytes and labels only. They are a rebuildable cache, never a source of truth, and the export fails if the shard counts do not match the manifest.

## Training and reproducibility

![Training and reproducibility flow](assets/architecture-training.svg)

A training run is named by one file in [`config/gold/training_runs/`](../config/gold/training_runs/). That file pins exactly one `dataset_version` (which images) to exactly one `preprocessing_version` (a recipe in [`config/preprocessing/`](../config/preprocessing/): resize, crop, normalize, augment), plus the label order and hyperparameters. The training entrypoint accepts only that name, so there is no way to train on an unpinned combination.

`ml.train.run_training` then:

1. Reads the shards through `StreamingDataset` and applies the preprocessing recipe at load time only.
2. Fine-tunes a PyTorch/torchvision ResNet-18.
3. Logs parameters (including the git commit), per-epoch metrics, final test metrics (balanced accuracy, per-class recall, confusion matrix), the full preprocessing recipe and the model itself to MLflow, tagged with the run name, `dataset_version` and `preprocessing_version`. The Gold manifest release and shard files it read are recorded as the run's dataset inputs.

The same call runs from the Databricks notebook ([`40_train_baseline_classifier`](../notebooks/40_train_baseline_classifier.ipynb)) or from a laptop; only the shard path and MLflow credentials differ. When the config names a model, it's registered in Unity Catalog, where each version links back to its run and from there to its data.

## From archive to model

```text
zip archive in the landing Volume                  (the only copy of the bytes)
  → hashed and indexed, bytes discarded            Bronze: image_index + source_metadata
  → label normalized, streamed, checksum-verified,
    decoded and size-checked                       Silver
  → assigned a leakage group                       Silver: image_inventory or rejected_records
  → sampled by whole group and given a split       Gold: manifest_rows
  → streamed again, checksum-verified, packed      Gold: shards per split
  → preprocessed at load time and trained on       Training: MLflow run
  → registered, linked to its run and inputs       Unity Catalog model
```

## Key design decisions

Every non-obvious call is written up as an Architecture Decision Record in [`docs/decisions/`](decisions/), including the ones that were later reversed. The most important:

**Stream from the source archives; never store image bytes anywhere else** ([ADR 006](decisions/006-stream-archives-no-blob-storage.md)). The first version extracted each image to cloud storage as its own file, which meant about 33,000 separate writes and cost about \$17 and 90 minutes per run, because cloud storage charges per operation ([lessons learned](../README.md#lessons-learned)). The second version ([ADR 002](decisions/002-store-bronze-images-as-delta-blobs.md), now superseded) wrote the bytes into a Delta table instead, which cut the run to about 5 minutes and an estimated $6 but kept a second copy of every image. The current design removes that copy entirely. The same ADR generalizes the lesson: only persist data when a step genuinely needs it.

**Verify immutability by checksum, not assumption** ([ADR 008](decisions/008-immutable-source-archives-checksum-verified.md)). Because every layer re-reads the archives, a silently replaced archive would change what old manifests point at. Every re-read is hashed and compared against what Bronze recorded.

**Preprocessing happens at training time, and is pinned per run** ([ADR 001](decisions/001-preprocessing-at-runtime.md), [ADR 010](decisions/010-pin-data-and-preprocessing-per-training-run.md), [ADR 011](decisions/011-baseline-training-framework-and-registry-sync.md)). One shard export serves any number of preprocessing recipes, and the training-run config records which one a given model used. [ADR 012](decisions/012-pin-metadata-feature-config-per-training-run.md) applies the same rule to tabular metadata features for future multimodal models.

**Labels are real columns on one shared table** ([ADR 003](decisions/003-silver-label-columns-not-map.md)), rather than a map column or a table per dataset. Labels stay visible in Unity Catalog's explorer, and a new dataset adds a nullable column when it needs a new label.

**Known limits are recorded as decisions too.** Leakage groups do not span datasets, because patient and lesion ids are not comparable across sources; only exact duplicate images are caught, before datasets are combined ([ADR 004](decisions/004-cross-dataset-leakage-not-checked.md)). How long shard exports are kept is explicitly undecided until there is real usage to base it on ([ADR 009](decisions/009-gold-shard-retention-undecided.md)).

## Code map

| Path | What it holds |
|---|---|
| [`src/data_platform/files.py`](../src/data_platform/files.py) | Archive staging and streaming images out of zips |
| [`src/data_platform/validate.py`](../src/data_platform/validate.py) | Image decoding and dimension checks |
| [`src/data_platform/labels.py`](../src/data_platform/labels.py) | Shared label vocabularies and diagnosis normalization |
| [`src/data_platform/sampling.py`](../src/data_platform/sampling.py) | Seeded, leakage-aware sampling and split assignment |
| [`src/data_platform/shard_export.py`](../src/data_platform/shard_export.py) | Writing Gold shards from the archives |
| [`src/data_platform/spark_io.py`](../src/data_platform/spark_io.py) | The Spark side of Bronze, Silver and Gold |
| [`src/ml/`](../src/ml/) | Training: config resolution, preprocessing, dataset, metrics, MLflow |
| [`config/`](../config/) | One YAML per dataset, manifest, export, preprocessing recipe and training run |
| [`notebooks/`](../notebooks/) | Thin, numbered Databricks notebooks that call the code above in order |
| [`tests/`](../tests/) | Tests on tiny real archives, shards and MLflow stores; no ISIC data needed |

Everything except `spark_io.py` is plain Python, so it is unit-tested locally and in GitHub Actions CI on every push.

## Running it

### Locally

Requires Python 3.11 and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync --extra dev
uv run pytest
```

The tests build their own tiny archives and shards and never download ISIC data. `tests/test_ml_train_smoke.py` runs the full training path end to end on CPU.

To train locally on a real release, copy its shards down from Databricks and point the entrypoint at them:

```bash
databricks fs cp -r dbfs:/Volumes/derm_showcase_project/bronze/files/gold/sample-v1/shards ./shards
uv run python -m ml.train --training-run-name sample-v1-resnet18 --shards-root ./shards
```

MLflow credentials come from `~/.databrickscfg`, or from `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

### On Databricks

Requires a workspace with Unity Catalog, and the source archives downloaded from the ISIC Archive under their licence terms.

1. Import the repository as a Databricks Git folder.
2. Run [`00_setup_storage_and_shared_tables`](../notebooks/00_setup_storage_and_shared_tables.ipynb) once to create the catalog, schemas, Volumes and shared tables.
3. For each dataset (`isic_2019`, `milk10k`): upload its zip archives to `/Volumes/derm_showcase_project/bronze/landing/archives/<dataset>/`, then run its `05_setup_tables_and_folders`, `10_bronze_ingest` and `20_silver_validate` notebooks in order.
4. Run [`30_create_gold_manifest`](../notebooks/30_create_gold_manifest.ipynb), then [`31_export_gold_shards`](../notebooks/31_export_gold_shards.ipynb). The export notebook's environment needs `mosaicml-streaming==0.13.0`.
5. Run [`40_train_baseline_classifier`](../notebooks/40_train_baseline_classifier.ipynb). Its environment needs `torch`, `torchvision`, `mlflow-skinny` and `scikit-learn`.

Every notebook ends with a review cell that prints what it produced. The Gold notebooks also fail loudly if a split or count invariant is broken.

## Further reading

- [architecture.md](architecture.md): storage layout and Unity Catalog structure
- [data_contract.md](data_contract.md): every table's and config file's columns and rules
- [datasets/](datasets/README.md): one page per dataset, and how to add one
- [decisions/](decisions/): every design decision record, with an index
