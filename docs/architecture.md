# Architecture

How the platform is built: what each layer does, how an image travels from a zip archive to a trained model, where every file and table lives, and the decisions worth knowing about. To run it yourself, see [running.md](running.md).

## The problem

The source data is public dermoscopy images of skin lesions from the [ISIC Archive](https://www.isic-archive.com/): ISIC 2019 (about 33,000 images, in train and test zip archives) and MILK10k (one archive). Each archive holds the images plus a metadata CSV with the diagnosis. Training a classifier straight from those files has three hidden traps:

1. **Unreproducible inputs.** If images are copied, resized, filtered or relabelled ad hoc, nobody can later say exactly what a model was trained on.
2. **Dirty data.** Some images may be unreadable or an unusable size, and some diagnoses don't map cleanly to a label.
3. **Leakage.** The same lesion or patient appears in several images. A random train/test split puts near-identical images on both sides and inflates test scores.

The platform turns the raw releases into versioned, validated training datasets that address all three, and pins every training run to exactly one of them.

## The one idea to know first

**Image bytes are stored in exactly one place: the original zip archives in the landing Volume.** No layer copies them into a table or extracts them as individual files. Every table carries a pointer instead:

```text
bronze_uri = archive:<source archive path>#<path of the image inside the zip>
```

plus the image's SHA-256 checksum, recorded once at ingestion. Any step that needs pixels (Silver validation, and the Gold shard export that packs images into training files) streams them from the archive, re-hashes them, and fails the run if the checksum no longer matches. So the archives are immutable in practice, not just by convention.

## The data pipeline

![Bronze/Silver/Gold data pipeline](assets/architecture.svg)

Everything lives in one Unity Catalog catalog (`isic_ml_data_platform`) with a schema per layer. Each dataset has its own config file in [`config/bronze/datasets/`](../config/bronze/datasets/) and its own short set of notebooks; the pipeline logic itself is shared.

### Bronze: record the source as it arrived

1. Each archive is copied **once** from the landing Volume to local disk.
2. Every image in it is read, hashed and measured, then discarded. One row per image goes into `bronze.<dataset>_image_index`: id, split, archive path, byte length and checksum. No bytes.
3. The archive's metadata CSV goes into `bronze.<dataset>_source_metadata` with its values unchanged (the source's `isic_id` becomes `image_id`, and each row gets the image's checksum and archive pointer).
4. The run is logged in the shared `bronze.ingestion_runs` table.

### Silver: decide what is trustworthy

The logic lives in [`src/data_platform/spark_io.py`](../src/data_platform/spark_io.py) and runs in this order:

1. **Reconcile.** Any indexed image with no metadata row is rejected, so orphan images can't slip through.
2. **Normalize labels** ([`labels.py`](../src/data_platform/labels.py)):
   - `malignancy`: `benign`, `malignant` or `indeterminate`. The vocabulary is enforced: a mapping that produces any other value fails the whole run.
   - `specific_diagnosis`: the most specific diagnosis available, kept as free text.

   Rows where neither label resolves are rejected with a reason.
3. **Validate images.** Each candidate is streamed from its archive, checksum-verified and decoded. Unreadable images and images outside 50 to 15,000 pixels per side are rejected.
4. **Group for leakage control.** Every accepted image gets a `group_id`, in priority order: identical bytes, then same lesion, then same patient, otherwise its own group. Splits are later made by group, never by image.
5. **Write** this dataset's results, replacing its rows from any earlier run, to three tables shared by every dataset: `silver.image_inventory` (accepted images with labels and group), `silver.leakage_groups` and `silver.rejected_records` (every excluded image and why).

### Gold: publish a versioned training dataset

**Manifest.** A small YAML file in [`config/gold/manifests/`](../config/gold/manifests/) names a `dataset_version`, the datasets it draws from, the label column, sample size, seeds and split ratios. From it, the manifest notebook:

1. Checks that no image (by checksum) appears in two of the datasets being combined.
2. Selects whole leakage groups and assigns them to `train`, `validation` or `test` with seeded, pure Python ([`sampling.py`](../src/data_platform/sampling.py)), so the same config always gives the same split.
3. Writes every dataset's rows to `gold.manifest_rows` in one atomic write, each with a row hash and the git commit of the code that wrote it. It refuses to run if it can't find a commit, and refuses to overwrite a release that's already published, since models may be trained on it.
4. Fails if any leakage group ends up in more than one split.

`gold.manifest_registry` is a view with one line per release. The current release, `sample-v1`, is deliberately small (about 100 images per dataset) so the pipeline can be iterated on cheaply.

**Shard export.** Streams the release's images out of their archives in one pass, re-verifies every checksum, and writes per-split [MosaicML](https://docs.mosaicml.com/projects/streaming/) shards: a handful of large files per split, each packing many images together with their labels, so training can stream them efficiently instead of opening thousands of small files. Each sample holds the raw image bytes, its label, `image_id`, `dataset_key` and `group_id`. Shards are a rebuildable cache, never a source of truth. They're written to a temporary folder and only moved into place once every image is verified, so a failed export never leaves half-written shards behind, and the export fails if its counts don't match the manifest.

## Training and reproducibility

![Training and reproducibility flow](assets/architecture-training.svg)

A training run is named by one file in [`config/gold/training_runs/`](../config/gold/training_runs/). It pins exactly one `dataset_version` (which images) to exactly one `preprocessing_version` (a recipe in [`config/preprocessing/`](../config/preprocessing/): resize, crop, normalize, augment), plus the label order and hyperparameters. Training accepts only that name, so there's no way to train on an unpinned combination.

`ml.train.run_training` then:

1. Looks up the git commit, and refuses to start without one.
2. Reads the shards and applies the preprocessing recipe at load time only.
3. Fine-tunes a pretrained ResNet-18.
4. Logs to MLflow: parameters including the commit, metrics, the full preprocessing recipe, and the model with its input/output signature. The Gold release and the shard files (with a digest of their index files) are recorded as the run's dataset inputs.
5. When the config names a model, registers it in Unity Catalog, tagged with the dataset version, preprocessing version and commit.

The same call runs from the Databricks notebook or a laptop; only the shard path and MLflow credentials differ.

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

## Design decisions

Every non-obvious call, with the alternatives considered and what it costs, is an Architecture Decision Record in [`docs/decisions/`](decisions/README.md). The index lists all of them in one line each.

## Where things live

Every table's columns are in [data_contract.md](data_contract.md).

### Files: two Unity Catalog Volumes

```text
/Volumes/isic_ml_data_platform/bronze/landing/
  archives/
    isic_2019/          source zip archives, the only copy of image bytes
    milk10k/
/Volumes/isic_ml_data_platform/bronze/files/
  isic_2019/
    metadata/           metadata CSVs extracted from each archive
  milk10k/
    metadata/
  gold/sample-v1/
    metadata/           optional per-dataset metadata CSVs for the release
    shards/             MosaicML shards, one folder per split (rebuildable cache)
      train/
      validation/
      test/
```

Silver has no folders; it's tables only.

### Tables: one catalog, a schema per layer

```text
Catalog: isic_ml_data_platform
  Schema: bronze
    Tables:
      ingestion_runs
      <dataset>_image_index        one pair per dataset, e.g. isic_2019_*, milk10k_*
      <dataset>_source_metadata
    Volumes:
      landing
      files
  Schema: silver
    Tables:
      image_inventory
      leakage_groups
      rejected_records
  Schema: gold
    Tables:
      manifest_rows
    Views:
      manifest_registry
    Models:
      baseline_classifier          registered by training
```

Training runs themselves live in MLflow, not in a table.

Every stage is a numbered notebook. They run by hand, or as Databricks jobs defined in [`databricks.yml`](../databricks.yml), one job per lifecycle step ([running.md](running.md), [ADR 012](decisions/012-jobs-by-lifecycle.md)).

## Code map

| Path | What it holds |
|---|---|
| [`src/data_platform/files.py`](../src/data_platform/files.py) | Archive staging and streaming images out of zips |
| [`src/data_platform/validate.py`](../src/data_platform/validate.py) | Image decoding and dimension checks |
| [`src/data_platform/labels.py`](../src/data_platform/labels.py) | Shared label vocabularies and diagnosis normalization |
| [`src/data_platform/sampling.py`](../src/data_platform/sampling.py) | Seeded, leakage-aware sampling and split assignment |
| [`src/data_platform/shard_export.py`](../src/data_platform/shard_export.py) | Writing Gold shards from the archives |
| [`src/data_platform/dataset_layout.py`](../src/data_platform/dataset_layout.py) | Each dataset's paths and table names, from its config |
| [`src/data_platform/provenance.py`](../src/data_platform/provenance.py) | Finding the git commit, locally or on Databricks |
| [`src/data_platform/spark_io.py`](../src/data_platform/spark_io.py) | The Spark side of Bronze, Silver and Gold |
| [`src/ml/`](../src/ml/) | Training: config resolution, preprocessing, dataset, metrics, MLflow |
| [`config/`](../config/) | One YAML per dataset, manifest, export, preprocessing recipe and training run |
| [`notebooks/`](../notebooks/) | Thin, numbered Databricks notebooks that call the code above in order |
| [`tests/`](../tests/) | Tests on tiny real archives, shards and MLflow stores; no ISIC data needed |

Everything except `spark_io.py` is plain Python, so it's unit-tested locally and in GitHub Actions CI on every push.

## Further reading

- [running.md](running.md): setup, and each notebook in order
- [data_contract.md](data_contract.md): every table's and config file's columns and rules
- [datasets/](datasets/README.md): one page per dataset, and how to add one
- [decisions/](decisions/README.md): every design decision record, with an index
