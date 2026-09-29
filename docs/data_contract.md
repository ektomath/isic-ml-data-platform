# Data Contract

The schema of every table and config file the platform writes or reads. This is reference material. For how the pieces fit together, start with [architecture.md](architecture.md).

## At a glance

| Table | Layer | One row per | Key |
|---|---|---|---|
| `bronze.<dataset>_source_metadata` | Bronze | Source metadata record, as published | `image_id`, `source_split` |
| `bronze.<dataset>_image_index` | Bronze | Image in a source archive (checksum and locator, no bytes) | `image_id`, `source_split` |
| `bronze.ingestion_runs` | Bronze | Ingestion run | `ingestion_run_id` |
| `silver.image_inventory` | Silver | Accepted, validated image | `dataset_key`, `image_id` |
| `silver.leakage_groups` | Silver | Leakage-control group | `dataset_key`, `group_id` |
| `silver.rejected_records` | Silver | Rejected image, with the reason | `dataset_key`, `image_id` |
| `gold.manifest_rows` | Gold | Image in a training release | `dataset_version`, `dataset_key`, `image_id` |
| `gold.manifest_registry` (view) | Gold | Training release | `dataset_version` |

Bronze tables are per dataset because source schemas differ. Silver and Gold tables are shared across datasets and carry a `dataset_key` column, since an `image_id` is only unique within one dataset. Source metadata, Silver and Gold are written with `MERGE INTO` on the keys above, and the image index is rewritten on each run, so reruns don't duplicate rows.

## Conventions

- IDs are stable across pipeline runs.
- No table stores image bytes, at any layer. The source archives in the landing Volume are the only permanent copy; tables hold checksums and `archive:<source_archive_uri>#<archive_member_path>` locators, and every step that needs pixels streams them from the archive ([ADR 004](decisions/004-stream-archives-no-blob-storage.md)).
- Paths are logical storage locations, never local filesystem paths.
- Tabular outputs are deterministic for a given source snapshot and configuration.

## Bronze

### `bronze.<dataset>_source_metadata`

One table per dataset, holding its source metadata exactly as published. The source columns differ by dataset and are listed on each [dataset's page](datasets/README.md); they're all stored as strings, with no model-specific transformation. Every dataset's table also carries these standard columns:

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | The source's stable image identifier |
| `source_split` | string | Which source archive split the record came from |
| `source_uri` | string | Archive locator for this image's bytes |
| `source_checksum` | string | SHA-256 of the image bytes, joined from the image index |
| `ingestion_run_id` | string | Links to `bronze.ingestion_runs` |
| `ingested_at` | timestamp | When ingested |

### `bronze.<dataset>_image_index`

Checksums and archive locators for every image, with no bytes. Bronze streams each archive once to compute these and keeps nothing else. Every dataset shares one schema (`IMAGE_INDEX_SCHEMA` in `data_platform.spark_io`).

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | From the archive member filename |
| `source_split` | string | Source archive split |
| `archive_member_path` | string | Path of the image inside the zip |
| `source_archive_uri` | string | Landing Volume URI of the source archive |
| `byte_length` | long | Encoded size in bytes |
| `source_checksum` | string | SHA-256 of the encoded bytes |

### `bronze.ingestion_runs`

One row per ingestion run, shared across datasets.

| Field | Type | Notes |
|---|---|---|
| `ingestion_run_id` | string | Primary key |
| `dataset_name` | string | Example: `isic/2019` |
| `source_version` | string | Source release identifier, from the dataset config |
| `started_at`, `finished_at` | timestamp | Run start and end |
| `status` | string | Always `success` today: a failed run raises before writing a row |
| `records_seen` | integer | Currently written equal to `records_written`; records seen before filtering aren't counted separately |
| `records_written` | integer | Rows written |

## Silver

### `silver.image_inventory`

One row per accepted image. Within one run a rejected image never lands here; it goes to `silver.rejected_records` instead. Both tables are upserted and never cleared, so an image whose outcome changes between runs can end up in both until the stale row is removed.

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset the row belongs to |
| `image_id` | string | Unique together with `dataset_key`, not globally |
| `bronze_uri` | string | Archive locator for the image's bytes |
| `source_checksum` | string | SHA-256 copied from Bronze |
| `image_width`, `image_height` | integer or null | Decoded dimensions |
| `image_format` | string or null | Decoded file format |
| `validation_status` | string | Always `accepted` in this table |
| `validation_reason` | string or null | Always null here (reserved); reasons live in `silver.rejected_records` |
| `malignancy` | string or null | `benign`, `malignant` or `indeterminate`. Enforced: any other value fails the run (`data_platform.labels.MALIGNANCY_VALUES`) |
| `specific_diagnosis` | string or null | Most specific diagnosis the source provides. Free text by design, not vocabulary-enforced |
| `patient_id`, `lesion_id` | string or null | Used for leakage grouping |
| `group_id` | string | Leakage-control group (always set) |
| `validated_at` | timestamp | When validated |

Label columns are real named columns, added with `ALTER TABLE ... ADD COLUMNS` when a new dataset needs a new label, and nullable for datasets that don't have it ([ADR 002](decisions/002-silver-label-columns-not-map.md)). Validation thresholds can differ per dataset (see the [dataset pages](datasets/README.md)).

### `silver.leakage_groups`

Groups images that must never be split across train, validation and test: exact duplicates first (same `source_checksum`), then same lesion, then same patient, otherwise a singleton. Grouping is scoped per dataset, because patient and lesion IDs aren't comparable across sources. Exact duplicate images across datasets are caught separately before Gold sampling ([ADR 003](decisions/003-cross-dataset-leakage-not-checked.md)).

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset the group belongs to |
| `group_id` | string | Embeds `dataset_key`, so it's globally unique |
| `group_type` | string | `duplicate`, `lesion`, `patient` or `singleton` |
| `group_source` | string | Which rule formed the group |
| `image_count` | integer | Images in the group |

### `silver.rejected_records`

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset the rejection belongs to |
| `image_id` | string | Unique together with `dataset_key` |
| `bronze_uri` | string | Archive locator for the image's bytes |
| `rejection_reason` | string | Human-readable reason |
| `rejected_at` | timestamp | When rejected |

## Gold

### `gold.manifest_rows`

One row per image in a training release. Several releases coexist, keyed by `dataset_version`, and the same image can appear in more than one of them with a different split.

| Field | Type | Notes |
|---|---|---|
| `dataset_version` | string | Release tag, e.g. `sample-v1` |
| `dataset_key` | string | Source dataset |
| `image_id` | string | Unique together with `dataset_key` |
| `bronze_uri` | string | Archive locator; enough on its own for shard export |
| `source_checksum` | string | SHA-256 from Bronze, re-verified at export |
| `label` | string | Training label, currently a passthrough of a Silver label column such as `malignancy` |
| `group_id` | string | Leakage group; never split across `split` values within one release |
| `split` | string | `train`, `validation` or `test` |
| `sample_seed` | integer | Seed that chose which images are in the release |
| `split_seed` | integer | Seed that divided them into splits. Kept separate so two releases can provably share an image pool |
| `preprocessing_version` | string | The release's intended default only. What a trained model actually used is pinned in its training-run config |
| `manifest_row_hash` | string | SHA-256 over `dataset_key`, `image_id`, `label`, `split` and `source_checksum` |
| `created_at` | timestamp | When last written; refreshed on rerun |
| `git_commit` | string | Commit of the code that wrote the release. Always set for new releases; empty for rows written before it was added |

### `gold.manifest_registry` (view)

One row per `dataset_version`, derived from `gold.manifest_rows` by aggregation: which datasets it includes, image and per-split counts, label values, preprocessing versions, seeds, the git commits that wrote it, and first and last write time. Because it's a view, it can't drift out of sync.

### Training runs (MLflow, not a table)

Training lineage lives in MLflow and Unity Catalog, not in a project table ([ADR 007](decisions/007-pin-data-and-preprocessing-per-training-run.md)). Every run records:

| What | Where | Notes |
|---|---|---|
| `training_run_name`, `dataset_version`, `preprocessing_version` | Run tags | From the training-run config |
| Hyperparameters, `git_commit`, device | Run params | `git_commit` is always set ([ADR 010](decisions/010-record-git-commit-on-releases-and-runs.md)) |
| `<manifest_table>@<dataset_version>` and `shards@<dataset_version>` | Dataset inputs | Metadata only. The manifest input carries the table's Delta version when run on Databricks; the shards input's digest is a SHA-256 over every split's `index.json`, so it identifies the exact shard files read |
| `preprocessing_config.json` (and `metadata_preprocessing_config.json` when set) | Artifacts | The full recipes, not just their names |
| Metrics, `confusion_matrix.json`, `classification_report.txt` | Metrics and artifacts | Per-epoch and final test metrics |
| `model` | Artifact | Logged with an input/output signature; registered in Unity Catalog when the config sets `registered_model_name`, with the run's `training_run_name`, `dataset_version`, `preprocessing_version` and `git_commit` copied onto the model version as tags |

### Gold shard export

`notebooks/31_export_gold_shards.ipynb` writes a release's images as MosaicML shards to `<storage_root>/gold/<dataset_version>/shards/<split>/`. Each sample carries `image` (raw encoded bytes), `label`, `image_id`, `dataset_key` and `group_id`. Shards are a rebuildable cache, never a second source of truth ([ADR 004](decisions/004-stream-archives-no-blob-storage.md)); how long they're kept is undecided ([ADR 006](decisions/006-gold-shard-retention-undecided.md)). They're keyed by `dataset_version` only, because the bytes are unprocessed and identical whatever the preprocessing.

If the export config sets `export_metadata_csv: true`, the notebook also writes each dataset's Bronze source metadata for the release's images to `<storage_root>/gold/<dataset_version>/metadata/<dataset_key>_source_metadata.csv`, as future input for a multimodal model ([ADR 009](decisions/009-pin-metadata-feature-config-per-training-run.md)). The baseline classifier doesn't read it.

A dataset card for each release is planned but not built yet.

## Config files

### `config/gold/training_runs/<name>.yaml`

Pins one `dataset_version` to one `preprocessing_version`, plus the hyperparameters for one run. Training accepts only this config's name, so there's no way to train on an unpinned combination. Loaded by `ml.training_run.resolve_training_run`.

| Field | Type | Notes |
|---|---|---|
| `training_run_name` | string | Name the file is referenced by |
| `dataset_version` | string | Gold release to train on |
| `preprocessing_version` | string | Which `config/preprocessing/<name>.yaml` to apply |
| `metadata_preprocessing_version` | string, optional | Which `config/metadata_preprocessing/<name>.yaml` to apply; omitted for image-only runs |
| `label_values` | list[string] | Fixes the class-index mapping (index = position in the list) |
| `architecture` | string | Only `resnet18` is implemented |
| `pretrained` | bool | Start from ImageNet weights |
| `batch_size`, `num_epochs`, `learning_rate` | int, int, float | Hyperparameters |
| `optimizer` | string | `adam` or `sgd` |
| `mlflow_experiment` | string, optional | Overrides the default experiment |
| `registered_model_name` | string, optional | If set, the model is also registered under this Unity Catalog name |

### `config/preprocessing/<name>.yaml`

A reusable image preprocessing recipe, applied only at training and inference time, never to stored bytes ([ADR 001](decisions/001-preprocessing-at-runtime.md)). Loaded by `ml.preprocessing.load_preprocessing_config` and turned into torchvision transforms by `build_transforms`. Every training run also saves the full recipe to MLflow as `preprocessing_config.json`. All fields are required except `framework_runtime_notes`.

| Field | Type | Notes |
|---|---|---|
| `preprocessing_version` | string | Name the file is referenced by |
| `image_size` | int | Final crop size in pixels |
| `normalization` | `{mean: [3 floats], std: [3 floats]}` | Per-channel, matching the pretrained weights |
| `resize_policy` | string | Currently only `shorter_side_to_256` |
| `crop_policy` | `{train: string, eval: string}` | Descriptive; `image_size` is what's actually used |
| `augmentation` | `{train: {...}, eval: [...]}` | `train` supports `random_horizontal_flip` and `random_rotation_degrees`. `eval` is ignored: validation and test images always get resize, center crop and normalize only |
| `random_seed` | int | Seeds PyTorch and the shard reader's shuffle order |
| `framework_runtime_notes` | string, optional | Free text describing the transform order |

### `config/metadata_preprocessing/<name>.yaml`

A versioned recipe for turning exported Bronze metadata into a fixed-length feature vector for a multimodal model ([ADR 009](decisions/009-pin-metadata-feature-config-per-training-run.md)). Columns not listed under `fields` are dropped. Loaded by `ml.metadata_preprocessing.load_metadata_preprocessing_config`; not used by any training run yet. The only recipe so far, `baseline-v1`, is a template: its category lists haven't been checked against real exported values, and matching is exact and case-sensitive, so a mismatch silently lands in the unknown slot.

| Field | Type | Notes |
|---|---|---|
| `metadata_preprocessing_version` | string | Name the file is referenced by |
| `fields[].name` | string | Bronze metadata column, e.g. `age_approx` |
| `fields[].kind` | string | `numeric` or `categorical` |
| `fields[].missing_value` | float | Numeric only: value used for null or unparseable input |
| `fields[].categories` | list[string] | Categorical only: fixed vocabulary; order fixes each one-hot slot |
| `fields[].unknown_category` | string | Categorical only: a descriptive name for the extra last slot that null or unrecognized values land in. Required, but the value itself isn't used |
| `fields[].source_columns` | `{dataset_key: column}`, optional | Per-dataset column name, when datasets name the same field differently |
| `fields[].value_map` | `{dataset_key: {raw: canonical}}`, optional | Per-dataset value remapping onto the canonical vocabulary |

## Rules

- No duplicate physical image copies in any layer, except the Gold shard export, which is a rebuildable cache ([ADR 004](decisions/004-stream-archives-no-blob-storage.md)).
- Every accepted Silver image has an archive locator, a source checksum, and at least one non-null label; rows with no label are rejected.
- Every Gold row maps back to a `silver.image_inventory` row.
- Every Gold release is reproducible from the source archives, its manifest config, its seeds and the Git commit recorded on its rows.
- Rejected rows keep enough context to explain why they were excluded.
