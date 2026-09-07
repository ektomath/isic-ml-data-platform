# Data Contract

This document defines the canonical records and file outputs used by the ISIC platform on Azure and Databricks.

## Conventions

- IDs are stable and should not change across pipeline runs.
- Paths and table-row references are stored as logical storage locations, not local filesystem paths.
- Image bytes are never stored in any table, at any layer. The landing-volume source archives are the sole permanent store of image bytes; Bronze/Silver hold only checksums and archive locators, and every layer that needs actual bytes (Silver validation, Gold shard export) streams them directly from the archive instead — see `docs/decisions/006-stream-archives-no-blob-storage.md`.
- Silver and Gold primarily store tables, manifests, and quality records.
- All tabular outputs should be deterministic for a given source snapshot and configuration.

## Bronze contracts

### `bronze.isic_2019_source_metadata`

Source metadata extracted from the ISIC release and preserved without model-specific transformation.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier from `isic_id` |
| `source_split` | string | Source archive split, for example `train` or `test` |
| `source_uri` | string | Archive-resolvable pointer (`archive:<source_archive_uri>#<archive_member_path>`) — the only way to locate this image's bytes, since none are stored in any table |
| `attribution` | string or null | Source attribution text |
| `copyright_license` | string or null | Source license value |
| `age_approx` | string or null | Approximate patient age from source metadata |
| `anatom_site_1` ... `anatom_site_5` | string or null | Source anatomic site hierarchy |
| `anatom_site_special` | string or null | Source special anatomic site value |
| `clin_size_long_diam_mm` | string or null | Clinical size value from source metadata |
| `concomitant_biopsy` | string or null | Source biopsy flag/value |
| `dermoscopic_type` | string or null | Source dermoscopic type |
| `diagnosis_1` ... `diagnosis_5` | string or null | Source diagnosis hierarchy |
| `diagnosis_confirm_type` | string or null | Diagnosis confirmation type |
| `family_hx_mm` | string or null | Family melanoma history value |
| `image_type` | string or null | Source image type |
| `lesion_id` | string or null | If available in source metadata |
| `melanocytic` | string or null | Source melanocytic flag/value |
| `patient_id` | string or null | If available in source metadata |
| `personal_hx_mm` | string or null | Personal melanoma history value |
| `sex` | string or null | Source sex value |
| `source_checksum` | string | SHA-256 of the source image bytes |
| `ingestion_run_id` | string | Links to the ingestion run |
| `ingested_at` | timestamp | Pipeline timestamp |

### `bronze.milk10k_source_metadata`

Source metadata extracted from the MILK10k release and preserved without model-specific transformation. A different shape than `bronze.isic_2019_source_metadata` by design (see `AGENT.md`'s "Table strategy" — Bronze source tables are kept dataset-specific since source schemas differ): 4 diagnosis levels and 2 anatomic-site levels instead of ISIC 2019's 5, no `patient_id`/`clin_size_long_diam_mm`/`dermoscopic_type`/`family_hx_mm`/`personal_hx_mm`, plus a MILK10k-only `image_manipulation` column. MILK10k is a single release with no train/test split, so `source_split` is always `all` for this table.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier from `isic_id` |
| `source_split` | string | Always `all` — MILK10k has no train/test split |
| `source_uri` | string | Archive-resolvable pointer (`archive:<source_archive_uri>#<archive_member_path>`) — the only way to locate this image's bytes, since none are stored in any table |
| `attribution` | string or null | Source attribution text |
| `copyright_license` | string or null | Source license value |
| `age_approx` | string or null | Approximate patient age from source metadata |
| `anatom_site_1` ... `anatom_site_2` | string or null | Source anatomic site hierarchy |
| `anatom_site_special` | string or null | Source special anatomic site value |
| `concomitant_biopsy` | string or null | Source biopsy flag/value |
| `diagnosis_1` ... `diagnosis_4` | string or null | Source diagnosis hierarchy |
| `diagnosis_confirm_type` | string or null | Diagnosis confirmation type |
| `image_manipulation` | string or null | Source image-manipulation value — not present in ISIC 2019's metadata |
| `image_type` | string or null | Source image type |
| `lesion_id` | string or null | If available in source metadata |
| `melanocytic` | string or null | Source melanocytic flag/value |
| `sex` | string or null | Source sex value |
| `source_checksum` | string | SHA-256 of the source image bytes |
| `ingestion_run_id` | string | Links to the ingestion run |
| `ingested_at` | timestamp | Pipeline timestamp |

### `bronze.isic_2019_image_index`

An index of images extracted from locally staged ISIC release archives — checksums and archive
locators only, no image bytes (see `docs/decisions/006-stream-archives-no-blob-storage.md`).
Bronze ingestion still streams every archive once to compute each row's `byte_length`/
`source_checksum`, it just never retains the bytes past that computation.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier from the archive member filename |
| `source_split` | string | Source archive split, for example `train` or `test` |
| `archive_member_path` | string | Path of the image inside the source zip archive |
| `source_archive_uri` | string | Landing Volume URI for the original source archive |
| `byte_length` | long | Number of encoded bytes, computed while streaming |
| `source_checksum` | string | SHA-256 of the image's encoded bytes, computed while streaming |

`bronze.milk10k_image_index` has the identical shape (`IMAGE_INDEX_SCHEMA` in `data_platform.spark_io` is the shared, dataset-agnostic schema both tables are created from) — only the table name and `source_split` values differ (always `all` for MILK10k).

### `bronze.ingestion_runs`

Run-level metadata for traceability.

| Field | Type | Notes |
|---|---|---|
| `ingestion_run_id` | string | Primary key |
| `dataset_name` | string | Example: `isic/2019` |
| `source_version` | string | Source release or snapshot identifier |
| `started_at` | timestamp | Run start |
| `finished_at` | timestamp | Run end |
| `status` | string | `success`, `partial`, or `failed` |
| `records_seen` | integer | Total source records processed |
| `records_written` | integer | Total rows written |

## Silver contracts

### `silver.image_inventory`

One row per **accepted** image after validation and normalization. A rejected
image never lands here — see `silver.rejected_records` instead, which is the
only table that carries rejected rows and their reasons.

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset this row belongs to (`config/bronze/datasets/<dataset>.yaml`'s `dataset_key`, e.g. `isic_2019`) — this table is shared across every dataset, so this is what lets you segment or compare across them without parsing `bronze_uri` |
| `image_id` | string | Stable identifier from the source dataset. Unique together with `dataset_key`, not guaranteed globally unique on its own |
| `bronze_uri` | string | Archive-resolvable pointer to this image's bytes (`archive:<source_archive_uri>#<archive_member_path>`) |
| `source_checksum` | string | SHA-256 copied from Bronze |
| `image_width` | integer or null | Decoded width |
| `image_height` | integer or null | Decoded height |
| `image_format` | string or null | Decoded file format |
| `validation_status` | string | Always `accepted` in this table — a row only exists here once it's passed every check; there is no `rejected` value in practice |
| `validation_reason` | string or null | Always `null` in this table (reserved); actual rejection reasons live in `silver.rejected_records.rejection_reason` |
| `malignancy` | string or null | Canonical benign/malignant/indeterminate classification, from this dataset's source label. Enforced (not just documented) against `data_platform.labels.MALIGNANCY_VALUES` at Silver-run time via `apply_label_normalization`'s `controlled_vocabularies` — a dataset's `normalize_labels` producing any other value fails the run rather than silently drifting this column |
| `specific_diagnosis` | string or null | Most specific diagnosis value available from this dataset's source label |
| `patient_id` | string or null | Used for leakage grouping |
| `lesion_id` | string or null | Used for leakage grouping |
| `group_id` | string or null | Leakage-control group |
| `validated_at` | timestamp | Validation timestamp |

`MERGE INTO` is keyed on (`dataset_key`, `image_id`) together, not `image_id` alone.

Label columns grow via `ALTER TABLE ... ADD COLUMNS` as new datasets need new label axes (for example a future dataset's `severity` or `body_site`) — each new column is nullable for every dataset that doesn't populate it. This keeps `image_inventory` a single shared table with real, Unity-Catalog-discoverable columns instead of a per-dataset table or an opaque map column. See `docs/decisions/003-silver-label-columns-not-map.md` for the rationale. Image validation criteria (e.g. dimension bounds) can also vary by dataset — see `docs/silver_validation_rules.md`.

`specific_diagnosis` is deliberately **not** enforced this way — it stays open-ended free text per dataset; only label axes meant to be cross-dataset comparable (currently just `malignancy`) get a controlled vocabulary.

### `silver.leakage_groups`

Grouping table used to prevent patient or lesion leakage across dataset splits. Scoped per dataset by design, not just as an implementation detail: cross-dataset leakage is never checked, and the pipeline assumes without verifying that its source datasets are non-overlapping — see `docs/decisions/004-cross-dataset-leakage-not-checked.md`.

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset this group belongs to; grouping is scoped per dataset (a `patient_id` collision between two different datasets should never merge their images into one group) |
| `group_id` | string | Stable group identifier, embeds `dataset_key` so it's globally unique across datasets even if two datasets happen to reuse the same lesion/patient/image ID values |
| `group_type` | string | Example: `patient`, `lesion`, `duplicate`, `singleton` |
| `group_source` | string | Source of the grouping rule |
| `image_count` | integer | Number of images in the group |

`MERGE INTO` is keyed on (`dataset_key`, `group_id`) together.

### `silver.rejected_records`

Rejected images and records with validation failures.

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset this rejection belongs to; see `silver.image_inventory.dataset_key` |
| `image_id` | string | Stable identifier from the source dataset. Unique together with `dataset_key`, not guaranteed globally unique on its own |
| `bronze_uri` | string | Archive-resolvable pointer to this image's bytes (`archive:<source_archive_uri>#<archive_member_path>`) |
| `rejection_reason` | string | Human-readable failure reason |
| `rejected_at` | timestamp | Timestamp of rejection |

`MERGE INTO` is keyed on (`dataset_key`, `image_id`) together, not `image_id` alone.

## Gold contracts

### `gold.manifest_rows`

One row per image in a training-ready release. Shared across datasets (like
every Silver table) but versioned by `dataset_version` rather than
dataset-prefixed — more than one release can coexist in this table (e.g.
`sample-v1` alongside a later full-scale `v1`), and the same `(dataset_key,
image_id)` can legitimately appear under several different `dataset_version`s
(each manifest is an independent, self-contained selection — nothing requires
an image's split to be consistent across manifests).

| Field | Type | Notes |
|---|---|---|
| `dataset_version` | string | Release tag, e.g. `sample-v1` or `v1` |
| `dataset_key` | string | Which source dataset this row came from — `image_id` is only unique together with `dataset_key`, not globally, same convention as `silver.image_inventory` |
| `image_id` | string | Stable identifier from the source dataset |
| `bronze_uri` | string | Archive-resolvable pointer to this image's bytes (`archive:<source_archive_uri>#<archive_member_path>`) — alone sufficient to find the image's bytes at Gold shard-export time, no Bronze lookup needed |
| `source_checksum` | string | SHA-256 from Bronze |
| `label` | string | Final normalized training label for this release — a direct passthrough of a Silver label column (e.g. `malignancy`) for `sample-v1`; a future release may source `label` from a different column (e.g. a cross-dataset `specific_diagnosis` crosswalk — not designed yet, see `docs/decisions/003-silver-label-columns-not-map.md` for why that's deliberately deferred) |
| `group_id` | string | Leakage-control group, from `silver.leakage_groups` — no `group_id` is ever split across `split` values within one `dataset_version`. Note this only guards against leakage *within* one dataset's own groups — a duplicate image across two different `dataset_key`s in the same manifest is not currently detected, see `docs/project-checklist.md`'s Gold section |
| `split` | string | `train`, `validation`, or `test` |
| `sample_seed` | integer | Seed used to select which images are in this manifest at all — deliberately separate from `split_seed`, so a later release can reuse the exact same image pool (e.g. the same selection under a different `preprocessing_version`) while only `split_seed` differs, or vice versa. Reusing `sample_seed` this way is cheap at the manifest level (metadata rows only) — but see the Gold shard export section below before also exporting shards for both: doing so duplicates identical bytes for no benefit, since Gold shard export never applies preprocessing (`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`) |
| `split_seed` | integer | Seed used to divide the selected images into `train`/`validation`/`test` |
| `preprocessing_version` | string | This release's *default/intended* preprocessing tag (e.g. `"sample-v1"`) — documentation, not a binding constraint enforced by this table. What preprocessing a *specific trained model* actually used is pinned per training run instead, by `config/gold/training_runs/<name>.yaml`'s own `preprocessing_version` field (which may differ from this one) — see the `config/preprocessing/<name>.yaml` and `config/gold/training_runs/<name>.yaml` contracts below |
| `manifest_row_hash` | string | `sha2`-256 over `dataset_key`\|`image_id`\|`label`\|`split`\|`source_checksum`, for row-level integrity checking |
| `created_at` | timestamp | When this row was (last) written — refreshed on rerun, same convention as `silver.image_inventory.validated_at`; not a strict immutable-creation guarantee |

`MERGE INTO` is keyed on (`dataset_version`, `dataset_key`, `image_id`)
together — `dataset_version` is in the key, unlike Silver's `(dataset_key,
image_id)`, because this table holds more than one release at once; without
it, writing a second release would collide with the first.

### `gold.manifest_registry`

A view over `gold.manifest_rows`, one row per `dataset_version`, summarizing
which datasets/labels/splits a manifest contains (dataset keys, image and
per-split counts, distinct label values, preprocessing versions, sample/split
seeds, first/last write time). Not a table — everything in it is fully derivable from
`manifest_rows` by aggregation, so it can never drift out of sync and needs no
separate write path. This is the answer to "which manifests do I have, and
what's in each one" — query it instead of hand-writing the aggregation.

**Deferred for the `sample-v1` release** (built for cheap Gold-pipeline
iteration, not a real release): `gold.dataset_card` is not generated. Additive
and cheap to add once there's a real consumer — a dataset card documenting an
actual released artifact.

### Gold shard export

`notebooks/31_export_gold_shards.ipynb` packs a published `dataset_version`'s images into
per-split MosaicML shard sets (`streaming.MDSWriter`) at
`<storage_root>/gold/<dataset_version>/shards/<split>/`, for local-machine and Databricks
ML-cluster training to read via `streaming.StreamingDataset`. This has no table of its own: it's
a derived, fully rebuildable cache — never a second source of truth for image bytes —
regenerated by rerunning the export notebook (which always fully rewrites every split's shard
directory) rather than incrementally synced. See `docs/decisions/006-stream-archives-no-blob-storage.md`
for why. **How long a shard export is actually kept around is a separate, currently undecided
question** — no cleanup runs by default and no cloud-storage lifecycle policy is assumed; see
`docs/decisions/009-gold-shard-retention-undecided.md`. Each shard sample carries `image` (raw
encoded bytes, `bytes`), `label`, `image_id`, `dataset_key`, `group_id` (`str`) — the same
passthrough columns `gold.manifest_rows` already has, never transformed pixels (preprocessing
stays runtime-only, see `docs/decisions/001-preprocessing-at-runtime.md`).

**A shard export is keyed by `dataset_version` alone, never by `preprocessing_version`.** Exporting
shards writes raw, unpreprocessed bytes regardless of what a manifest's `preprocessing_version`
says — two `dataset_version`s that select the exact same images but differ only in
`preprocessing_version` would export two byte-for-byte identical shard sets. Don't export shards
for a `dataset_version` created solely to carry a different `preprocessing_version` label; see
`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`.

### `gold.dataset_card`

The dataset card is a Markdown document stored with the Gold release.

Minimum contents:

- Dataset version and source snapshot
- Class distribution
- Split distribution
- Validation summary
- Known limitations
- Contact and provenance notes

### `config/preprocessing/<name>.yaml`

Implemented (`docs/decisions/011-baseline-training-framework-and-registry-sync.md`) as a named,
reusable preprocessing recipe — decoupled from any one Gold release, not stored *with* one, since
`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md` established that the same
image selection can be trained under several different preprocessing recipes without re-exporting
shards. A `config/gold/training_runs/<name>.yaml` (below) references one by its
`preprocessing_version` name. Loaded/validated by `ml.preprocessing.load_preprocessing_config`,
resolved into `torchvision` transforms by `ml.preprocessing.build_transforms` at training/inference
time only — never applied to stored bytes (`docs/decisions/001-preprocessing-at-runtime.md`).

| Field | Type | Notes |
|---|---|---|
| `preprocessing_version` | string | Name this file is referenced by |
| `image_size` | int | Final crop size in pixels |
| `normalization` | `{mean: [float, float, float], std: [float, float, float]}` | Per-channel normalization, matching whatever pretrained weights the architecture uses |
| `resize_policy` | string | One of `ml.preprocessing`'s known resize policies (currently just `shorter_side_to_256`) |
| `crop_policy` | `{train: string, eval: string}` | Documented for readability; `image_size` is the actual source of truth for the crop dimension, not this string |
| `augmentation` | `{train: {...}, eval: [...] }` | `train` keys: `random_horizontal_flip` (bool), `random_rotation_degrees` (int). `eval` is typically empty — no augmentation at eval time |
| `random_seed` | int | Passed to `torch.manual_seed` by `ml.train.run_training` |
| `framework_runtime_notes` | string | Free text documenting the exact transform compose order for this recipe |

### `config/gold/training_runs/<name>.yaml`

Pins exactly one `dataset_version` to exactly one `preprocessing_version`, plus the hyperparameters
for one training run. `ml.train`'s entrypoint accepts only this one name — never free-standing
`dataset_version`/`preprocessing_version` parameters — so there is no code path to train against an
unpinned pairing. Loaded/validated by `ml.training_run.resolve_training_run`.

| Field | Type | Notes |
|---|---|---|
| `training_run_name` | string | Name this file is referenced by |
| `dataset_version` | string | Which Gold release/shard export to train against |
| `preprocessing_version` | string | Which `config/preprocessing/<name>.yaml` to apply |
| `label_values` | list[string] | Fixes the class-index mapping deterministically (index = position in this list) — not derived from a shard scan |
| `architecture` | string | Currently only `resnet18` is implemented (`ml.train.build_model`) |
| `pretrained` | bool | Whether to start from ImageNet-pretrained weights (needs outbound network access on first use) |
| `batch_size`, `num_epochs`, `learning_rate` | int/int/float | Standard training hyperparameters |
| `optimizer` | string | `adam` or `sgd` |
| `mlflow_experiment` | string, optional | Overrides `ml.mlflow_utils.DEFAULT_MLFLOW_EXPERIMENT` |

### `gold.training_run_registry`

One row per actual MLflow training run — audit-trail table, same pattern as
`bronze.ingestion_runs`/`gold.manifest_registry`. Populated *from* MLflow runs by
`data_platform.spark_io.write_training_run_registry_rows`, called on demand from
`notebooks/40_train_baseline_classifier.ipynb`'s sync cell — never written to directly by local training
code, since a local run has no Spark session
(`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`).

| Field | Type | Notes |
|---|---|---|
| `mlflow_run_id` | string | Globally unique per actual MLflow run — the `MERGE INTO` key |
| `training_run_name` | string | From `config/gold/training_runs/<name>.yaml` |
| `dataset_version` | string | From the same config |
| `preprocessing_version` | string | From the same config |
| `mlflow_experiment_id` | string | Which MLflow experiment the run belongs to |
| `created_at` | timestamp | Stamped at sync time, not carried from MLflow's own run-start time — same convention as `gold.manifest_rows.created_at` |

## File and partitioning rules

- Source image bytes stay in the landing-volume archives only — never in a Bronze/Silver table, and never extracted as individual Volume objects at any layer (see `docs/decisions/006-stream-archives-no-blob-storage.md`).
- Silver and Gold tables may be partitioned by dataset year or release version if the volume warrants it.
- Do not create duplicate physical image copies in Silver or Gold, **except** the Gold shard export (`notebooks/31_export_gold_shards.ipynb`) — a deliberate, scoped exception: the archives remain the sole source of truth, and shards are a derived, fully rebuildable cache, never a second source of truth. See `docs/decisions/006-stream-archives-no-blob-storage.md`. How long shard exports are actually kept around is a separate, undecided question — see `docs/decisions/009-gold-shard-retention-undecided.md`.
- Do not register every image as a separate table in every layer unless the table has a clear analytical purpose.

## Validation rules

- Every accepted Silver image must have an archive-resolvable `bronze_uri` and source checksum.
- Every accepted Silver image must have at least one non-null label column (for example `malignancy` or `specific_diagnosis`); rows where every label column is null are rejected instead.
- Every Gold row must map back to a Silver image_inventory row.
- Every Gold release must be reproducible from source data, Git commit, and config files.
- Rejected rows must preserve enough context to explain why the record was excluded.
