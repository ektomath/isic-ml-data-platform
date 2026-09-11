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
| `source_uri` | string | Archive-resolvable pointer to this image's bytes (`archive:<source_archive_uri>#<archive_member_path>`) |
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

Source metadata extracted from the MILK10k release, preserved without model-specific transformation. Deliberately a different shape than `bronze.isic_2019_source_metadata` (`AGENT.md`'s "Table strategy" — Bronze source tables stay dataset-specific since source schemas differ): 4 diagnosis levels and 2 anatomic-site levels instead of ISIC 2019's 5, no `patient_id`/`clin_size_long_diam_mm`/`dermoscopic_type`/`family_hx_mm`/`personal_hx_mm`, plus a MILK10k-only `image_manipulation` column. A single release with no train/test split, so `source_split` is always `all`.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier from `isic_id` |
| `source_split` | string | Always `all` — MILK10k has no train/test split |
| `source_uri` | string | Archive-resolvable pointer to this image's bytes (`archive:<source_archive_uri>#<archive_member_path>`) |
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

An index of images from locally staged ISIC release archives — checksums and archive locators
only, no image bytes (`docs/decisions/006-stream-archives-no-blob-storage.md`). Bronze still
streams every archive once to compute each row's `byte_length`/`source_checksum`, it just never
retains the bytes past that computation.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier from the archive member filename |
| `source_split` | string | Source archive split, for example `train` or `test` |
| `archive_member_path` | string | Path of the image inside the source zip archive |
| `source_archive_uri` | string | Landing Volume URI for the original source archive |
| `byte_length` | long | Number of encoded bytes, computed while streaming |
| `source_checksum` | string | SHA-256 of the image's encoded bytes, computed while streaming |

`bronze.milk10k_image_index` is identical in shape (`IMAGE_INDEX_SCHEMA` in `data_platform.spark_io` is the shared, dataset-agnostic schema both are created from) — only the table name and `source_split` (always `all` for MILK10k) differ.

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

One row per **accepted** image after validation and normalization. A rejected image never lands
here — see `silver.rejected_records`, the only table carrying rejected rows and their reasons.

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset this row belongs to (`config/bronze/datasets/<dataset>.yaml`'s `dataset_key`) — this table is shared across every dataset, so this is what lets you segment/compare without parsing `bronze_uri` |
| `image_id` | string | Stable identifier from the source dataset. Unique together with `dataset_key`, not globally |
| `bronze_uri` | string | Archive-resolvable pointer to this image's bytes (`archive:<source_archive_uri>#<archive_member_path>`) |
| `source_checksum` | string | SHA-256 copied from Bronze |
| `image_width` | integer or null | Decoded width |
| `image_height` | integer or null | Decoded height |
| `image_format` | string or null | Decoded file format |
| `validation_status` | string | Always `accepted` here — a row only exists once it's passed every check |
| `validation_reason` | string or null | Always `null` here (reserved); real rejection reasons live in `silver.rejected_records.rejection_reason` |
| `malignancy` | string or null | Canonical benign/malignant/indeterminate classification. Enforced (not just documented) against `data_platform.labels.MALIGNANCY_VALUES` via `apply_label_normalization`'s `controlled_vocabularies` — a `normalize_labels` producing any other value fails the run rather than drifting this column |
| `specific_diagnosis` | string or null | Most specific diagnosis value available from this dataset's source label |
| `patient_id` | string or null | Used for leakage grouping |
| `lesion_id` | string or null | Used for leakage grouping |
| `group_id` | string or null | Leakage-control group |
| `validated_at` | timestamp | Validation timestamp |

`MERGE INTO` is keyed on (`dataset_key`, `image_id`) together, not `image_id` alone.

Label columns grow via `ALTER TABLE ... ADD COLUMNS` as new datasets need new axes (e.g. a future
`severity`/`body_site`), each nullable for datasets that don't populate it — a single shared,
Unity-Catalog-discoverable table rather than a per-dataset table or opaque map column (rationale:
`docs/decisions/003-silver-label-columns-not-map.md`). Validation criteria (e.g. dimension bounds)
can also vary by dataset — see `docs/silver_validation_rules.md`. `specific_diagnosis` is
deliberately **not** vocabulary-enforced — open-ended free text per dataset; only axes meant to be
cross-dataset comparable (currently just `malignancy`) get one.

### `silver.leakage_groups`

Grouping table preventing patient or lesion leakage across dataset splits. Scoped per dataset by
design, not just implementation detail: grouping never merges across datasets even on an ID
collision (`patient_id`/`lesion_id` aren't guaranteed to share a namespace across datasets), and
cross-dataset duplicate *images* (a sound signal, unlike the IDs) are checked separately by
`assert_no_cross_dataset_duplicate_checksums` before Gold sampling — see
`docs/decisions/004-cross-dataset-leakage-not-checked.md`.

| Field | Type | Notes |
|---|---|---|
| `dataset_key` | string | Which dataset this group belongs to; grouping is scoped per dataset |
| `group_id` | string | Stable identifier, embeds `dataset_key` so it's globally unique even if two datasets reuse the same lesion/patient/image ID |
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

One row per image in a training-ready release. Shared across datasets (like every Silver table)
but versioned by `dataset_version`, not dataset-prefixed — more than one release coexists here
(e.g. `sample-v1` alongside a later `v1`), and the same `(dataset_key, image_id)` can legitimately
appear under several `dataset_version`s (each manifest is an independent, self-contained
selection — nothing requires an image's split to match across manifests).

| Field | Type | Notes |
|---|---|---|
| `dataset_version` | string | Release tag, e.g. `sample-v1` or `v1` |
| `dataset_key` | string | Which source dataset this row came from — `image_id` is unique only together with `dataset_key`, same convention as `silver.image_inventory` |
| `image_id` | string | Stable identifier from the source dataset |
| `bronze_uri` | string | Archive-resolvable pointer to this image's bytes — alone sufficient at Gold shard-export time, no Bronze lookup needed |
| `source_checksum` | string | SHA-256 from Bronze |
| `label` | string | Final normalized training label — a direct passthrough of a Silver label column (e.g. `malignancy`) for `sample-v1`; a future release could source it from elsewhere (e.g. a cross-dataset `specific_diagnosis` crosswalk, deliberately deferred — `docs/decisions/003-silver-label-columns-not-map.md`) |
| `group_id` | string | Leakage-control group from `silver.leakage_groups` — never split across `split` values within one `dataset_version`. Guards only *within* one dataset's own groups; cross-dataset duplicates are a separate check, `assert_no_cross_dataset_duplicate_checksums` |
| `split` | string | `train`, `validation`, or `test` |
| `sample_seed` | integer | Seed selecting which images are in this manifest — separate from `split_seed` so a later release can reuse the same image pool under a different `preprocessing_version` (cheap at this metadata level; see the Gold shard export section before also exporting shards for both) |
| `split_seed` | integer | Seed dividing the selected images into `train`/`validation`/`test` |
| `preprocessing_version` | string | This release's *default/intended* preprocessing tag — documentation, not enforced here. What a *specific trained model* actually used is pinned per training run instead, by `config/gold/training_runs/<name>.yaml` (which may differ from this) |
| `manifest_row_hash` | string | `sha2`-256 over `dataset_key`\|`image_id`\|`label`\|`split`\|`source_checksum`, for row-level integrity checking |
| `created_at` | timestamp | When (last) written — refreshed on rerun, same convention as `silver.image_inventory.validated_at`; not a strict immutable-creation guarantee |

`MERGE INTO` is keyed on (`dataset_version`, `dataset_key`, `image_id`) — `dataset_version` is in
the key, unlike Silver's `(dataset_key, image_id)`, since this table holds more than one release
at once; without it, a second release would collide with the first.

### `gold.manifest_registry`

A view over `gold.manifest_rows`, one row per `dataset_version`, summarizing what a manifest
contains (dataset keys, image/per-split counts, distinct label values, preprocessing versions,
sample/split seeds, first/last write time). Not a table — fully derivable from `manifest_rows` by
aggregation, so it can't drift out of sync and needs no separate write path. Query it for "which
manifests do I have, and what's in each one" instead of hand-writing the aggregation.

**Deferred for `sample-v1`** (built for cheap Gold-pipeline iteration, not a real release):
`gold.dataset_card` isn't generated. Additive and cheap to add once there's a real consumer.

### Gold shard export

`notebooks/31_export_gold_shards.ipynb` packs a published `dataset_version`'s images into
per-split MosaicML shard sets (`streaming.MDSWriter`) at
`<storage_root>/gold/<dataset_version>/shards/<split>/`, for local-machine and Databricks
ML-cluster training to read via `streaming.StreamingDataset`. No table of its own — a derived,
fully rebuildable cache, never a second source of truth for image bytes, regenerated by rerunning
the export notebook (which fully rewrites every split's shard directory) rather than incrementally
synced (`docs/decisions/006-stream-archives-no-blob-storage.md`). How long an export is actually
kept around is still undecided — no cleanup runs by default, no lifecycle policy assumed
(`docs/decisions/009-gold-shard-retention-undecided.md`). Each shard sample carries `image` (raw
encoded bytes), `label`, `image_id`, `dataset_key`, `group_id` — the same passthrough columns
`gold.manifest_rows` already has, never transformed pixels (`docs/decisions/001-preprocessing-at-runtime.md`).

**Keyed by `dataset_version` alone, never by `preprocessing_version`.** Exporting writes raw,
unpreprocessed bytes regardless of a manifest's `preprocessing_version` — two `dataset_version`s
selecting the same images but differing only in `preprocessing_version` would export two
byte-for-byte identical shard sets. Don't export shards for a `dataset_version` created solely to
carry a different `preprocessing_version` label; see
`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`.

**Optional metadata CSV export.** If `config/gold/exports/<name>.yaml` sets
`export_metadata_csv: true`, the export notebook also writes each `dataset_key`'s raw Bronze
source metadata (`age_approx`, `sex`, `anatom_site_*`, `diagnosis_*`, `image_type`, etc. — see the
Bronze contracts above) for exactly this release's images to
`<storage_root>/gold/<dataset_version>/metadata/<dataset_key>_source_metadata.csv` — one file per
`dataset_key`, since the two datasets' Bronze metadata schemas don't fully agree. This metadata
otherwise never travels past Bronze in the pipeline itself (the shards carry only `image`/`label`/
`image_id`/`dataset_key`/`group_id`), so exporting it here is what makes it reachable at all for
local use — subgroup analysis, or as auxiliary input to a multimodal (image + tabular/text) model
alongside the shards (`data_platform.spark_io.export_source_metadata_csv`). The baseline ResNet-18
classifier (`src/ml/train.py`) doesn't read it today; a model that does would join it in locally by
`image_id`, and should select/encode fields via a versioned
`config/metadata_preprocessing/<name>.yaml` (below) rather than an ad hoc local column
selection — see `docs/decisions/012-pin-metadata-feature-config-per-training-run.md`.

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

A named, reusable preprocessing recipe (`docs/decisions/011-baseline-training-framework-and-registry-sync.md`)
— decoupled from any one Gold release, not stored *with* one, since the same image selection can
train under several preprocessing recipes without re-exporting shards
(`docs/decisions/010-pin-data-and-preprocessing-per-training-run.md`). Referenced by name from
`config/gold/training_runs/<name>.yaml` (below). Loaded/validated by
`ml.preprocessing.load_preprocessing_config`, resolved into `torchvision` transforms by
`ml.preprocessing.build_transforms` at training/inference time only — never applied to stored
bytes (`docs/decisions/001-preprocessing-at-runtime.md`).

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

### `config/metadata_preprocessing/<name>.yaml`

A named, versioned recipe for turning raw Bronze source metadata (exported by
`notebooks/31_export_gold_shards.ipynb`'s optional `export_metadata_csv` step, above) into a
fixed-length feature vector for a multimodal (image + tabular/text) model — the same "pin the
recipe, don't hand-edit the data" discipline `config/preprocessing/<name>.yaml` already applies
to images (`docs/decisions/012-pin-metadata-feature-config-per-training-run.md`). A Bronze column
not listed under `fields` is implicitly dropped. Referenced by name from
`config/gold/training_runs/<name>.yaml` (below), same convention as `preprocessing_version`.
Loaded/validated by `ml.metadata_preprocessing.load_metadata_preprocessing_config`, resolved into
a feature-vector callable by `ml.metadata_preprocessing.build_metadata_transform` at
training/inference time only — never applied to the exported CSV itself, which stays a raw,
unmodified passthrough.

| Field | Type | Notes |
|---|---|---|
| `metadata_preprocessing_version` | string | Name this file is referenced by |
| `fields` | list of field specs | Only these columns are used; everything else in the raw metadata is dropped |
| `fields[].name` | string | Bronze source-metadata column name (e.g. `age_approx`, `sex`) |
| `fields[].kind` | string | `numeric` or `categorical` |
| `fields[].missing_value` | float | `numeric` only — sentinel used when the source value is null or unparseable |
| `fields[].categories` | list[string] | `categorical` only — fixed vocabulary; order fixes each category's one-hot slot, never derived from a scan |
| `fields[].unknown_category` | string | `categorical` only — label for the trailing one-hot slot a null or out-of-vocabulary value lands in |
| `fields[].source_columns` | `{dataset_key: string}`, optional | For a release combining more than one `dataset_key` whose raw column name for this conceptual field differs — maps a `dataset_key` to its actual raw column name. A `dataset_key` not listed falls back to `fields[].name`; omit entirely if every dataset already agrees |
| `fields[].value_map` | `{dataset_key: {raw_value: string}}`, optional | For a release where a `dataset_key`'s raw *values* for this field don't already match another's — remaps a `dataset_key`'s raw value onto this field's canonical vocabulary before encoding. A `dataset_key` not listed, or a raw value not in its map, passes through unchanged |

### `config/gold/training_runs/<name>.yaml`

Pins exactly one `dataset_version` to exactly one `preprocessing_version` (and, optionally, one
`metadata_preprocessing_version`), plus the hyperparameters for one training run. `ml.train`'s entrypoint
accepts only this one name — never free-standing `dataset_version`/`preprocessing_version`
parameters — so there is no code path to train against an unpinned pairing. Loaded/validated by
`ml.training_run.resolve_training_run`.

| Field | Type | Notes |
|---|---|---|
| `training_run_name` | string | Name this file is referenced by |
| `dataset_version` | string | Which Gold release/shard export to train against |
| `preprocessing_version` | string | Which `config/preprocessing/<name>.yaml` to apply |
| `metadata_preprocessing_version` | string, optional | Which `config/metadata_preprocessing/<name>.yaml` to apply, for a multimodal model. Omitted for an image-only run (e.g. the baseline ResNet-18 classifier) |
| `label_values` | list[string] | Fixes the class-index mapping deterministically (index = position in this list) — not derived from a shard scan |
| `architecture` | string | Currently only `resnet18` is implemented (`ml.train.build_model`) |
| `pretrained` | bool | Whether to start from ImageNet-pretrained weights (needs outbound network access on first use) |
| `batch_size`, `num_epochs`, `learning_rate` | int/int/float | Standard training hyperparameters |
| `optimizer` | string | `adam` or `sgd` |
| `mlflow_experiment` | string, optional | Overrides `ml.mlflow_utils.DEFAULT_MLFLOW_EXPERIMENT` |
| `registered_model_name` | string, optional | A Unity Catalog `catalog.schema.model` name (or workspace registry name). If set, the trained model is registered as a new version of this name in the MLflow Model Registry in addition to being logged to the run; omitted, the model is logged but not registered |

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
