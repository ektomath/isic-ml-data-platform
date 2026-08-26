# Data Contract

This document defines the canonical records and file outputs used by the ISIC platform on Azure and Databricks.

## Conventions

- IDs are stable and should not change across pipeline runs.
- Paths are stored as logical storage locations, not local filesystem paths.
- Image bytes are stored only in Bronze.
- Silver and Gold primarily store tables, manifests, and quality records.
- All tabular outputs should be deterministic for a given source snapshot and configuration.

## Bronze contracts

### `bronze.isic_2019_source_metadata`

Source metadata extracted from the ISIC release and preserved without model-specific transformation.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier from `isic_id` |
| `source_split` | string | Source archive split, for example `train` or `test` |
| `source_uri` | string | Bronze path to the JPEG |
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
| `source_checksum` | string | SHA-256 of the JPEG |
| `ingestion_run_id` | string | Links to the ingestion run |
| `ingested_at` | timestamp | Pipeline timestamp |

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

One row per image after validation and normalization.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier |
| `bronze_uri` | string | Bronze JPEG location |
| `source_checksum` | string | SHA-256 copied from Bronze |
| `image_width` | integer or null | Decoded width |
| `image_height` | integer or null | Decoded height |
| `image_format` | string or null | Decoded file format |
| `validation_status` | string | `accepted` or `rejected` |
| `validation_reason` | string or null | Reason for rejection |
| `normalized_label` | string or null | Canonical label value |
| `patient_id` | string or null | Used for leakage grouping |
| `lesion_id` | string or null | Used for leakage grouping |
| `group_id` | string or null | Leakage-control group |
| `validated_at` | timestamp | Validation timestamp |

### `silver.leakage_groups`

Grouping table used to prevent patient or lesion leakage across dataset splits.

| Field | Type | Notes |
|---|---|---|
| `group_id` | string | Stable group identifier |
| `group_type` | string | Example: `patient`, `lesion`, `duplicate` |
| `group_source` | string | Source of the grouping rule |
| `image_count` | integer | Number of images in the group |

### `silver.rejected_records`

Rejected images and records with validation failures.

| Field | Type | Notes |
|---|---|---|
| `image_id` | string | Stable ISIC identifier |
| `bronze_uri` | string | Original source path |
| `rejection_reason` | string | Human-readable failure reason |
| `rejected_at` | timestamp | Timestamp of rejection |

## Gold contracts

### `gold.classification_manifest`

One row per image in the training-ready dataset.

| Field | Type | Notes |
|---|---|---|
| `dataset_version` | string | Version tag such as `v1` |
| `image_id` | string | Stable ISIC identifier |
| `bronze_uri` | string | Immutable Bronze path |
| `source_checksum` | string | SHA-256 from Bronze |
| `label` | string | Final normalized training label |
| `group_id` | string | Leakage-control group |
| `split` | string | `train`, `validation`, or `test` |
| `split_seed` | integer | Seed used to generate the split |
| `preprocessing_version` | string | Version of preprocessing config |
| `manifest_row_hash` | string | Optional row-level integrity hash |

### `gold.dataset_card`

The dataset card is a Markdown document stored with the Gold release.

Minimum contents:

- Dataset version and source snapshot
- Class distribution
- Split distribution
- Validation summary
- Known limitations
- Contact and provenance notes

### `gold.preprocessing.yaml`

The preprocessing configuration stored with a Gold release must include:

- Input image size
- Normalization scheme
- Resize and crop policy
- Augmentation policy
- Random seed
- Framework-specific runtime notes

## File and partitioning rules

- JPEGs stay in Bronze.
- Silver and Gold tables may be partitioned by dataset year or release version if the volume warrants it.
- Do not create duplicate physical image copies in Silver or Gold.
- Do not register every image as a separate table in every layer unless the table has a clear analytical purpose.

## Validation rules

- Every accepted Silver image must have a Bronze URI and source checksum.
- Every Gold row must map back to a Silver image_inventory row.
- Every Gold release must be reproducible from source data, Git commit, and config files.
- Rejected rows must preserve enough context to explain why the record was excluded.
