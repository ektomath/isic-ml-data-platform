# MILK10k (`milk10k`)

The MILK10k release, downloaded from the [ISIC Archive](https://www.isic-archive.com/). It's a single release with no train/test split. Licensing is per image and is kept in the `copyright_license` column.

| | |
|---|---|
| Config | [`config/bronze/datasets/milk10k.yaml`](../../config/bronze/datasets/milk10k.yaml) |
| Notebooks | [`notebooks/milk10k/`](../../notebooks/milk10k/) |
| `source_version` | `milk10k`, a provisional label until a real release tag is known |
| Archives | `milk10k.zip` (split `all`, [download](https://api.isic-archive.com/collections/425/)), with a `metadata.csv` |
| Status | Bronze and Silver run and verified on Databricks |

## Bronze metadata columns

`bronze.milk10k_source_metadata` keeps the source metadata as published, plus the standard lineage columns described in the [data contract](../data_contract.md#bronzedataset_source_metadata). All source columns are strings.

| Columns | Notes |
|---|---|
| `attribution`, `copyright_license` | Attribution and license, per image |
| `age_approx`, `sex` | Patient demographics |
| `anatom_site_1`, `anatom_site_2`, `anatom_site_special` | Anatomic site hierarchy (2 levels) |
| `diagnosis_1` … `diagnosis_4`, `diagnosis_confirm_type` | Diagnosis hierarchy (4 levels) and how it was confirmed |
| `concomitant_biopsy`, `melanocytic`, `image_type` | Source flags |
| `image_manipulation` | Only in MILK10k |
| `lesion_id` | Used for leakage grouping |

Compared with ISIC 2019, there's no `patient_id`, `clin_size_long_diam_mm`, `dermoscopic_type`, `family_hx_mm` or `personal_hx_mm`.

## Labels

The same shared `normalize_diagnosis_labels` as ISIC 2019, reading all four diagnosis levels:

- `malignancy` comes from `diagnosis_1`, mapped onto `benign`, `malignant` or `indeterminate`.
- `specific_diagnosis` is the deepest level that has a value.

A row where neither resolves is rejected.

## Leakage grouping

Exact duplicate images first, then `lesion_id`, otherwise a singleton. With no `patient_id`, images of the same patient's different lesions can't be grouped.

## Image validation

Uses the [default thresholds](README.md#default-image-validation-thresholds) unchanged, since it's the same photography domain as ISIC 2019.
