# Datasets

One page per onboarded dataset. Everything specific to a dataset lives on its page: where it comes from, its archives, its Bronze metadata columns, how its labels map onto the shared Silver label columns, and its image validation thresholds. The shared tables and rules are in [data_contract.md](../data_contract.md).

| Dataset | Images from | Splits | Page |
|---|---|---|---|
| `isic_2019` | ISIC 2019 Challenge release | `train`, `test` | [isic_2019.md](isic_2019.md) |
| `milk10k` | MILK10k release on the ISIC Archive | `all` | [milk10k.md](milk10k.md) |

## Adding a dataset

Datasets arrive in different shapes and packaging, so each one gets its own notebooks for the parts that depend on that. Everything else is shared code in `src/data_platform/`.

1. **Config:** `config/bronze/datasets/<dataset_key>.yaml` lists the archives and splits, the source version, and which metadata columns feed the labels.
2. **Bronze notebooks** (`notebooks/<dataset_key>/05_…` and `10_…`): create the dataset's Bronze tables and ingest it. The source metadata schema lives here, because it's specific to each source. Archive staging, checksumming and the image index are shared functions.
3. **Silver notebook** (`notebooks/<dataset_key>/20_…`): mostly configuration. It points the shared validation, label and leakage functions at the dataset's config. A dataset whose labels don't follow the ISIC `diagnosis_1…N` convention supplies its own label mapping here.
4. **This folder:** add a page for the dataset, and a row to the table above.

The current Bronze code assumes each source is a set of zip archives with one metadata CSV each. A dataset delivered another way, for example through an API, needs its own ingestion step.

## Default image validation thresholds

Set in `src/data_platform/validate.py`. A dataset only overrides them, by passing `min_dimension`/`max_dimension` to `validate_images` from its Silver notebook, when its images are a structurally different kind of thing. The override and its reason go on the dataset's page.

| Rule | Default | Meaning |
|---|---|---|
| `MIN_DIMENSION` | 50 px | Images narrower or shorter than this are rejected as likely corrupt |
| `MAX_DIMENSION` | 15,000 px | Images wider or taller than this are rejected as likely corrupt, or the wrong kind of file |

These are sanity guard rails for close-up dermoscopy and clinical photos, not values derived from the real size distribution. A different domain, such as whole-slide histopathology scans that routinely exceed 15,000 px, would need its own limits.
