# 012. Pin metadata feature engineering per training run

Status: accepted.

## Summary

The Gold export can include each image's clinical metadata (age, sex, anatomic site and so on) as a CSV, as future input for a model that uses both images and metadata. Choosing and encoding those columns is part of what a model was trained on, so it's pinned the same way image preprocessing is: a versioned recipe, `config/metadata_preprocessing/<name>.yaml`, referenced from the training-run config and applied only when training loads the data. No training script uses it yet, so this sets up the mechanism rather than a working multimodal model.

## Context

The metadata CSV is an unmodified copy of Bronze rows. A model can't use it as-is: some columns should be dropped, categories need a fixed encoding, and missing numbers need a defined value. If those choices were made in an uncommitted notebook cell, nobody could later tell which metadata a model used or how it was encoded. That's the same gap [ADR 010](010-pin-data-and-preprocessing-per-training-run.md) closed for images.

## Decision

- **A versioned recipe** in `config/metadata_preprocessing/<name>.yaml` lists the columns to keep and how to encode each one: a fill value for missing numbers, or a fixed category list for one-hot encoding plus a bucket for unknown values. Any column not listed is dropped.
- **Applied only at load time** by `ml.metadata_preprocessing.build_metadata_transform`, the metadata equivalent of the image transforms. The exported CSV itself is never changed.
- **Pinned per training run** through an optional `metadata_preprocessing_version` in the training-run config. It's optional because the baseline model uses images only. It's resolved today, but not yet logged to MLflow or the registry; that comes with the first training script that uses it.
- **Datasets that name or code a field differently** are reconciled in the same recipe. A field can map each dataset to its own column name (`source_columns`) and remap its values onto a shared vocabulary (`value_map`), for example `M`/`F` onto `male`/`female`. The export adds a `dataset_key` column so rows from several datasets can be told apart.

## Alternatives considered

- **Encode metadata by hand in a notebook.** It's quick, but the result can't be reproduced.
- **Match differing column names automatically.** It's convenient, but it hides a decision that should be explicit and reviewable.

## Consequences

- A future multimodal model gets its feature vector and input size from the recipe alone.
- The dataset-joining code and the multimodal model itself aren't built yet. They'll be built together with the first real multimodal training script, so the design is tested against a real need.
- The reconciliation mapping can rename columns and relabel values one-to-one, but it can't derive a field from several columns or convert units. It also hasn't been needed yet, since ISIC 2019 and MILK10k use the same names for their shared fields. If a real case needs more, replace it with per-dataset functions, as the label mapping does.
