# 009. Metadata feature engineering waits for a model that uses it

Status: accepted.

## Summary

The Gold export can include each image's clinical metadata (age, sex, anatomic site and so on) as a CSV, as future input for a model that uses both images and metadata. How those columns are chosen and encoded will be pinned per training run, like image preprocessing, but that's built together with the first model that uses metadata, not ahead of it.

## Context

The metadata CSV is an unmodified copy of Bronze rows. A model can't use it as-is: columns need choosing, categories need a fixed encoding, and missing numbers need a defined value. Those choices are part of what a model was trained on, so they need the same treatment [ADR 007](007-pin-data-and-preprocessing-per-training-run.md) gives image preprocessing. But no model uses metadata yet, so anything built now would be designed around guessed needs.

## Decision

- **The export keeps the metadata available:** one CSV per dataset per release, joinable on `image_id`, with a `dataset_key` column so rows from several datasets can be told apart.
- **No feature-encoding code or config exists** until the first model that uses metadata.
- **When that model is built,** its encoding goes in a versioned recipe referenced from the training-run config and saved to MLflow with the run, as the image recipe is. It's applied when training loads the data; the exported CSV is never changed.

## Alternatives considered

- **Build the recipe and encoder now.** An earlier version did: a versioned config with fixed category lists and per-dataset column mapping, plus the code to apply it. It was removed because no model read it, its category lists were never checked against real exported values, and its only effect at runtime was rejecting training configs that set it.
- **Encode metadata by hand in a notebook.** It's quick, but the result can't be reproduced.

## Consequences

- Training is image-only; the metadata CSV is only useful for analysis until a multimodal model exists.
- Some fields are named or coded differently across datasets. Reconciling them is part of that future work, which the `dataset_key` column makes possible.
