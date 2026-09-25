# 012. Pin metadata feature engineering per training run

Status: accepted

## Context

`notebooks/31_export_gold_shards.ipynb`'s optional `export_metadata_csv` step (added alongside
this ADR) writes each `dataset_key`'s raw Bronze source metadata (`age_approx`, `sex`,
`anatom_site_*`, `diagnosis_*`, `image_type`, etc. — see `docs/data_contract.md`'s Bronze
contracts) for a release's images to a local-downloadable CSV, intended as auxiliary input to a
future multimodal (image + tabular/text) model, not just for ad hoc inspection.

That export is a straight, unmodified passthrough of immutable Bronze rows — the same posture as
the Gold shard export itself. But a real model doesn't consume every raw column as-is: some
fields are irrelevant and should be dropped, categorical fields need a fixed vocabulary encoding,
numeric fields need a defined answer for a null/unparseable value. If that column
selection/encoding work happens as a local, unversioned step against the downloaded CSV — pick
some columns, one-hot others by hand, decide on a null-handling convention in a notebook cell
that never gets committed — a trained model's reported metrics stop being reproducible from
anything in this repo. Nobody looking at a trained model later could tell which metadata fields,
or which encoding of them, it actually used. That's exactly the gap
[ADR 001](001-preprocessing-at-runtime.md)/[ADR 010](010-pin-data-and-preprocessing-per-training-run.md)
already closed for image preprocessing, just reopened for metadata.

## Decision

Apply the same two-part pattern ADR 001/010 already established for image preprocessing, to
metadata feature engineering:

1. **`config/metadata_preprocessing/<name>.yaml`** is a versioned, reviewable recipe: which
   Bronze columns to keep (everything else is implicitly dropped), and how to encode each one —
   a numeric field's null/unparseable sentinel, or a categorical field's fixed vocabulary (order
   fixes each category's one-hot slot, never derived from a scan) plus its unknown-value bucket.
   Loaded and validated by `ml.metadata_preprocessing.load_metadata_preprocessing_config`. Named
   `metadata_preprocessing`, not just `metadata`, to match `config/preprocessing/<name>.yaml`'s
   own naming — both are the same kind of thing (a versioned, pinned recipe resolved into a
   runtime transform at training time), just for a different input.
2. **`ml.metadata_preprocessing.build_metadata_transform`** is the one place a loaded config
   turns into an actual runtime feature-vector callable — applied at training/inference load time
   only, exactly mirroring `ml.preprocessing.build_transforms`'s role for images. Never applied to
   the exported CSV itself; the CSV stays a raw, unmodified passthrough.
3. **`config/gold/training_runs/<name>.yaml`** gains an optional `metadata_preprocessing_version`
   field, resolved by `ml.training_run.resolve_training_run` alongside `preprocessing_version`.
   Optional because most training runs (the current baseline ResNet-18 classifier) are
   image-only; a multimodal training run pins it the same way `preprocessing_version` is already
   pinned. It is resolved today but not yet logged to MLflow or `gold.training_run_registry`,
   since no training script consumes it yet; logging it is part of adding a multimodal run.

## Consequences

- The metadata CSV export itself needed no change beyond stamping on a `dataset_key` column (see
  the cross-dataset addendum below) — it was already a raw passthrough, never the place column
  selection was happening.
- A multimodal model's training code (not yet written — this ADR only lands the config
  mechanism a future one builds on) resolves its metadata feature vector from
  `metadata_preprocessing_version` alone, the same way `ml.dataset.build_dataloader` already
  resolves image transforms from `preprocessing_version` alone.
- `ml.metadata_preprocessing.metadata_feature_dim` gives a multimodal model's input layer its
  fixed size directly from the config, rather than inferring it from a batch at runtime.
- Deliberately not implemented yet: any actual multimodal model architecture, or a
  `GoldShardDataset`-equivalent that joins shard samples to metadata rows by `image_id` at
  `__getitem__` time. Building either without a real model to validate the shape against would
  risk the same premature-guess problem [ADR 009](009-gold-shard-retention-undecided.md) and
  [ADR 004](004-cross-dataset-leakage-not-checked.md) already avoided elsewhere — build them
  alongside the first real multimodal training script, the same way ADR 011 built the training
  registry alongside the first real (image-only) one.

## Addendum: cross-dataset field reconciliation

A release combining more than one `dataset_key` (e.g. `isic_2019` and `milk10k`) can have the
same conceptual field under a different raw column name, or under matching names but
non-matching raw values (e.g. `"M"/"F"` vs `"male"/"female"`). Left unhandled, a single
`metadata_preprocessing_version` covering both datasets would need a lossy union of unrelated
columns, or silently produce wrong feature vectors for whichever dataset's raw shape it didn't
match.

This is the same reconciliation problem `apply_label_normalization` (`data_platform/spark_io.py`)
already solves for labels — dataset-specific raw columns feeding a shared canonical output — so
the fix follows that precedent in spirit:

- Each field spec in `config/metadata_preprocessing/<name>.yaml` may optionally carry
  `source_columns` (`{dataset_key: raw_column_name}`) and `value_map`
  (`{dataset_key: {raw_value: canonical_value}}`). Both are no-ops for a `dataset_key` not listed
  in them, so a single-dataset config, or a field that already agrees across every dataset in a
  release, needs neither.
- `export_source_metadata_csv` (`data_platform/spark_io.py`) now stamps a `dataset_key` column
  onto its output — it's already scoped to one dataset per call, so this didn't exist before —
  giving `ml.metadata_preprocessing.build_metadata_transform` something to key its per-field
  reconciliation off of when rows from more than one dataset's CSV get combined.
- Resolution order per field, per row: resolve the actual source column for this row's
  `dataset_key` (falling back to the canonical `name`), read the raw value, then remap it through
  `value_map` if this `dataset_key` has one. Encoding (numeric parse, categorical one-hot) happens
  after, unchanged.

Still deliberately not built: anything that reconciles column *names* automatically (e.g. fuzzy
matching) — every mapping here is explicit, reviewable config, matching this ADR's whole premise
that this is a decision worth pinning, not inferring.

**Caveat, added on review:** unlike `apply_label_normalization`, which delegates to a real,
dataset-specific Python function (`normalize_diagnosis_labels` takes `*diagnosis_levels`,
proven necessary because isic_2019 has 5 diagnosis levels and milk10k has 4 — a shape only a
callable can express), `source_columns`/`value_map` is a static declarative dict: it can rename a
column or relabel one value 1:1, but it cannot derive a canonical field from two raw columns or
apply a differently-bucketed/unit-converted transform per dataset. It is a shallower mechanism
than the precedent it follows, not an equal one. It's also, as of this writing, unvalidated
against a real need — milk10k's actual Bronze schema already uses the same column names as
isic_2019 (`sex`, `age_approx`, `anatom_site_*`, `melanocytic`; see
`notebooks/milk10k/10_bronze_ingest.ipynb`), so nothing in this repo has yet required
`source_columns`/`value_map` for real. Left in place as forward-looking scaffolding rather than
reverted, but the first real mismatch that needs *derivation* rather than renaming should prompt
replacing this with a callable-based mechanism (mirroring `labels.py`'s pattern) instead of
extending the dict shape further.
