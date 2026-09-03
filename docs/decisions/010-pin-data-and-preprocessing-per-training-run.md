# 010. Pin dataset + preprocessing identity per training run

Status: accepted (design); implementation deferred until baseline training code exists

## Context

[ADR 001](001-preprocessing-at-runtime.md) already decided that preprocessing happens at training
time, not baked into stored bytes — `write_gold_shards_for_splits` writes raw archive bytes only,
with no decode/resize/normalize step. That's the right call for avoiding storage duplication (one
Gold shard export per `dataset_version` serves every `preprocessing_version` anyone tries against
it, not one export per combination), but it opens a gap: nothing about a Gold shard export, or
`gold.manifest_rows`, records which `preprocessing_version` a *specific trained model* actually
used. `manifest_rows.preprocessing_version` reflects whatever was the default when the manifest
was published — not a binding fact about any one training run, once preprocessing is freely
swappable at load time.

Several ways to close that gap were considered:

- Bake preprocessing back into the shard export path (e.g.
  `gold/<dataset_version>/<preprocessing_version>/shards/`) — rejected, since it reintroduces the
  exact storage duplication ADR 001 avoids.
- Rely on training code discipline alone (just remember to log both values) — rejected as
  unenforced; nothing stops a mismatched or missing log entry.
- A content hash over `(dataset_version, preprocessing_version, resolved preprocessing config)`
  logged as the model's sole identity — real extra rigor, but more moving parts than justified
  before there's even one real training run to validate the design against. Worth revisiting if
  the lighter design below ever proves insufficient.

The chosen combination (below) reuses two patterns already established elsewhere in this project
— a versioned config file per distinct thing (`config/bronze/datasets/`, `config/gold/manifests/`,
`config/gold/exports/`), and an audit-trail table populated per run (`bronze.ingestion_runs`,
`gold.manifest_registry`) — rather than introducing a new mechanism.

## Decision

1. **`config/gold/training_runs/<name>.yaml`** pins exactly one `dataset_version` to exactly one
   `preprocessing_version` — same convention as `manifests/<name>.yaml` and `exports/<name>.yaml`.
   The training entrypoint accepts only this one name, never two free-standing
   `dataset_version`/`preprocessing_version` parameters, so there is no code path to train against
   an unpinned pairing.
2. **`gold.training_run_registry`** records provenance per actual training run
   (`dataset_version`, `preprocessing_version`, an MLflow `run_id`, `created_at`) — the same
   audit-trail pattern as `bronze.ingestion_runs`/`gold.manifest_registry`.
3. **MLflow is the bridge, not written to directly from local code.** MLflow's tracking server is
   the one built into the Databricks workspace (no separate product/cost — see below); it's
   reachable from a local machine via its plain REST client
   (`mlflow.set_tracking_uri("databricks")` + a personal access token), with no Spark or
   Databricks Connect session required just to log a run. Every training run — local or on a
   Databricks cluster — logs hyperparameters, metrics, model artifacts, and the training-run
   config's name (or `dataset_version`/`preprocessing_version` directly) as MLflow run
   tags/params. `gold.training_run_registry` gets populated *from* MLflow runs (e.g. a small sync
   step, run wherever a live Spark session already exists), rather than written to directly by
   local training code — so a local run never needs its own Spark session just to log one audit
   row.
4. **MLflow's scope stays "what happened during training," not "what data was used."** Data/
   preprocessing identity is owned entirely by (1) and (2) above; MLflow owns hyperparameters,
   metrics, model artifacts, and model versioning — genuinely different concerns, not duplicated
   across both systems. Logging costs are ordinary compute-hours plus artifact storage (normal
   Blob storage rates) — nothing like the per-object Volume-write cost trap in
   `docs/lessons-learned.md`.

## Consequences

- **Not implemented yet, deliberately.** No `config/gold/training_runs/` directory and no
  `gold.training_run_registry` table exist in the codebase today. Building either now, with no
  training script to consume them, risks guessing at a shape that real training code later proves
  wrong (e.g. a model-architecture or hyperparameter-config version might turn out to belong in
  the pairing too) — the same "don't build ahead of a real consumer" discipline already applied in
  [ADR 009](009-gold-shard-retention-undecided.md) and [ADR 004](004-cross-dataset-leakage-not-checked.md).
  Build both alongside the first real training script (see `docs/project-checklist.md`'s Training
  section), not before it.
- Trying multiple preprocessing settings against the same image selection stays cheap: this design
  changes nothing about shard storage, since ADR 001 already keeps preprocessing out of the shard
  bytes — a new `preprocessing_version` just means a new `config/gold/training_runs/<name>.yaml`
  entry, not a new shard export.
- Local and Databricks training both go through the identical MLflow call path, so there's no
  local-only or Databricks-only branch for the audit trail — the only local-specific requirement
  is a personal access token, not a Databricks Connect setup.
