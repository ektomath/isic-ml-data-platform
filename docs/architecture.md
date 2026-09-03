# Architecture

This project uses a Bronze / Silver / Gold medallion layout on Databricks Unity Catalog, with Databricks handling ingestion, validation, transformation, and model training.

## System overview

![ISIC platform architecture](assets/architecture.svg)

The design keeps the original image bytes solely in the landing-volume source archives — never in a Bronze/Silver table, and never extracted as individual Volume objects at any layer (see `docs/decisions/006-stream-archives-no-blob-storage.md`). Bronze/Silver hold only checksums and archive locators; every layer that ever needs actual bytes (Silver validation, Gold shard export) streams them directly from the archive. Silver and Gold otherwise hold tabular outputs, manifests, and quality records that reference the archives via an archive-resolvable `bronze_uri`.

## Storage layout

Two Unity Catalog volumes are used for file storage:

```text
/Volumes/derm_showcase_project/bronze/landing/
  archives/
    isic_2019/
/Volumes/derm_showcase_project/bronze/files/
  isic_2019/
    metadata/
  gold/sample-v1/
    shards/
      train/
      validation/
      test/
```

Silver (`silver.image_inventory`, `silver.leakage_groups`, `silver.rejected_records`) is table-only — Delta managed tables, no Volume folders — so it does not appear in this file layout. The `gold/<dataset_version>/shards/` tree is a derived, fully rebuildable cache written by `notebooks/31_export_gold_shards.ipynb` (MosaicML shards, one directory per split) — see the Reproducibility rules below. How long a given export is kept around is an open question, not yet decided (`docs/decisions/009-gold-shard-retention-undecided.md`).

### Layer responsibilities

| Layer | Responsibility | Typical contents |
|---|---|---|
| Bronze | Preserve the source faithfully | Image index (checksums, archive locators — no image bytes), raw metadata, raw labels, ingestion logs |
| Silver | Produce a trustworthy canonical view | Image inventory, normalized labels, validation results, leakage-control groups, rejected rows |
| Gold | Publish training-ready data products | Versioned manifest, preprocessing config, dataset card, derived per-release MosaicML shard export |

## Databricks structure

Use a dedicated catalog for the project tables and keep file storage in volumes:

```text
Catalog: derm_showcase_project
  Schema: bronze
    Tables:
      ingestion_runs
      isic_2019_image_index
      isic_2019_source_metadata
    Volumes:
      landing
      files
  Schema: silver
    Tables:
      image_inventory
      leakage_groups
      rejected_records
  Schema: gold
    Tables:
      manifest_rows
    Views:
      manifest_registry
```

The catalog holds logical tabular assets. Source archives stay in the landing Volume as the sole permanent store of image bytes — no image bytes are ever written to a table, and no image is ever materialized as its own object in a Volume (see `docs/decisions/006-stream-archives-no-blob-storage.md`). Raw metadata files, manifests, and the derived Gold shard export remain in Unity Catalog volume paths.

In other words:

- Unity Catalog volume layout describes where files live.
- Databricks / Unity Catalog structure describes where tables live.

## Databricks jobs

The first implementation should stay small:

1. Bronze ingestion job: stage each archive locally, index each image's checksum/archive locator into a Bronze table (no image bytes retained), and ingest metadata and source-image references.
2. Silver validation job: stream candidate images directly from their source archives to validate them, normalize metadata, and assign leakage-control groups.
3. Gold publishing job: generate a deterministic classification manifest and dataset card.
4. Gold shard export job: stream a published manifest's images from their source archives into a derived, per-release MosaicML shard export.
5. Training job: consume the Gold shard export (locally or via a mounted Volume on a Databricks ML cluster), and track metrics in MLflow.

Job definitions do not need to be committed until deployment begins, but the code should already assume this structure.

## Reproducibility rules

- Original source image bytes are immutable in the landing-volume archives, the sole permanent store of image bytes at any layer. This is checked, not just assumed: Silver validation and Gold shard export both re-verify each streamed image's checksum against the one Bronze originally recorded, and fail loudly on any mismatch rather than silently resolving `bronze_uri` to different bytes than what was actually validated or manifested — see `docs/decisions/008-immutable-source-archives-checksum-verified.md`.
- Silver and Gold records reference the source archive (via an archive-resolvable `bronze_uri`) rather than duplicating image bytes into a table. The one deliberate, scoped exception is the Gold shard export — a derived, fully rebuildable cache, never a second source of truth (see `docs/decisions/006-stream-archives-no-blob-storage.md`). How long it's kept around is a separate, currently undecided question (`docs/decisions/009-gold-shard-retention-undecided.md`).
- Preprocessing settings are versioned separately from the model code.
- Every Gold release must be traceable to the Git commit, pipeline config, preprocessing config, and source-image checksums used to produce it.
- Fixture tests should use only small local files and should not download ISIC data.
