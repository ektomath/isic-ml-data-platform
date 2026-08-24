# Architecture

This project uses a Bronze / Silver / Gold medallion layout on Azure Data Lake Storage Gen2, with Databricks handling ingestion, validation, transformation, and model training.

## System overview

![ISIC platform architecture](assets/architecture.png)

The design keeps the original JPEGs in Bronze and uses Silver and Gold primarily for tabular outputs, manifests, and quality records that reference the Bronze assets.

## Azure storage layout

One Azure storage container is used for the physical medallion file layout:

```text
isic-data/
  bronze/isic/2019/
    images/
    metadata/
    ingestion_runs/
  silver/isic/2019/
    image_inventory/
    labels/
    leakage_groups/
    rejected_records/
  gold/classification/v1/
    manifest.parquet
    preprocessing.yaml
    dataset_card.md
```

### Layer responsibilities

| Layer | Responsibility | Typical contents |
|---|---|---|
| Bronze | Preserve the source faithfully | Original JPEGs, raw metadata, raw labels, ingestion logs, checksums |
| Silver | Produce a trustworthy canonical view | Image inventory, normalized labels, validation results, leakage-control groups, rejected rows |
| Gold | Publish training-ready data products | Versioned manifest, preprocessing config, dataset card, split metadata |

## Databricks structure

When Unity Catalog is available, use a dedicated catalog for the project tables:

```text
Catalog: isic_showcase
  Schema: bronze
    Tables:
      ingestion_runs
      isic_2019_source_metadata
  Schema: silver
    Tables:
      image_inventory
      labels
      leakage_groups
      rejected_records
  Schema: gold
    Tables:
      classification_manifest
```

The catalog holds logical tabular assets. The physical image files, raw metadata files, manifests, and other file outputs remain in Azure storage paths.

In other words:

- Azure storage layout describes where files live.
- Databricks / Unity Catalog structure describes where tables live.

## Databricks jobs

The first implementation should stay small:

1. Bronze ingestion job: ingest metadata, labels, and source-image references.
2. Silver validation job: validate images, normalize metadata, and assign leakage-control groups.
3. Gold publishing job: generate a deterministic classification manifest and dataset card.
4. Training job: consume the Gold manifest, build the local or mounted training dataset, and track metrics in MLflow.

Job definitions do not need to be committed until deployment begins, but the code should already assume this structure.

## Reproducibility rules

- Original JPEGs are immutable after Bronze ingestion.
- Silver and Gold records reference Bronze paths rather than duplicating image bytes.
- Preprocessing settings are versioned separately from the model code.
- Every Gold release must be traceable to the Git commit, pipeline config, preprocessing config, and source-image checksums used to produce it.
- Fixture tests should use only small local files and should not download ISIC data.
