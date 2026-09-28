# Architecture: where things live

Where the platform's files and tables physically live in Databricks. For how data moves between them, see [how-it-works.md](how-it-works.md); for every table's columns, see [data_contract.md](data_contract.md).

## Files: two Unity Catalog Volumes

```text
/Volumes/derm_showcase_project/bronze/landing/
  archives/
    isic_2019/          source zip archives, the only copy of image bytes
    milk10k/
/Volumes/derm_showcase_project/bronze/files/
  isic_2019/
    metadata/           metadata CSVs extracted from each archive
  milk10k/
    metadata/
  gold/sample-v1/
    metadata/           optional per-dataset metadata CSVs for the release
    shards/             MosaicML shards, one folder per split (rebuildable cache)
      train/
      validation/
      test/
```

Silver has no folders; it's tables only.

## Tables: one catalog, a schema per layer

```text
Catalog: derm_showcase_project
  Schema: bronze
    Tables:
      ingestion_runs
      <dataset>_image_index        one pair per dataset, e.g. isic_2019_*, milk10k_*
      <dataset>_source_metadata
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
    Models:
      baseline_classifier          registered by training
```

Training runs themselves live in MLflow, not in a table.

Every stage is a numbered notebook, run by hand in order ([running.md](running.md)). Scheduling them as Databricks Jobs is future work.
