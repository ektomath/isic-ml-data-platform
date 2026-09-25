# 003. Store labels as named columns on one shared Silver table

Status: accepted.

## Summary

Each label axis, such as `malignancy` or `specific_diagnosis`, is a real, commented column on the shared `silver.image_inventory` table. A dataset that needs a new axis adds a nullable column. This keeps labels visible in Unity Catalog's explorer and `information_schema`, at the cost of the table's schema growing as datasets are added.

## Context

Every image needs labels, but datasets don't all label the same things. ISIC 2019 and MILK10k both give malignancy and a specific diagnosis; a future dataset might add severity or body site. The storage has to handle that without losing information or hiding it.

## Decision

- Label axes are named, commented columns on `silver.image_inventory`.
- A new axis is added with `ALTER TABLE ... ADD COLUMNS`. That's a metadata-only change in Delta, and the new column is null for datasets that don't populate it.
- Only axes meant to be compared across datasets get an enforced vocabulary. Today that's `malignancy` (`benign`, `malignant`, `indeterminate`).

## Alternatives considered

- **One label string per image.** Using the coarsest value throws away detail, and using the most specific value mixes granularities from row to row.
- **A `MAP<STRING, STRING>` column.** No schema changes are needed, but Unity Catalog shows the map as one opaque column, so nobody can see which labels exist without reading separate docs.
- **One label table per dataset.** The columns stay visible, but the tables multiply, and finding them needs a hand-maintained registry that duplicates what the metastore already provides.

## Consequences

- The table gains columns as new label axes appear. In practice, labeling schemes converge on a small shared vocabulary, so most new datasets reuse existing columns. Even many unique axes are well within what Delta handles.
