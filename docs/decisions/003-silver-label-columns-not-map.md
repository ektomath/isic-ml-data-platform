# 003. Silver label columns, not a map column or per-dataset tables

Status: accepted

`silver.image_inventory` needs a canonical label per image, but different datasets will need different label axes (ISIC 2019 needs malignancy and specific diagnosis; a future dataset might need severity or body site). Three alternatives were considered.

A single fixed-granularity string (always the coarsest available value) throws away information a later Gold product might want. The deepest-available value in one string mixes granularities row to row and confuses any consumer reading the column directly.

A `MAP<STRING, STRING>` column avoids a schema change per dataset, but it defeats Unity Catalog's native discoverability: Catalog Explorer, column comments, and `information_schema` all describe real columns, not the keys inside a map value. A map shows up as one opaque column with no way to see what is inside without already knowing to look, or reading external docs that can drift out of sync.

A labels table per dataset keeps real, discoverable columns, but creates table proliferation, and the natural fix — a hand-built registry table listing datasets and their label columns — would just duplicate what Unity Catalog's own metastore already provides for free, and would need to be kept in sync by hand.

Bronze ingestion therefore stores label axes as real, named, commented columns directly on the single shared `silver.image_inventory` table (`malignancy`, `specific_diagnosis` for ISIC 2019), and grows that schema with `ALTER TABLE ... ADD COLUMNS` when a future dataset needs an axis this one does not have. New columns are nullable for every dataset that does not populate them; this is a Delta metadata-only operation, not a rewrite.

Column growth is not expected to be a real problem even at a much larger number of datasets: labeling schemes converge onto a small, shared vocabulary in practice, so most future datasets populate existing columns rather than adding new ones, and even a pessimistic case of many genuinely unique axes stays well within what Delta/Parquet handle without strain.
