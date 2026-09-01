# 002. Store Bronze images as Delta blob rows

Status: accepted

The ISIC 2019 source archives contain tens of thousands of images. Extracting every archive member directly into a Unity Catalog Volume creates one cloud object per JPEG and turns ingestion into many small storage operations.

Bronze ingestion therefore stages each zip archive once from the landing Volume to local cluster disk, reads archive members locally, computes checksums locally, and writes the image bytes directly to `bronze.isic_2019_image_blobs` in bounded batches: the first batch overwrites the table, every batch after that appends. Metadata remains small and is copied separately for traceability.

The batch size is capped so Spark Connect does not embed a multi-gigabyte local relation in one query plan. This creates a small number of Delta writes while still avoiding one Volume object write per image.

This is a direct write, not a stage-then-atomically-replace pattern — there is no separate staging table and no all-or-nothing swap. If a batch fails partway through a run, `bronze.isic_2019_image_blobs` is left holding whatever batches from *this* run already succeeded, not the previous run's data (the first batch already overwrote that) and not a complete new dataset either. Recovery is to simply rerun the ingestion cell from the top: its first batch always overwrites again, so a full rerun cleanly discards whatever partial state was left and rebuilds the table from scratch across every archive — including any archive that had already finished before the failure, not just the one that failed.

This tradeoff was accepted deliberately rather than building a staging-table + atomic-replace pipeline, for two reasons specific to this project's current shape: archives are already staged to local disk by the time this write step runs, so a full rerun costs re-reading local disk and rewriting Delta rows, not the per-file Volume API calls that caused the original $17 ingestion cost (see `docs/lessons-learned.md`); and ingestion currently runs interactively, over a small, fixed number of archives (train + test), so a mid-run failure is something the operator sees and reruns, not something that could silently reach Silver unnoticed. Revisit this if either assumption stops holding — many more archives, a scale where a full redo gets expensive, or this running unattended (e.g. a scheduled job) where a partially-written table could go unnoticed before Silver reads it.

This keeps the source bytes in Bronze while avoiding per-image Volume writes during archive extraction. Silver and Gold should reference the Bronze image blob rows and should not duplicate image bytes.
