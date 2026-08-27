# Lessons Learned

This document tracks concrete some concrete lessons I have learned form this project

## Cloud storage costs

The first version of Bronze ingestion extracted each archive member (image) directly into a Unity Catalog Volume, i.e. one cloud object write per JPEG. For the ISIC 2019 train & test split (~33k images) this turned ingestion into tens of thousands of small storage operations and cost roughly $17 for a single run.

**Lesson:** on cloud object storage, operation count costs more than byte count, which should be taken into account.

**Fix:** stage the archive once from the landing Volume to local cluster disk, extract and read members from local disk, and write image bytes as binary rows into a Delta table instead of one object per image. See `docs/decisions/002-store-bronze-images-as-delta-blobs.md` and `data_platform.files.iter_image_blob_rows`.
