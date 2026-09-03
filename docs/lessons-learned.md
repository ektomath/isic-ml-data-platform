# Lessons Learned

This document tracks some concrete lessons I have learned from this project.

## Cloud storage costs

The first version of Bronze ingestion extracted each archive member (image) directly into a Unity Catalog Volume, i.e. one cloud object write per JPEG. For the ISIC 2019 train & test split (~33k images) this turned ingestion into tens of thousands of small storage operations and cost roughly $17 for a single run.

**Lesson:** on cloud object storage, operation count costs more than byte count, which should be taken into account.

**Fix:** stage the archive once from the landing Volume to local cluster disk, and extract and read members from local disk instead of one object per image. The original fix additionally wrote image bytes as binary rows into a Delta table (`docs/decisions/002-store-bronze-images-as-delta-blobs.md`); that part was later superseded — no image bytes are stored anywhere at any layer now, not even as Delta rows, since the archives already hold them safely and checksummably (`docs/decisions/006-stream-archives-no-blob-storage.md`). See `data_platform.files.iter_archive_image_rows`.

**Side note**  As a bonus, notebook runtime dropped from 90 minutes to 5
