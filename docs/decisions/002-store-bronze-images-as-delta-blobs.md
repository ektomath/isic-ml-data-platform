# 002. Store Bronze images as Delta blob rows

Status: accepted

The ISIC 2019 source archives contain tens of thousands of images. Extracting every archive member directly into a Unity Catalog Volume creates one cloud object per JPEG and turns ingestion into many small storage operations.

Bronze ingestion therefore stages each zip archive once from the landing Volume to local cluster disk, reads archive members locally, computes checksums locally, and writes the image bytes to a run-specific staging Delta table with bounded Spark table batches. After all batches succeed, the job replaces `bronze.isic_2019_image_blobs` from the complete staging table. Metadata remains small and is copied separately for traceability.

The batch size is capped so Spark Connect does not embed a multi-gigabyte local relation in one query plan. This creates a small number of Delta writes while still avoiding one Volume object write per image, and it avoids truncating the production image table before a full replacement is ready.

This keeps the source bytes in Bronze while avoiding per-image Volume writes during archive extraction. Silver and Gold should reference the Bronze image blob rows and should not duplicate image bytes.
