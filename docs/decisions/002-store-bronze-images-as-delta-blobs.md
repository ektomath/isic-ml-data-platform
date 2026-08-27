# 002. Store Bronze images as Delta blob rows

Status: accepted

The ISIC 2019 source archives contain tens of thousands of images. Extracting every archive member directly into a Unity Catalog Volume creates one cloud object per JPEG and turns ingestion into many small storage operations.

Bronze ingestion therefore stages each zip archive once from the landing Volume to local cluster disk, reads archive members locally, computes checksums locally, and writes the image bytes to `bronze.isic_2019_image_blobs` with one Spark table write. Metadata remains small and is copied separately for traceability.

This keeps the source bytes in Bronze while avoiding per-image Volume writes during archive extraction. Silver and Gold should reference the Bronze image blob rows and should not duplicate image bytes.
