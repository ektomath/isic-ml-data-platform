# 002. Store Bronze image bytes as Delta table rows

Status: superseded by [006](006-stream-archives-no-blob-storage.md).

## Summary

The first Bronze design wrote every image into a Unity Catalog Volume as its own file, and a single ingestion run took 90 minutes and cost about $17. This decision replaced that with staging each archive on local disk and writing the image bytes into a Bronze Delta table in batches. It fixed the cost, but kept a second copy of every image. [ADR 006](006-stream-archives-no-blob-storage.md) later removed that copy too.

## Context

ISIC 2019 has about 33,000 images. On cloud object storage, the number of operations drives cost more than the number of bytes, so one write per image was the expensive part.

## Decision

- Copy each archive once from the landing Volume to local disk, and read and checksum the images there.
- Write image bytes to `bronze.isic_2019_image_blobs` in bounded batches. The first batch overwrites the table, and later batches append. The batch cap stops Spark Connect from putting gigabytes of data into a single query plan.
- If a run fails partway, rerun it from the start. Because the first batch overwrites, a rerun rebuilds the table cleanly. A staging table with an atomic swap wasn't worth it while ingestion ran by hand over two archives.

## Why it was superseded

The source archives already hold every image safely and are checksummed at ingestion, so the Delta copy added storage, and a large table for Silver and Gold to depend on, without adding safety. [ADR 006](006-stream-archives-no-blob-storage.md) streams images from the archives instead. The lesson about per-operation cost still stands: no layer writes one file per image.
