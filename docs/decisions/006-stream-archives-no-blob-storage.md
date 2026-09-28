# 006. Stream images from the source archives; store image bytes nowhere else

Status: accepted. Supersedes [002](002-store-bronze-images-as-delta-blobs.md). The retention point is amended by [009](009-gold-shard-retention-undecided.md).

## Summary

Image bytes were stored twice: once in the original source archives and again in a Bronze Delta table. Now the archives are the only permanent copy. Every step that needs pixels (Silver validation and Gold shard export) streams them straight from the archive, and the tables hold only checksums and a pointer into the archive. The trade-off is that each of those steps re-reads the archives when it runs.

## Context

The first Bronze design extracted every image into a Unity Catalog Volume as its own file. For about 33,000 ISIC 2019 images that meant about 33,000 separate cloud writes, and a run took 90 minutes and cost roughly $17, because cloud storage charges per operation, not per byte. On a personal budget, that's too much to rerun freely. [ADR 002](002-store-bronze-images-as-delta-blobs.md) fixed that by staging each archive on local disk and writing the image bytes into a Bronze Delta table instead.

That solved the cost problem but left the bytes in two places: the archives in the landing Volume and the Bronze table. Silver then joined that table to validate images, and Gold would have needed it again for export. The second copy wasn't buying anything. The archives are already durable, never modified, and checksummed at ingestion ([ADR 008](008-immutable-source-archives-checksum-verified.md)).

## Decision

- **No layer stores image bytes.** The Bronze blob table becomes an image index: the same rows without the bytes, just checksum, size and location in the archive. Bronze still reads each archive once to compute checksums, then discards the bytes.
- **Every image has a resolvable pointer.** `bronze_uri` has the form `archive:<archive path>#<path inside the zip>`, which is enough for any later step to find the image's bytes without going back to Bronze.
- **Silver validates by streaming.** Each candidate image is read from the staged archive, decoded, checked and discarded.
- **Gold's shard export is the one place bytes are written again.** It streams a published manifest's images into MosaicML shards for training. Shards are a derived cache that can always be rebuilt identically from the manifest and archives, so rerunning the export simply rewrites them.
- **No image is ever written to a Volume as an individual file**, at any layer.

## Alternatives considered

- **Keep the Bronze blob table (ADR 002).** It works, but duplicates every image and makes Silver and Gold depend on a large, growing table.
- **Extract images to individual files.** This was the original design, and it's what caused the per-operation cost.

## Consequences

- Silver validation and shard export each re-read the relevant archives when they run. That's a bounded cost, paid in exchange for never maintaining a second copy of the data.
- Adopting this meant rerunning Bronze and Silver on the new design and dropping the old blob table. Both were rerun and verified on Databricks for ISIC 2019 and MILK10k.
- The same principle applies to future work: persist a new copy of data only when there's a concrete, current reason, and as late in the pipeline as possible, treating it as a rebuildable cache. Shard export is the one place that bar is met today, because training needs actual files rather than a Spark session. If reading the archives repeatedly ever becomes the bottleneck, revisit that trade-off, not the principle.
- Shard export depends on `mosaicml-streaming`, which brings in a heavy dependency tree (`torch`, `torchvision`, cloud SDKs). That's accepted in exchange for not writing a custom shard format.
