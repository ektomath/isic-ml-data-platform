# 004. Stream images from the source archives; store image bytes nowhere else

Status: accepted. How long shard exports are kept is a separate, open decision ([006](006-gold-shard-retention-undecided.md)).

## Summary

The source zip archives in the landing Volume are the only permanent copy of image bytes. Every step that needs pixels (Silver validation and Gold shard export) streams them straight from the archive, and the tables hold only checksums and a pointer into the archive. The trade-off is that each of those steps re-reads the archives when it runs.

## Context

The archives are already durable, never modified, and checksummed at ingestion ([ADR 005](005-immutable-source-archives-checksum-verified.md)). Any other place that holds image bytes is a second copy of the same data, which costs storage and has to be kept consistent with the original. How the bytes are stored also matters for cost: on cloud object storage, the number of operations drives cost more than the number of bytes, so anything that writes one object per image is slow and expensive at tens of thousands of images.

## Decision

- **No layer stores image bytes.** Bronze keeps an image index: checksum, size and location in the archive for every image. It reads each archive once to compute checksums, then discards the bytes.
- **Every image has a resolvable pointer.** `bronze_uri` has the form `archive:<archive path>#<path inside the zip>`, which is enough for any later step to find the image's bytes without going back to Bronze.
- **Archives are staged once on local disk**, and images are read from there, so no step makes one cloud request per image.
- **Silver validates by streaming.** Each candidate image is read from the staged archive, decoded, checked and discarded.
- **Gold's shard export is the one place bytes are written again.** It streams a published manifest's images into MosaicML shards for training. Shards are a derived cache that can always be rebuilt identically from the manifest and archives, so rerunning the export simply rewrites them.
- **No image is ever written to a Volume as an individual file**, at any layer.

## Alternatives considered

- **Extract every image into a Volume as its own file.** Simple, but it means one cloud write per image, about 33,000 for ISIC 2019, which makes ingestion slow and expensive (the README's Lessons learned section has the measured cost). It also duplicates every image.
- **Store image bytes as rows in a Bronze Delta table**, written in batches from locally staged archives. This avoids the per-image writes, but still duplicates every image and makes Silver and Gold depend on a large, growing table.

## Consequences

- Silver validation and shard export each re-read the relevant archives when they run. That's a bounded cost, paid in exchange for never maintaining a second copy of the data.
- The same principle applies to future work: persist a new copy of data only when there's a concrete, current reason, and as late in the pipeline as possible, treating it as a rebuildable cache. Shard export is the one place that bar is met today, because training needs actual files rather than a Spark session. If reading the archives repeatedly ever becomes the bottleneck, revisit that trade-off, not the principle.
- Shard export depends on `mosaicml-streaming`, which brings in a heavy dependency tree (`torch`, `torchvision`, cloud SDKs). That's accepted in exchange for not writing a custom shard format.
