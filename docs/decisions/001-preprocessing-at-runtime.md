# 001. Apply image preprocessing at training time, not in storage

Status: accepted.

## Summary

Stored data keeps the original, unprocessed image bytes. Resizing, normalization and augmentation happen when training loads an image. One stored copy then serves any number of preprocessing experiments, at the cost of repeating that work on every training run.

## Context

Models differ in the input size and normalization they expect, and trying different preprocessing is a normal part of training. If preprocessing were applied before storage, each variant would need its own stored copy of every image, and it would be harder to tell which variant a model was trained on.

## Decision

- Bronze, Silver and Gold never transform pixels. The Gold shard export carries the original encoded bytes ([ADR 006](006-stream-archives-no-blob-storage.md)).
- Preprocessing is a versioned recipe in `config/preprocessing/<name>.yaml`, turned into transforms when training loads the data.

## Alternatives considered

- **Store preprocessed images.** This makes loading faster, but needs one stored copy per preprocessing variant and bakes in choices that might change.

## Consequences

- Decoding and resizing are paid on every training run instead of once.
- Which preprocessing a model used has to be recorded per training run, not per dataset. [ADR 010](010-pin-data-and-preprocessing-per-training-run.md) does that.
