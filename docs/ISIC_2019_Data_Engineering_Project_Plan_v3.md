# Original Plan (Retrospective)

This is the original pre-implementation plan for this project, cut down to a short record of
what was planned versus what actually got built — kept for the portfolio value of showing that
comparison, not as current documentation. For up-to-date, authoritative documentation, see
[`AGENT.md`](../AGENT.md) (conventions and working state), [`docs/decisions/`](decisions/) (the
full reasoning behind every design change below), and [`docs/data_contract.md`](data_contract.md)
(schemas).

## Original outcome and scope

> A reviewer can trace a model run back to an immutable Gold manifest, a validated Silver
> inventory, and the exact Bronze image checksums used to create it.

MVP scope: Bronze ingestion → Silver validation → one Gold classifier manifest → one baseline
model → a public case study. Explicitly out of scope for the MVP: super-resolution pairs/
experiments (a deferred "Release 2"), multi-environment CI/CD, incremental ingestion from newer
ISIC collections, and production-grade operations (key rotation, SLAs/paging, Kubernetes, feature
stores) — not omissions, deliberate boundaries set from the start.

## What changed during implementation

The plan's mechanics evolved substantially while building it — expected for a project developed
iteratively with real engineering judgment, not executed blindly against an upfront spec:

- **Bronze stopped storing image bytes at all.** Originally planned as image bytes in Delta table
  rows; the source archives are now the sole permanent store, with Bronze holding only checksums
  and archive locators. See [ADR 006](decisions/006-stream-archives-no-blob-storage.md) (supersedes
  [ADR 002](decisions/002-store-bronze-images-as-delta-blobs.md)).
- **The Gold manifest publishes as a Delta table, not the originally planned Parquet file** —
  the same portability, plus native `MERGE`/versioning for multiple releases coexisting in one
  table, without separate file-sync logic.
- **The local training cache became derived MosaicML shards, not a raw per-image SSD sync.**
  See ADR 006.
- **Orchestration is notebook-first, not CLI-stages-plus-a-Databricks-Job.** `src/data_platform/ingest.py`/`publish.py`
  are empty placeholders; the real logic lives in `src/data_platform/` and the numbered notebooks,
  run manually in sequence — real orchestration is still open (see `docs/project-checklist.md`'s
  Automation section).
- **Bronze reruns fully rebuild the table rather than skipping unchanged images** — a deliberate
  scale tradeoff (archives are already staged locally, so a full rebuild re-reads local disk
  rather than repeating the costly per-object cloud writes that originally motivated this design),
  not an oversight. See ADR 002.

## Original source references

- [ISIC challenge datasets](https://challenge.isic-archive.com/data/)
- [ISIC 2019 challenge](https://challenge.isic-archive.com/landing/2019/)
- [ISIC API bulk-download guidance](https://api.isic-archive.com/api/docs/swagger/)
- [Official isic-cli](https://github.com/ImageMarkup/isic-cli)
- [ISIC AWS Open Data listing](https://registry.opendata.aws/isic-archive/)
