# ISIC ML Data Platform

I wanted to explore how I would store and catalog large numbers of skin-cancer image datasets for ML training using Databricks, so that I could train classification models on any mix of them. I tried to keep two things in mind: the same image, or images of the same lesion or patient, should never end up in both the training and the test data, and for any model I train, I should be able to rebuild exactly the images, labels and preprocessing it was trained on.

So far it has been tested with two datasets from the [ISIC Archive](https://www.isic-archive.com/): ISIC 2019 and MILK10k. The data itself isn't in this repository, and nothing in the design is tied to those two. In principle it could take in every dataset the ISIC Archive hosts, or other public dermatology collections, with a config file and a couple of dataset-specific notebooks for each ([adding a dataset](docs/datasets/README.md)).

It runs on Databricks with a Bronze, Silver and Gold layout. It's a portfolio project, so the docs try to explain why things are built the way they are, not just what they do.

## Status

| Stage | State |
|---|---|
| Bronze (ingest and index) | ✅ Run and verified on Databricks for both datasets |
| Silver (validate and normalize) | ✅ Run and verified on Databricks for both datasets |
| Gold (manifest and training shards) | ✅ `sample-v1` manifest and shards built on Databricks. Shards are a few large files per split, each packing many images with their labels, which training streams from |
| Baseline training | 🟨 Written and smoke-tested locally on tiny fixture shards; never run on Databricks or on the real shards |

Gold releases and training runs both record the Git commit of the code that produced them, and refuse to run without one. On Databricks the commit is read from the workspace Git folder through the Databricks API; that lookup hasn't been verified on a real workspace yet.

## Architecture

Source archives in a landing volume are the *sole* permanent store of image bytes — nothing is ever extracted to a Volume as an individual file, and no layer stores bytes in a table. Every layer that needs actual pixels streams them from the archive on demand instead:

![Bronze/Silver/Gold data pipeline](docs/assets/architecture.svg)

Training pins exactly which data *and* preprocessing a run used, then records it — not something you have to trust the training code got right:

![Training and reproducibility flow](docs/assets/architecture-training.svg)

New to the project? [How it works](docs/how-it-works.md) is a short walkthrough of the layers, the data flow and the code, and [Running the pipeline](docs/running.md) covers setup and each notebook in order. Where every file and table lives is in [architecture.md](docs/architecture.md), and every table's columns are in the [data contract](docs/data_contract.md).

## Key design decisions

A few of the more interesting calls, out of the [full decision log](docs/decisions/):

- [Stream from source archives, never store image bytes anywhere else](docs/decisions/004-stream-archives-no-blob-storage.md) — the central storage decision, took ingestion cost down for the two used datasets.
- [Verify source-archive immutability by checksum, not assumption](docs/decisions/005-immutable-source-archives-checksum-verified.md) — every layer that streams bytes re-verifies them against what Bronze originally recorded.
- [Pin exactly which data + preprocessing a training run used](docs/decisions/007-pin-data-and-preprocessing-per-training-run.md) — so a trained model's reported metrics are never just a matter of trusting training-code discipline.
- [Retention for derived artifacts is an open question, not a default](docs/decisions/006-gold-shard-retention-undecided.md) — left undecided on purpose until there's real usage to base it on.

## Lessons learned

**On cloud object storage, the number of operations costs more than the number of bytes in the short term.** The first Bronze ingestion extracted every image in the archives into a Unity Catalog Volume as its own file, one cloud write per JPEG. For ISIC 2019's roughly 33,000 images that meant about 33,000 separate writes: a single run took 90 minutes and cost about \$17, which is too much to rerun freely on a personal budget. Staging each archive once on local disk and reading images from there brought the run down to 5 minutes and an estimated $6.

## Getting started

The full setup, locally and on Databricks, is in [docs/running.md](docs/running.md).

## Source data

Uses the [ISIC Archive](https://www.isic-archive.com/) and the [ISIC Archive API](https://api.isic-archive.com/api/docs/swagger/). Follow the applicable dataset terms, licences, and citation requirements before downloading or redistributing any data.

## Development approach

Built with heavy use of AI tools for code and documentation, generated in small iterative chunks rather than one large unsupervised pass. I make the architectural decisions myself and take final responsibility for the result. Here is what I have and haven't checked personally:

- **Reviewed and edited by me:** all code in `src/`, the configs, the notebooks, the docs and the ADRs.
- **Verified by running it:** Bronze and Silver, rerun on Databricks against real ISIC data for both onboarded datasets, and the Gold notebooks, run to produce training shards.
- **Not reviewed line by line:** the test suite in `tests/`. I've checked what it covers and that it passes, but I haven't read every assertion.
- **Not verified end to end:** baseline classifier training (`src/ml/`). The code is complete and passes a local smoke test on tiny fixture shards, but I never ran it on Databricks because my credits ran out first. Treat anything the docs say about real training runs as untested.
