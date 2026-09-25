# ISIC ML Data Platform

I set out to build a central data platform that holds many dermatology image datasets side by side and lets you assemble machine learning training sets from any combination of them. Duplicate images can't leak between training and test data, and every training set is reproducible: for any trained ML model you can recreate exactly which images, labels and preprocessing it was trained on. The implementation was designed and tested around two datasets, ISIC 2019 and MILK10k.

> A portfolio project demonstrating Azure, Databricks, and reproducible computer-vision data engineering.

An Azure and Databricks-based data platform that turns raw dermatology images and metadata into validated Bronze, Silver, and Gold data products, and includes baseline classifier training code built against them (not yet run end to end, see [Development approach](#development-approach)). The goal it's built around:

> A reviewer can trace a trained model back to an immutable Gold manifest, a validated Silver inventory, and the exact Bronze image checksums used to create it.

Adding a dataset takes a config file in `config/bronze/datasets/` and a mostly-configuration Silver notebook, not new pipeline code. The current ingestion code handles archive-based sources, so a dataset delivered some other way (for example, from an API) would need its own ingestion step.

## Architecture

Source archives in a landing volume are the *sole* permanent store of image bytes — nothing is ever extracted to a Volume as an individual file, and no layer stores bytes in a table. Every layer that needs actual pixels streams them from the archive on demand instead:

![Bronze/Silver/Gold data pipeline](docs/assets/architecture.svg)

Training pins exactly which data *and* preprocessing a run used, then records it — not something you have to trust the training code got right:

![Training and reproducibility flow](docs/assets/architecture-training.svg)

See [`docs/architecture.md`](docs/architecture.md) for the full system layout and Databricks execution model, and [`docs/data_contract.md`](docs/data_contract.md) for every table schema.

New to the project? [How it works](docs/how-it-works.md) is a 10-minute walkthrough of the layers, the data flow, the key decisions and how to run it.

## What's here

- **`src/data_platform/`** — the Bronze/Silver/Gold pipeline: archive streaming, image validation, label normalization, leakage-aware sampling, Spark orchestration.
- **`src/ml/`** — baseline classifier training (PyTorch/torchvision), designed to run identically from a Databricks notebook or a local script, with MLflow logging and pinned data+preprocessing provenance. Unit-tested locally, not yet run on Databricks.
- **`config/`** — one versioned YAML file per dataset, Gold manifest, shard export, preprocessing recipe, and training run — never inline values in a notebook.
- **`notebooks/`** — the numbered pipeline stages (`00` setup → `10` Bronze → `20` Silver → `30`/`31` Gold → `40` training), run manually in sequence.
- **`tests/`** — fixture-backed checks (real tiny archives/shards/MLflow stores, no mocks) for everything that doesn't require a live Spark session.
- **`docs/`** — architecture, data contract, an [ADR log](docs/decisions/) recording every non-obvious design decision (and why it changed), and a [project checklist](docs/project-checklist.md) tracking real status.
- **[`AGENT.md`](AGENT.md)** — the canonical technical reference: conventions, contracts, and current working state, for a human or an AI picking this project back up.

## Key design decisions

A few of the more interesting calls, out of the [full decision log](docs/decisions/):

- [Stream from source archives, never store image bytes anywhere else](docs/decisions/006-stream-archives-no-blob-storage.md) — the central storage decision, after an earlier design ([superseded](docs/decisions/002-store-bronze-images-as-delta-blobs.md)) cost $17 in 90 minutes on per-object cloud writes (see [lessons learned](docs/lessons-learned.md)).
- [Verify source-archive immutability by checksum, not assumption](docs/decisions/008-immutable-source-archives-checksum-verified.md) — every layer that streams bytes re-verifies them against what Bronze originally recorded.
- [Pin exactly which data + preprocessing a training run used](docs/decisions/010-pin-data-and-preprocessing-per-training-run.md) — so a trained model's reported metrics are never just a matter of trusting training-code discipline.
- [Retention for derived artifacts is an open question, not a default](docs/decisions/009-gold-shard-retention-undecided.md) — a deliberate retraction of an earlier, premature policy.

## Local setup

```powershell
uv venv --python 3.11 .venv
.venv\Scripts\Activate.ps1
uv sync --extra dev
uv run pytest
```

## Source data

Uses the [ISIC Archive](https://www.isic-archive.com/) and the [ISIC Archive API](https://api.isic-archive.com/api/docs/swagger/). Follow the applicable dataset terms, licences, and citation requirements before downloading or redistributing any data.

## Development approach

Built with heavy use of AI tools for code and documentation, generated in small iterative chunks rather than one large unsupervised pass. I make the architectural decisions myself and take final responsibility for the result. Here is what I have and haven't checked personally:

- **Reviewed and edited by me:** all code in `src/`, the configs, the notebooks, the docs and the ADRs.
- **Verified by running it:** Bronze and Silver, rerun on Databricks against real ISIC data for both onboarded datasets, and the Gold notebooks, run to produce training shards.
- **Not reviewed line by line:** the test suite in `tests/`. I've checked what it covers and that it passes, but I haven't read every assertion.
- **Not verified end to end:** baseline classifier training (`src/ml/`). The code is complete and passes a local smoke test on tiny fixture shards, but I never ran it on Databricks because my credits ran out first. Treat anything the docs say about real training runs as untested.
