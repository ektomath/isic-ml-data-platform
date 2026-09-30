# 012. Orchestrate with one job per lifecycle step, defined in an Asset Bundle

Status: accepted. Not yet deployed to a workspace.

## Summary

The notebooks run as Databricks jobs defined in the repo (`databricks.yml` and `resources/jobs.yml`). There is one job per thing a person actually does, each started by hand when needed: set up the workspace, ingest a dataset, build a training release, train a model. Settings that change from run to run, such as which release to build, are job parameters instead of values edited inside notebooks. The cost is a few more jobs to know about, and a step that has to pass the git commit explicitly.

## Context

The steps don't happen together. The workspace is set up once. A dataset is ingested when it's onboarded, which can be months apart for different datasets. Training releases are built whenever someone wants a new combination of data, and models are trained whenever someone wants to experiment. Before this, every run meant opening notebooks in the right order and editing names like `MANIFEST_NAME` by hand.

## Decision

- **One job per lifecycle step:** `setup` (notebook 00), `ingest_<dataset>` (05, 10, 20 for that dataset), `build_release` (30, 31) and `train` (40).
- **One ingest job per dataset.** Each dataset has its own notebooks, and a job task's notebook path can't come from a parameter. Onboarding a dataset adds its config, its notebooks and one job entry.
- **Parameters, not notebook edits.** `build_release` takes `release_name` (naming both its manifest and export configs) and `overwrite_existing_release`; `train` takes `training_run_name`. The notebooks read these as widgets whose defaults match a manual run, so opening a notebook by hand still works.
- **No schedules.** The sources don't change on their own, so every job is started by hand.
- **The bundle passes the git commit.** A deployed bundle is a copy of the files, not a Git folder, so the commit lookup from [ADR 010](010-record-git-commit-on-releases-and-runs.md) would find nothing. `build_release` and `train` get `${bundle.git.commit}` as a parameter, which takes priority over the lookup.

## Alternatives considered

- **One job that runs everything in order.** It's simple, but it re-ingests every dataset each time you want a new training set, which costs credits and doesn't match how the steps are actually used.
- **Jobs created by hand in the Databricks UI.** They'd work, but they wouldn't be in version control or reviewable.
- **Multiple deployment targets and CI deployment.** More than one person and one workspace need.

## Consequences

- `databricks bundle deploy` creates or updates all jobs from the repo; `databricks bundle run <job>` runs one. Running notebooks by hand keeps working.
- The commit a job records is the one deployed, even if the working tree had uncommitted changes at deploy time. Deploy from a clean checkout.
- An ingest job could later start automatically when an archive lands in its landing folder (a file-arrival trigger). Not needed while datasets are added by hand.
