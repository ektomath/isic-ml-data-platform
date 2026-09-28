# 013. Record the Git commit on every Gold release and training run, and refuse to run without one

Status: accepted.

## Summary

A Gold release is decided by its config, its seeds and the code that samples and splits, and a model by its config and the training code. Config and seeds were already recorded, but the code wasn't. Now both Gold releases and training runs record the Git commit of the code that produced them, and fail up front if no commit can be found. The cost is that a run outside a Git checkout or Databricks Git folder can't happen at all, and the Databricks lookup is still unverified.

## Context

Without the commit, rerunning the same manifest config after a change to the sampling code quietly produces a different release, and nothing shows why. Training already logged a commit, but it used `git rev-parse`, which almost certainly returns nothing on Databricks serverless, where the repo is a workspace Git folder with no `git` command. A missing value was logged silently.

## Decision

- **One shared lookup**, `data_platform.provenance.resolve_git_commit`, used by both the Gold manifest notebook and training.
- **On Databricks**, ask the Databricks Repos API for the current commit of the Git folder the code runs from.
- **Everywhere else**, use `git rev-parse HEAD`, with `-dirty` appended when the checkout has uncommitted changes, so a recorded commit never claims to be clean code when it wasn't.
- **No commit, no run.** If neither source works, the lookup raises before any work starts.
- **Where it's stored:** a `git_commit` column on `gold.manifest_rows` (and summarized in `gold.manifest_registry`), and a `git_commit` MLflow param on every training run.
- **Shards don't get their own commit.** They're raw bytes copied from a manifest and checked against its checksums, so the manifest's commit covers them.

## Alternatives considered

- **Record the commit when available, empty otherwise.** It never blocks a run, but it lets untraceable releases through without anyone noticing.
- **Enter the commit by hand as a notebook parameter.** It's easy to get wrong or forget.

## Consequences

- The Databricks lookup hasn't run on a real workspace yet. If it fails there, Gold and training fail with an error naming both attempts, rather than writing untraceable data.
- A Databricks Git folder with uncommitted edits reports its last commit, not a dirty marker, because the API doesn't expose that.
- Rows and runs written before this change have an empty commit. The columns stay nullable for that reason, and the code refuses to write new empty values.
