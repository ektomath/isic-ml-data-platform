"""MLflow tracking setup shared by local-machine and Databricks-cluster training runs.

Depends only on `mlflow-skinny` (a REST client, not a tracking server) -- see
docs/decisions/010-pin-data-and-preprocessing-per-training-run.md and
docs/decisions/011-baseline-training-framework-and-registry-sync.md for why. MLflow's own scope
here is "what happened during training" (hyperparameters, metrics, artifacts) -- it is never the
source of truth for data/preprocessing identity, which config/gold/training_runs/<name>.yaml and
gold.training_run_registry own instead.
"""

from __future__ import annotations

import os
import subprocess

import mlflow

DEFAULT_MLFLOW_EXPERIMENT = "/Shared/isic_ml_data_platform/baseline_training"


def running_on_databricks() -> bool:
    """True inside a Databricks notebook/job, where an MLflow tracking URI/session is already
    configured by the runtime -- False on a local machine, where this project's code must
    configure one itself."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def configure_mlflow_tracking(experiment_name: str | None, tracking_uri: str | None = None) -> str:
    """Point MLflow at the right tracking store and experiment, identically in shape whether
    called from a Databricks notebook or a local machine.

    `tracking_uri`, if given, is used verbatim and takes priority over everything else -- this
    is for tests (a local `file://` store) and advanced overrides, not normal use.
    Otherwise: on Databricks, nothing needs to be set (the runtime already configures the
    tracking URI); off Databricks, `mlflow.set_tracking_uri("databricks")` points at the same
    Databricks-hosted tracking server over its REST API -- credentials are resolved by
    `databricks-sdk` (an `mlflow-skinny` dependency) from `~/.databrickscfg` or the
    `DATABRICKS_HOST`/`DATABRICKS_TOKEN` environment variables; this project's own code never
    reads or handles those credentials directly.

    Calls `mlflow.set_experiment(...)` with `experiment_name or DEFAULT_MLFLOW_EXPERIMENT`
    (auto-created if it doesn't exist and the caller has permission) and returns the resolved
    name.
    """
    if tracking_uri is not None:
        mlflow.set_tracking_uri(tracking_uri)
    elif not running_on_databricks():
        mlflow.set_tracking_uri("databricks")

    resolved_experiment_name = experiment_name or DEFAULT_MLFLOW_EXPERIMENT
    mlflow.set_experiment(resolved_experiment_name)
    return resolved_experiment_name


def current_git_commit() -> str | None:
    """Best-effort current commit hash, logged as an MLflow param for lineage. Returns None
    (never raises) if git isn't available or this isn't a git checkout -- a training run on a
    Databricks cluster with a plain file copy of the repo, rather than a real git checkout,
    shouldn't fail just because this one param can't be resolved."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=5
        )
        return result.stdout.strip()
    except Exception:
        return None
