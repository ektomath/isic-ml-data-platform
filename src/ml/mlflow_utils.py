"""MLflow tracking setup shared by local-machine and Databricks-cluster training runs.

Depends only on `mlflow-skinny` (a REST client, not a tracking server): this project only
talks to the Databricks-hosted tracking server and never runs its own, so full mlflow's
server stack isn't needed. See docs/decisions/007-pin-data-and-preprocessing-per-training-run.md
for how runs are tracked. MLflow's own scope
here is "what happened during training" (hyperparameters, metrics, artifacts) -- it is never the
source of truth for data/preprocessing identity, which config/gold/training_runs/<name>.yaml owns
instead.
"""

from __future__ import annotations

import os

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


def configure_model_registry(registry_uri: str | None = None) -> None:
    """Point MLflow's model registry at Unity Catalog, identically in shape to
    configure_mlflow_tracking -- but with different default logic, since Databricks does *not*
    default the model registry to Unity Catalog on its own, even from inside a notebook where
    the tracking URI is already configured for you. Only called at all when a training run
    actually has a registered_model_name to register (see ml.train.run_training), so a run that
    never registers a model never touches this global MLflow state.

    `registry_uri`, if given, is used verbatim -- for tests (a local `file://` store, shared
    with the tracking URI so a registered model lands in the same fixture directory) and
    advanced overrides, not normal use.
    """
    mlflow.set_registry_uri(registry_uri if registry_uri is not None else "databricks-uc")
