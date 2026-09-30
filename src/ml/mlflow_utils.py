"""MLflow setup shared by local and Databricks training runs. Uses mlflow-skinny, a client only,
since tracking goes to the Databricks-hosted server. MLflow records what happened in a run; which
data and preprocessing it used is fixed by config/gold/training_runs/<name>.yaml (ADR 007).
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
    """Point MLflow at the tracking server and set the experiment. Returns the experiment name.

    On Databricks the runtime already sets the tracking URI. Elsewhere it's set to "databricks",
    with credentials found by databricks-sdk in ~/.databrickscfg or DATABRICKS_HOST and
    DATABRICKS_TOKEN; this project never reads them itself. `tracking_uri`, when given, overrides
    both (used by tests). The experiment defaults to DEFAULT_MLFLOW_EXPERIMENT and is created if
    missing.
    """
    if tracking_uri is not None:
        mlflow.set_tracking_uri(tracking_uri)
    elif not running_on_databricks():
        mlflow.set_tracking_uri("databricks")

    resolved_experiment_name = experiment_name or DEFAULT_MLFLOW_EXPERIMENT
    mlflow.set_experiment(resolved_experiment_name)
    return resolved_experiment_name


def configure_model_registry(registry_uri: str | None = None) -> None:
    """Point MLflow's model registry at Unity Catalog, which Databricks doesn't do by default, even
    inside a notebook. Only called when a run registers a model. `registry_uri`, when given,
    overrides it (used by tests).
    """
    mlflow.set_registry_uri(registry_uri if registry_uri is not None else "databricks-uc")
