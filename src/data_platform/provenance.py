"""Resolve which git commit of this repo is running, for lineage on Gold releases and
training runs.

Pure Python, no Spark dependency. Two sources are tried in order:

1. On Databricks, the workspace Git folder's current commit, asked of the Databricks Repos
   API. Serverless compute has no `git` command, so this is the only way to learn the commit
   there. Not yet verified on a real workspace.
2. `git rev-parse HEAD` in the repo checkout, for local runs and CI. A checkout with
   uncommitted changes gets a `-dirty` suffix, so a recorded commit never claims to be clean
   code when it wasn't.

If neither works, `resolve_git_commit` raises instead of returning nothing: a Gold release or
training run without a commit can't be traced back to the code that produced it, so it
shouldn't be written at all.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from data_platform import running_on_databricks

# The repo root, derived from this file's location (src/data_platform/provenance.py), so the
# commit always describes the code that's actually running.
REPO_ROOT = Path(__file__).resolve().parents[2]


class GitCommitUnavailableError(RuntimeError):
    """Raised when no source can say which commit is running."""


def resolve_git_commit(repo_root: str | Path = REPO_ROOT, explicit_commit: str | None = None) -> str:
    """Return the running code's commit hash, or raise GitCommitUnavailableError.

    `explicit_commit` wins when given: a job deployed with a Databricks Asset Bundle runs a
    copy of the files rather than a Git folder, so the bundle passes the commit it deployed
    (`${bundle.git.commit}`) as a job parameter instead."""
    if explicit_commit:
        return explicit_commit
    repo_root = Path(repo_root)
    attempts = []

    if running_on_databricks():
        commit, error = commit_from_databricks_git_folder(repo_root)
        if commit:
            return commit
        attempts.append(f"Databricks Git folder lookup: {error}")

    commit, error = commit_from_git_cli(repo_root)
    if commit:
        return commit
    attempts.append(f"git rev-parse: {error}")

    raise GitCommitUnavailableError(
        f"Could not determine the git commit for {repo_root}. Run from a git checkout or a "
        f"Databricks Git folder. Tried: {'; '.join(attempts)}"
    )


def commit_from_git_cli(repo_root: Path) -> tuple[str | None, str | None]:
    """(commit, None) from `git rev-parse HEAD`, with `-dirty` appended if the working tree
    has uncommitted changes; (None, reason) if git or the checkout isn't available."""
    try:
        commit = _git(repo_root, "rev-parse", "HEAD")
        dirty = _git(repo_root, "status", "--porcelain", "--untracked-files=no")
    except (OSError, subprocess.SubprocessError) as error:
        return None, str(error) or type(error).__name__
    return (f"{commit}-dirty" if dirty else commit), None


def commit_from_databricks_git_folder(repo_root: Path, workspace_client=None) -> tuple[str | None, str | None]:
    """(commit, None) for the Databricks Git folder containing repo_root; (None, reason)
    otherwise. `workspace_client` is injectable for tests; by default the Databricks SDK
    client authenticates from the notebook's own context."""
    workspace_path = str(repo_root)
    if workspace_path.startswith("/Workspace/"):
        workspace_path = workspace_path[len("/Workspace") :]
    try:
        if workspace_client is None:
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient()
        for repo in workspace_client.repos.list(path_prefix=workspace_path):
            if repo.path == workspace_path and repo.head_commit_id:
                return repo.head_commit_id, None
    except Exception as error:  # SDK import, auth or API failure: report it, don't mask it
        return None, f"{type(error).__name__}: {error}"
    return None, f"no Git folder found at {workspace_path}"


def _git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), *args], capture_output=True, text=True, check=True, timeout=10
    )
    return result.stdout.strip()
