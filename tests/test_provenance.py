import subprocess
from types import SimpleNamespace

import pytest

from data_platform.provenance import (
    GitCommitUnavailableError,
    commit_from_databricks_git_folder,
    resolve_git_commit,
)


def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def git_repo(tmp_path):
    # A real, throwaway git repository with one commit -- no mocking of git itself.
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    (repo / "file.txt").write_text("one\n")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-q", "-m", "first")
    return repo


def test_resolve_git_commit_returns_head_of_a_clean_checkout(git_repo, monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    assert resolve_git_commit(git_repo) == _git(git_repo, "rev-parse", "HEAD")


def test_resolve_git_commit_marks_uncommitted_changes_as_dirty(git_repo, monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    (git_repo / "file.txt").write_text("changed\n")

    assert resolve_git_commit(git_repo) == _git(git_repo, "rev-parse", "HEAD") + "-dirty"


def test_resolve_git_commit_raises_outside_a_git_checkout(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    with pytest.raises(GitCommitUnavailableError, match="git rev-parse"):
        resolve_git_commit(tmp_path)


class _FakeRepos:
    """Stands in for WorkspaceClient().repos -- the Databricks API can't be reached in tests."""

    def __init__(self, repos):
        self._repos = repos

    def list(self, path_prefix=None):
        return [repo for repo in self._repos if repo.path.startswith(path_prefix)]


def test_databricks_lookup_matches_the_git_folder_path():
    client = SimpleNamespace(
        repos=_FakeRepos(
            [
                SimpleNamespace(path="/Users/me/isic-ml-data-platform-old", head_commit_id="old"),
                SimpleNamespace(path="/Users/me/isic-ml-data-platform", head_commit_id="abc123"),
            ]
        )
    )

    commit, error = commit_from_databricks_git_folder(
        "/Workspace/Users/me/isic-ml-data-platform", workspace_client=client
    )

    assert (commit, error) == ("abc123", None)


def test_databricks_lookup_reports_a_missing_git_folder():
    client = SimpleNamespace(repos=_FakeRepos([]))

    commit, error = commit_from_databricks_git_folder("/Workspace/Users/me/elsewhere", workspace_client=client)

    assert commit is None
    assert "no Git folder found" in error


def test_resolve_git_commit_uses_an_explicit_commit_first(tmp_path, monkeypatch):
    # A bundle-deployed job passes the commit it deployed; no checkout is needed then.
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    assert resolve_git_commit(tmp_path, explicit_commit="abc123") == "abc123"
