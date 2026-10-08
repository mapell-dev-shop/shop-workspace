"""Runs checkout-lesson.sh against throwaway git repos laid out like the real workspace."""

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / "checkout-lesson.sh"
SERVICES = ["contracts", "orders-service", "inventory-service", "payments-service"]


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def commit(repo: Path, name: str) -> None:
    (repo / name).write_text(name)
    git(repo, "add", name)
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", name)


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """Five repos, each with tag lesson-01-start one commit behind its main branch."""
    shutil.copy(SCRIPT, tmp_path / SCRIPT.name)
    (tmp_path / ".gitignore").write_text("".join(f"/{s}/\n" for s in SERVICES))
    for repo in [tmp_path, *(tmp_path / s for s in SERVICES)]:
        repo.mkdir(exist_ok=True)
        git(repo, "init", "-q", "-b", "main")
        if repo == tmp_path:
            git(repo, "add", ".gitignore", SCRIPT.name)
        commit(repo, "first")
        git(repo, "tag", "lesson-01-start")
        commit(repo, "second")
    return tmp_path


def run(workspace: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(workspace / SCRIPT.name), *args], capture_output=True, text=True, check=False
    )


def branches(workspace: Path) -> list[str]:
    return [git(r, "branch", "--show-current") for r in [workspace, *(workspace / s for s in SERVICES)]]


def test_every_repo_moves_to_the_lesson_start(workspace: Path) -> None:
    result = run(workspace, "01")

    assert result.returncode == 0, result.stderr
    assert branches(workspace) == ["lesson-01"] * 5
    for repo in [workspace, *(workspace / s for s in SERVICES)]:
        assert git(repo, "rev-parse", "HEAD") == git(repo, "rev-parse", "lesson-01-start")


def test_a_missing_tag_changes_nothing(workspace: Path) -> None:
    git(workspace / "payments-service", "tag", "-d", "lesson-01-start")

    result = run(workspace, "01")

    assert result.returncode == 1
    assert "payments-service: tag lesson-01-start not found" in result.stderr
    assert branches(workspace) == ["main"] * 5


def test_uncommitted_work_is_kept_unless_discard_is_given(workspace: Path) -> None:
    (workspace / "orders-service" / "first").write_text("my unsaved change")

    refused = run(workspace, "01")
    assert refused.returncode == 1
    assert branches(workspace) == ["main"] * 5
    assert (workspace / "orders-service" / "first").read_text() == "my unsaved change"

    discarded = run(workspace, "01", "--discard")
    assert discarded.returncode == 0, discarded.stderr
    assert (workspace / "orders-service" / "first").read_text() == "first"


@pytest.mark.parametrize("args", [[], ["1"], ["01", "--force"], ["01", "--discard", "x"]])
def test_bad_arguments_print_usage(workspace: Path, args: list[str]) -> None:
    result = run(workspace, *args)

    assert result.returncode == 2
    assert "usage:" in result.stderr
