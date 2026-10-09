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
    """Five repos laid out like the real workspace, with its .gitignore. In each one, tag
    lesson-01-start is on the first commit and lesson-01-end on the second, where main is."""
    shutil.copy(SCRIPT, tmp_path / SCRIPT.name)
    shutil.copy(SCRIPT.parent / ".gitignore", tmp_path / ".gitignore")
    for repo in [tmp_path, *(tmp_path / s for s in SERVICES)]:
        repo.mkdir(exist_ok=True)
        git(repo, "init", "-q", "-b", "main")
        if repo == tmp_path:
            git(repo, "add", ".gitignore", SCRIPT.name)
        else:
            (repo / ".gitignore").write_text("*.db\n")
            git(repo, "add", ".gitignore")
        commit(repo, "first")
        git(repo, "tag", "lesson-01-start")
        commit(repo, "second")
        git(repo, "tag", "lesson-01-end")
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


def test_an_untracked_file_blocks_the_checkout(workspace: Path) -> None:
    (workspace / "contracts" / "notes.md").write_text("mine")

    result = run(workspace, "01")

    assert result.returncode == 1
    assert branches(workspace) == ["main"] * 5
    assert (workspace / "contracts" / "notes.md").read_text() == "mine"


def test_rerunning_a_lesson_keeps_work_committed_on_its_branch(workspace: Path) -> None:
    orders = workspace / "orders-service"
    assert run(workspace, "01").returncode == 0
    commit(orders, "my-work")
    my_work = git(orders, "rev-parse", "HEAD")

    refused = run(workspace, "01")

    assert refused.returncode == 1
    assert (
        "orders-service: branch lesson-01 has commits that lesson-01-start does not have"
        in refused.stderr
    )
    assert git(orders, "rev-parse", "lesson-01") == my_work

    discarded = run(workspace, "01", "--discard")

    assert discarded.returncode == 0, discarded.stderr
    assert my_work[:7] in discarded.stdout
    assert git(orders, "rev-parse", "HEAD") == git(orders, "rev-parse", "lesson-01-start")


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["1"],
        ["01", "--force"],
        ["01", "--discard", "x"],
        ["01", "end"],
        ["01", "--end", "--end"],
        ["01", "--discard", "--discard"],
    ],
)
def test_bad_arguments_print_usage(workspace: Path, args: list[str]) -> None:
    result = run(workspace, *args)

    assert result.returncode == 2
    assert "usage:" in result.stderr


def test_end_puts_every_repo_at_the_end_of_the_lesson(workspace: Path) -> None:
    result = run(workspace, "01", "--end")

    assert result.returncode == 0, result.stderr
    assert branches(workspace) == ["lesson-01"] * 5
    for repo in [workspace, *(workspace / s for s in SERVICES)]:
        assert git(repo, "rev-parse", "HEAD") == git(repo, "rev-parse", "lesson-01-end")


def test_jumping_to_the_end_from_an_untouched_start_needs_no_discard(workspace: Path) -> None:
    assert run(workspace, "01").returncode == 0

    result = run(workspace, "01", "--end")

    assert result.returncode == 0, result.stderr
    assert "To get it back" not in result.stdout


def test_work_committed_during_the_lesson_blocks_the_jump_to_the_end(workspace: Path) -> None:
    orders = workspace / "orders-service"
    assert run(workspace, "01").returncode == 0
    commit(orders, "live-demo")
    live = git(orders, "rev-parse", "HEAD")

    refused = run(workspace, "01", "--end")

    assert refused.returncode == 1
    assert (
        "orders-service: branch lesson-01 has commits that lesson-01-end does not have"
        in refused.stderr
    )
    assert git(orders, "rev-parse", "HEAD") == live

    jumped = run(workspace, "01", "--end", "--discard")

    assert jumped.returncode == 0, jumped.stderr
    assert live[:7] in jumped.stdout
    assert git(orders, "rev-parse", "HEAD") == git(orders, "rev-parse", "lesson-01-end")


def test_the_demo_databases_are_removed(workspace: Path) -> None:
    database = workspace / "orders-service" / "orders.db"
    database.write_text("migrated by a later lesson")

    result = run(workspace, "01")

    assert result.returncode == 0, result.stderr
    assert not database.exists()
    assert "orders-service/orders.db" in result.stdout


def test_a_worktree_next_to_a_service_does_not_block_the_checkout(workspace: Path) -> None:
    worktree = workspace / "orders-service-cancel"
    git(workspace / "orders-service", "worktree", "add", "-q", str(worktree), "-b", "cancel")

    result = run(workspace, "01")

    assert result.returncode == 0, result.stderr
    assert branches(workspace) == ["lesson-01"] * 5
