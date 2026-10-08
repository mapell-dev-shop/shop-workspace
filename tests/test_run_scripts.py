"""run-all.sh and stop-all.sh against stand-in processes, in a throwaway directory."""

import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    for script in ("run-all.sh", "stop-all.sh"):
        shutil.copy(ROOT / script, tmp_path / script)
    (tmp_path / ".run").mkdir()
    (tmp_path / ".venv" / "bin").mkdir(parents=True)
    (tmp_path / ".venv" / "bin" / "python").symlink_to(sys.executable)
    return tmp_path


SLOW_EXIT = """
import signal, sys, time

def stop(signum, frame):
    time.sleep(1)  # a worker finishing its current event
    sys.exit(0)

signal.signal(signal.SIGTERM, stop)
open("ready", "w").close()
while True:
    time.sleep(0.1)
"""


@pytest.fixture
def ours(workspace: Path) -> Iterator[subprocess.Popen[bytes]]:
    """A process that looks like a worker run-all.sh starts (python -m orders.worker) and takes
    a second to exit after SIGTERM, like a worker finishing an event."""
    (workspace / "orders").mkdir()
    (workspace / "orders" / "__init__.py").write_text("")
    (workspace / "orders" / "worker.py").write_text(SLOW_EXIT)
    process = subprocess.Popen(
        [str(workspace / ".venv" / "bin" / "python"), "-m", "orders.worker"], cwd=workspace
    )
    deadline = time.monotonic() + 5
    while not (workspace / "ready").exists():
        assert time.monotonic() < deadline, "stand-in process did not start"
        time.sleep(0.05)
    yield process
    process.kill()
    process.wait()


@pytest.fixture
def stranger() -> Iterator[subprocess.Popen[bytes]]:
    """An unrelated process, as one that reused an old pid after a reboot."""
    process = subprocess.Popen(["sleep", "30"])
    yield process
    process.kill()
    process.wait()


def run(workspace: Path, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(workspace / script)], cwd=workspace, capture_output=True, text=True, timeout=60
    )


def test_run_all_refuses_while_the_shop_is_running(
    workspace: Path, ours: subprocess.Popen[bytes]
) -> None:
    (workspace / ".run" / "orders-api.pid").write_text(f"{ours.pid}\n")

    result = run(workspace, "run-all.sh")

    assert result.returncode == 1
    assert "already running" in result.stderr
    assert not list((workspace / ".run").glob("*.log"))


def test_run_all_drops_a_pid_file_that_points_at_someone_else(
    workspace: Path, stranger: subprocess.Popen[bytes]
) -> None:
    pidfile = workspace / ".run" / "orders-api.pid"
    pidfile.write_text(f"{stranger.pid}\n")

    result = run(workspace, "run-all.sh")

    assert "already running" not in result.stderr
    assert not pidfile.exists()


def test_run_all_refuses_when_a_port_is_taken(workspace: Path) -> None:
    with socket.socket() as listener:
        try:
            listener.bind(("127.0.0.1", 8002))
            listener.listen()
        except OSError:
            pass  # something already listens there, which is the condition under test

        result = run(workspace, "run-all.sh")

    assert result.returncode == 1
    assert "is already in use" in result.stderr


def test_stop_all_stops_its_own_processes_and_waits_for_them(
    workspace: Path, ours: subprocess.Popen[bytes]
) -> None:
    pidfile = workspace / ".run" / "orders-api.pid"
    pidfile.write_text(f"{ours.pid}\n")

    result = run(workspace, "stop-all.sh")

    assert result.returncode == 0
    assert ours.poll() is not None
    assert not pidfile.exists()


def test_stop_all_leaves_an_unrelated_process_alone(
    workspace: Path, stranger: subprocess.Popen[bytes]
) -> None:
    pidfile = workspace / ".run" / "orders-api.pid"
    pidfile.write_text(f"{stranger.pid}\n")

    result = run(workspace, "stop-all.sh")

    assert result.returncode == 0
    assert stranger.poll() is None
    assert not pidfile.exists()
