import subprocess
from unittest.mock import MagicMock, patch

import pytest

from common.exceptions import ExecError
from workload import SNAP_NAME, SNAP_SERVICE, CassandraWorkload


@pytest.fixture
def mock_snap_cache():
    with patch("workload.snap.SnapCache") as snap_cache:
        mock_snap = MagicMock()
        mock_snap.services = {SNAP_SERVICE: {"active": True}}
        snap_cache.return_value = {SNAP_NAME: mock_snap}
        yield mock_snap


def test_alive_true_if_service_active(mock_snap_cache):
    workload = CassandraWorkload()
    assert workload.is_alive() is True


def test_alive_false_if_service_missing(mock_snap_cache):
    mock_snap_cache.services = {}  # simulate service not found
    workload = CassandraWorkload()
    assert workload.is_alive() is False


def test_exec_successful_command_returns_output():
    with patch("workload.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["echo", "hello"], returncode=0, stdout="hello\n", stderr=""
        )
        workload = CassandraWorkload()
        stdout, stderr = workload.exec(["echo", "hello"])
        assert stdout == "hello"
        assert stderr == ""


def test_exec_command_raises_on_failure():
    with patch("workload.subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd=["false"], output="", stderr="error"
        )
        workload = CassandraWorkload()
        with pytest.raises(ExecError) as e:
            workload.exec(["false"])
        assert "error" in e.value.stderr
