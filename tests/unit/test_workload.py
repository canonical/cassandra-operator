# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import subprocess
from contextlib import contextmanager
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from common.exceptions import ExecError
from workload import SNAP_NAME, SNAP_SERVICE, CassandraWorkload


@contextmanager
def snap_cache(service: bool = False) -> Generator[MagicMock, None, None]:
    with patch(
        "workload.snap.SnapCache",
        return_value={
            SNAP_NAME: MagicMock(services={SNAP_SERVICE: {"active": True}} if service else {})
        },
    ) as snap_cache:
        yield snap_cache


def test_alive_true_if_service_active():
    with snap_cache(service=True):
        assert CassandraWorkload().is_alive


def test_alive_false_if_service_missing():
    with snap_cache():
        assert not CassandraWorkload().is_alive


def test_exec_successful_command_returns_output():
    with snap_cache(), patch("workload.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["echo", "hello"], returncode=0, stdout="hello\n", stderr=""
        )
        workload = CassandraWorkload()
        stdout, stderr = workload.exec(["echo", "hello"])
        assert stdout == "hello"
        assert stderr == ""


def test_exec_command_raises_on_failure():
    with snap_cache(), patch("workload.subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd=["false"], output="", stderr="error"
        )
        workload = CassandraWorkload()
        with pytest.raises(ExecError) as e:
            workload.exec(["false"])
        assert "error" in e.value.stderr
