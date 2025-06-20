#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
import subprocess
from pathlib import Path
from shutil import rmtree

from charmlibs import pathops
from charms.operator_libs_linux.v2 import snap
from typing_extensions import override

from common.exceptions import ExecError
from common.literals import SNAP_NAME, SNAP_SERVICE
from core.workload import CassandraPaths, ManagementApiPaths, WorkloadBase

SNAP_VAR_CURRENT_PATH = "/var/snap/charmed-cassandra/current"
SNAP_CURRENT_PATH = "/snap/charmed-cassandra/current"

logger = logging.getLogger(__name__)


class CassandraWorkload(WorkloadBase):
    """Implementation of WorkloadBase for running on VMs."""

    def __init__(self) -> None:
        super().__init__()
        self.cassandra_paths = CassandraPaths(
            config_path=pathops.LocalPath(f"{SNAP_VAR_CURRENT_PATH}/etc/cassandra")
        )
        self.management_api_paths = ManagementApiPaths(
            agent_path=f"{SNAP_CURRENT_PATH}/opt/mgmt-api/libs/datastax-mgmtapi-agent.jar"
        )

    @override
    def start(self) -> None:
        try:
            self._cassandra.start(services=[SNAP_SERVICE])
        except snap.SnapError as e:
            logger.exception(f"Failed to start cassandra snap: {e}")

    @override
    def install(self) -> None:
        """Install the cassandra snap."""
        logger.debug("Installing & configuring Cassandra snap")
        snap.install_local("charmed-cassandra_5.0.4_amd64.snap", devmode=True)
        self._cassandra.connect("process-control")
        self._cassandra.connect("system-observe")

    @override
    def alive(self) -> bool:
        try:
            return bool(self._cassandra.services[SNAP_SERVICE]["active"])
        except KeyError:
            return False

    @override
    def write_file(self, content: str, file: str) -> None:
        path = Path(file)
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(content)

    @override
    def read_file(self, file: str) -> str:
        path = Path(file)
        if not path.exists():
            raise FileNotFoundError(f"File '{file}' does not exist.")
        return path.read_text()

    @override
    def stop(self) -> None:
        self._cassandra.stop(services=[SNAP_SERVICE])

    @override
    def restart(self) -> None:
        self._cassandra.restart(services=[SNAP_SERVICE])

    @override
    def remove_file(self, file) -> None:
        path = Path(file)
        path.unlink(missing_ok=True)

    @override
    def remove_directory(self, directory: str) -> None:
        rmtree(directory)

    @override
    def path_exists(self, path: str) -> bool:
        path_object = Path(path)

        if path_object.exists():
            if path_object.is_dir():
                # consider it false if the directory is empty
                return len(list(path_object.glob("*"))) > 0
            return True

        return False

    @override
    def exec(self, command: list[str]) -> tuple[str, str]:
        try:
            result = subprocess.run(
                command,
                check=True,
                text=True,
                capture_output=True,
                timeout=10,
            )
            stdout = result.stdout.strip()
            stderr = result.stderr.strip()
            logger.debug("Executed command: %s", " ".join(command))
            logger.debug("STDOUT: %s", stdout)
            logger.debug("STDERR: %s", stderr)
            return stdout, stderr
        except subprocess.CalledProcessError as e:
            stdout = e.stdout.strip()
            stderr = e.stderr.strip()
            logger.error(
                "Got non-zero return code %s while executing command: %s",
                e.returncode,
                " ".join(command),
            )
            logger.debug("STDOUT: %s", e.stdout.strip())
            logger.debug("STDERR: %s", e.stderr.strip())
            raise ExecError(e.stdout.strip(), e.stderr.strip())
        except subprocess.TimeoutExpired as e:
            stdout = e.stdout.decode().strip() if e.stdout else ""
            stderr = e.stderr.decode().strip() if e.stderr else ""
            logger.error("Got timeout error while executing command: %s", " ".join(command))
            logger.debug("STDOUT: %s", stdout)
            logger.debug("STDERR: %s", stderr)
            raise ExecError(stdout, stderr)

    @property
    def _cassandra(self) -> snap.Snap:
        return snap.SnapCache()[SNAP_NAME]
