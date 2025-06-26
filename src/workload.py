#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
import subprocess
from shutil import rmtree

from charmlibs import pathops
from charms.operator_libs_linux.v2 import snap
from typing_extensions import override

from common.exceptions import ExecError
from core.workload import CassandraPaths, WorkloadBase

SNAP_CURRENT_PATH = "/snap/charmed-cassandra/current"
SNAP_VAR_PATH = "/var/snap/charmed-cassandra"
SNAP_VAR_COMMON_PATH = f"{SNAP_VAR_PATH}/common"
SNAP_VAR_CURRENT_PATH = f"{SNAP_VAR_PATH}/current"

SNAP_NAME = "charmed-cassandra"
SNAP_REVISION = "8"
SNAP_SERVICE = "mgmt-server"

logger = logging.getLogger(__name__)


class CassandraWorkload(WorkloadBase):
    """Implementation of WorkloadBase for running on VMs."""

    def __init__(self) -> None:
        super().__init__()
        self.substrate = "vm"
        self.root = pathops.LocalPath("/")

        self.cassandra_paths = CassandraPaths()
        self.cassandra_paths.env = self.root / "/etc/environment"
        self.cassandra_paths.config_dir = self.root / f"{SNAP_VAR_CURRENT_PATH}/etc/cassandra"
        self.cassandra_paths.data_dir = self.root / f"{SNAP_VAR_COMMON_PATH}/var/lib/cassandra"

        self._cassandra_snap = snap.SnapCache()[SNAP_NAME]

    @override
    def start(self) -> None:
        try:
            self._cassandra_snap.start(services=[SNAP_SERVICE])
        except snap.SnapError as e:
            logger.exception(f"Failed to start cassandra snap: {e}")
            raise

    @override
    def install(self) -> None:
        """Install the cassandra snap."""
        logger.debug("Installing & configuring Cassandra snap")
        self._cassandra_snap.ensure(snap.SnapState.Present, revision=SNAP_REVISION)
        self._cassandra_snap.connect("process-control")
        self._cassandra_snap.connect("system-observe")
        self._cassandra_snap.connect("mount-observe")
        self._cassandra_snap.hold()

    @override
    def alive(self) -> bool:
        try:
            return bool(self._cassandra_snap.services[SNAP_SERVICE]["active"])
        except KeyError:
            return False

    @override
    def write_file(self, content: str, file: str) -> None:
        path = self.root / file
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(content)

    @override
    def read_file(self, file: str) -> str:
        path = self.root / file
        if not path.exists():
            raise FileNotFoundError(f"File '{file}' does not exist.")
        return path.read_text()

    @override
    def stop(self) -> None:
        self._cassandra_snap.stop(services=[SNAP_SERVICE])

    @override
    def restart(self) -> None:
        self._cassandra_snap.restart(services=[SNAP_SERVICE])

    @override
    def remove_file(self, file) -> None:
        path = self.root / file
        path.unlink(missing_ok=True)

    @override
    def remove_directory(self, directory: str) -> None:
        # TODO: https://github.com/canonical/charmtech-charmlibs/issues/23
        rmtree(directory)

    @override
    def path_exists(self, path: str) -> bool:
        path_object = self.root / path
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
