#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
import subprocess
from pathlib import Path
from shutil import rmtree
from typing import List

from charms.operator_libs_linux.v2 import snap
from tenacity import Retrying, retry, stop_after_attempt, wait_fixed
from typing_extensions import override

from common.literals import SNAP_NAME, SNAP_SERVICE
from common.workload import WorkloadBase

logger = logging.getLogger(__name__)


class CassandraWorkload(WorkloadBase):
    """Implementation of WorkloadBase for running on VMs."""

    def __init__(self):
        for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(5)):
            with attempt:
                self.cassandra = snap.SnapCache()[SNAP_NAME]

    @override
    def start(self) -> None:
        try:
            self.cassandra.start(services=[SNAP_SERVICE])
        except snap.SnapError as e:
            logger.exception(f"Failed to start cassandra snap: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def install(self) -> bool:
        """Install the cassandra snap.

        Returns:
            True if successfully installed, False if any error occurs.
        """
        try:
            logger.debug("Installing & configuring Cassandra snap")
            snap.install_local("charmed-cassandra_5.0.4_amd64.snap", devmode=True)
            self.cassandra.connect("process-control")
            self.cassandra.connect("system-observe")

            return True
        except snap.SnapError as e:
            logger.error(f"Failed to install cassandra snap: {e}")
            return False

    @override
    def alive(self) -> bool:
        try:
            return bool(self.cassandra.services[SNAP_SERVICE]["active"])
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
        self.cassandra.stop(services=[SNAP_SERVICE])

    @override
    def restart(self) -> None:
        self.cassandra.restart(services=[SNAP_SERVICE])

    @override
    def remove_file(self, file) -> None:
        path = Path(file)
        path.unlink(missing_ok=True)

    @override
    def remove_directory(self, directory: str) -> None:
        rmtree(directory)

    @override
    def exists(self, path: str) -> bool:
        path_object = Path(path)

        if path_object.exists():
            if path_object.is_dir():
                # consider it false if the directory is empty
                return len(list(path_object.glob("*"))) > 0
            return True

        return False

    @override
    def exec(self, command: List[str]) -> None:
        try:
            output = subprocess.run(
                command,
                check=True,
                text=True,
                capture_output=True,
                timeout=10,
            ).stdout.strip()
            logger.debug(output)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.error(e)
            raise
