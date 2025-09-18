#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cluster manager."""

import logging
import socket

from common.exceptions import ExecError
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)

_NODETOOL = "charmed-cassandra.nodetool"


class ClusterManager:
    """Manager of Cassandra cluster, including this unit."""

    def __init__(self, workload: WorkloadBase):
        self._workload = workload

    def is_healthy(self, ip: str) -> bool:
        """Whether Cassandra healthy and ready in this unit."""
        if not ip:
            return False
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "status"], suppress_error_log=True)
            return f"UN  {ip}" in stdout
        except ExecError:
            return False

    @property
    def is_bootstrap_pending(self) -> bool:
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return "Bootstrap state        : NEEDS_BOOTSTRAP" in stdout
        except ExecError:
            return False

    @property
    def is_bootstrap_failed(self) -> bool:
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return (
                "Bootstrap state        : IN_PROGRESS" not in stdout
                and "Bootstrap state        : COMPLETED" not in stdout
            )
        except ExecError:
            return False

    def network_address(self) -> tuple[str, str]:
        """Get hostname and IP of this unit."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname

    def resume_bootstrap(self) -> bool:
        try:
            self._workload.exec([_NODETOOL, "bootstrap", "resume"])
            return True
        except ExecError:
            return False

    def prepare_shutdown(self) -> None:
        """Prepare Cassandra unit for safe shutdown."""
        self._workload.exec([_NODETOOL, "drain"])

    def decommission(self) -> None:
        """Disconnect node from the cluster."""
        self._workload.exec([_NODETOOL, "decommission", "-f"])
