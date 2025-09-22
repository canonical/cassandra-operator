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


class NodeManager:
    """Manager of Cassandra cluster, including this unit."""

    def __init__(self, workload: WorkloadBase):
        self._workload = workload

    def is_healthy(self, ip: str) -> bool:
        """Whether Cassandra healthy and ready in this unit."""
        if not self._is_in_cluster(ip):
            return False
        if not self._is_gossip_active:
            return False
        return True

    @property
    def is_bootstrap_pending(self) -> bool:
        """Whether `nodetool info` bootstrap state is NEEDS_BOOTSTRAP."""
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return "Bootstrap state        : NEEDS_BOOTSTRAP" in stdout
        except ExecError:
            return False

    @property
    def is_bootstrap_in_unknown_state(self) -> bool:
        """Whether `nodetool info` bootstrap state is not IN_PROGRESS or COMPLETED."""
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
        """Resume Cassandra bootstrap.

        Returns:
            whether operation was successful.
        """
        try:
            self._workload.exec([_NODETOOL, "bootstrap", "resume"])
            return True
        except ExecError:
            return False

    def prepare_shutdown(self) -> None:
        """Prepare Cassandra unit for safe shutdown using nodetool drain command."""
        self._workload.exec([_NODETOOL, "drain"])

    def decommission(self) -> None:
        """Disconnect node from the cluster using nodetool decommission command."""
        self._workload.exec([_NODETOOL, "decommission", "-f"])

    def _is_in_cluster(self, ip: str) -> bool:
        if not ip:
            return False
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "status"], suppress_error_log=True)
            return f"UN  {ip}" in stdout
        except ExecError:
            return False

    @property
    def _is_gossip_active(self) -> bool:
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return "Gossip active          : true" in stdout
        except ExecError:
            return False
