#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
import socket

from common.management_client import ManagementClient
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class ClusterManager:
    """Manage cluster members, quorum and authorization."""

    def __init__(self, workload: WorkloadBase):
        self.workload = workload
        self.management_client = ManagementClient()

    def start_node(self) -> None:
        """Start a cluster node."""
        self.workload.start()

    def restart_node(self) -> None:
        """Restart a cluster node."""
        self.workload.restart()

    @property
    def is_healthy(self) -> bool:
        """Perform cassandra helth and readiness checks and return True if healthy.

        Returns:
            bool: True if the cluster or node is healthy.
        """
        return self.management_client.is_healthy()

    def network_address(self) -> tuple[str, str]:
        """TODO."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname
