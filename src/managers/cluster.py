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

    @property
    def is_healthy(self) -> bool:
        """Whether Cassandra healthy and ready in this unit."""
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return "Native Transport active: true" in stdout
        except ExecError:
            return False

    def network_address(self) -> tuple[str, str]:
        """Get hostname and IP of this unit."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname
