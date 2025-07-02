#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
import socket

from common.exceptions import ExecError
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)

_NODETOOL = "charmed-cassandra.nodetool"


class ClusterManager:
    """Manage cluster members, quorum and authorization."""

    def __init__(self, workload: WorkloadBase):
        self._workload = workload
        pass

    @property
    def is_healthy(self) -> bool:
        """TODO."""
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return "Native Transport active: true" in stdout
        except ExecError:
            return False

    def network_address(self) -> tuple[str, str]:
        """TODO."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname
