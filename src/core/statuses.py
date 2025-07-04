#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from enum import Enum

from ops import ActiveStatus, BlockedStatus, MaintenanceStatus


class Status(Enum):
    """Collection of possible statuses for the charm."""

    ACTIVE = ActiveStatus()
    INSTALLING = MaintenanceStatus("installing Cassandra")
    STARTING = MaintenanceStatus("waiting for Cassandra to start")
    INVALID_CONFIG = BlockedStatus("invalid config")
