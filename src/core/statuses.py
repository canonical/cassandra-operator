#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm statuses."""

from enum import Enum

from ops import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus


class Status(Enum):
    """Collection of possible statuses for the charm."""

    ACTIVE = ActiveStatus()
    INSTALLING = MaintenanceStatus("installing Cassandra")
    STARTING = MaintenanceStatus("waiting for Cassandra to start")
    CANT_START = BlockedStatus("Cassandra can't start")
    WAITING_FOR_CLUSTER = WaitingStatus("waiting for cluster to start")
    INVALID_CONFIG = BlockedStatus("invalid config")
    INVALID_SYSTEM_USERS_SECRET = BlockedStatus("invalid system-users secret")
    WAITING_FOR_INTERNAL_TLS = WaitingStatus("waiting for internal TLS setup")
    WAITING_FOR_TLS = WaitingStatus("waiting for TLS setup")
    ROTATING_PEER_TLS = MaintenanceStatus("waiting for peer tls rotation to complete")
    ROTATING_CLIENT_TLS = MaintenanceStatus("waiting for client tls rotation to complete")
    REPAIRING_AUTH = MaintenanceStatus("repairing system_auth keyspace")
