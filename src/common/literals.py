#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, StatusBase

SNAP_VAR_CURRENT_PATH = "/var/snap/charmed-cassandra/current"
SNAP_CURRENT_PATH = "/snap/charmed-cassandra/current"

SNAP_CONF_PATH = f"{SNAP_VAR_CURRENT_PATH}/etc"

CAS_CONF_PATH = f"{SNAP_CONF_PATH}/cassandra"

CAS_CONF_FILE = f"{CAS_CONF_PATH}/cassandra.yaml"
CAS_ENV_CONF_FILE = f"{CAS_CONF_PATH}/cassandra-env.sh"

MGMT_API_DIR = f"{SNAP_CURRENT_PATH}/opt/mgmt-api"

PEER_RELATION = "cassandra-peers"

SUBSTRATES = Literal["vm", "k8s"]
SUBSTRATE = "vm"

PEER_PORT = 7000
CLIENT_PORT = 9042
CLIENT_MGMT_PORT = 8080


DebugLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
SNAP_NAME = "charmed-cassandra"
SNAP_SERVICE = "mgmt-server"


@dataclass
class StatusLevel:
    """Status object helper."""

    status: StatusBase
    log_level: DebugLevel


class CassandraClusterState(Enum):
    """Enum for Cluster state in cassandra."""

    EXISTING = "existing"
    NEW = "new"


class Status(Enum):
    """Collection of possible statuses for the charm."""

    ACTIVE = StatusLevel(ActiveStatus(), "DEBUG")
    REMOVED = StatusLevel(BlockedStatus("unit removed from cluster"), "INFO")
    CLUSTER_NOT_JOINED = StatusLevel(MaintenanceStatus("Waiting to join cluster"), "DEBUG")
    CLUSTER_INITIALIZING = StatusLevel(
        MaintenanceStatus("Initializing cassandra cluster..."), "DEBUG"
    )
    CLUSTER_NOT_INITIALIZED = StatusLevel(
        BlockedStatus("Waiting for cluster initialization"), "ERROR"
    )
    SERVICE_INSTALLING = StatusLevel(MaintenanceStatus("Installing cassandra..."), "DEBUG")
    SERVICE_STARTING = StatusLevel(MaintenanceStatus("Waiting for cassandra to start..."), "DEBUG")
    SERVICE_NOT_INSTALLED = StatusLevel(BlockedStatus("unable to install cassandra snap"), "ERROR")
    SERVICE_NOT_RUNNING = StatusLevel(BlockedStatus("cassandra service not running"), "ERROR")
