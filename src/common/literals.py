#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from enum import Enum
from typing import Literal

SNAP_VAR_CURRENT_PATH = "/var/snap/charmed-cassandra/current"
SNAP_CURRENT_PATH = "/snap/charmed-cassandra/current"

SNAP_CONF_PATH = f"{SNAP_VAR_CURRENT_PATH}/etc"

CAS_CONF_PATH = f"{SNAP_CONF_PATH}/cassandra"

CAS_CONF_FILE = f"{CAS_CONF_PATH}/cassandra.yaml"
CAS_ENV_CONF_FILE = f"{CAS_CONF_PATH}/cassandra-env.sh"

MGMT_API_DIR = f"{SNAP_CURRENT_PATH}/opt/mgmt-api"

PEER_RELATION = "cassandra-peers"

SUBSTRATES = Literal["vm", "k8s"]

PEER_PORT = 7000
CLIENT_PORT = 9042
CLIENT_MGMT_URL = "http://127.0.0.1:8080/api/v0"

SNAP_NAME = "charmed-cassandra"
SNAP_SERVICE = "mgmt-server"


class ClusterState(Enum):
    """TODO."""

    ACTIVE = "active"


class UnitWorkloadState(Enum):
    """TODO."""

    STARTING = "starting"
    ACTIVE = "active"
