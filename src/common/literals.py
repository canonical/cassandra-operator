#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from typing import Literal

PEER_RELATION = "cassandra-peers"

SUBSTRATES = Literal["vm", "k8s"]

PEER_PORT = 7000
CLIENT_PORT = 9042
CLIENT_MGMT_URL = "http://127.0.0.1:8080/api/v0"
