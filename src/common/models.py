#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from dataclasses import dataclass


@dataclass
class Node:
    """Class representing the nodes of an Cassandra cluster."""

    id: str
    name: str
    peer_urls: list[str]
    client_urls: list[str]
