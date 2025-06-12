#!/usr/bin/env python3
"""TODO."""

from dataclasses import dataclass


@dataclass
class Node:
    """Class representing the nodes of an Cassandra cluster."""

    id: str
    name: str
    peer_urls: list[str]
    client_urls: list[str]
