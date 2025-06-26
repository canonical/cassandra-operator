#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cassandra CQL client."""

import logging
from contextlib import contextmanager
from typing import Generator

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

logger = logging.getLogger(__name__)


class CassandraClient:
    """TODO."""

    def __init__(self, hosts: list[str], user: str | None = None, password: str | None = None):
        self.auth_provider = (
            PlainTextAuthProvider(username=user, password=password)
            if user is not None and password is not None
            else None
        )
        self.hosts = hosts

        return

    @contextmanager
    def _session(self, keyspace: str | None = None) -> Generator[Session]:
        cluster = Cluster(contact_points=self.hosts, auth_provider=self.auth_provider)
        session = cluster.connect()
        if keyspace:
            session.set_keyspace(keyspace)
        try:
            yield session
        finally:
            cluster.shutdown()
