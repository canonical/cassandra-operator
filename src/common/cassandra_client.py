#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
from typing import List, Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

logger = logging.getLogger(__name__)


class CassandraClient:
    """TODO."""

    def __init__(
        self, hosts: List[str], user: Optional[str] = None, password: Optional[str] = None
    ):
        self.hosts = hosts
        self.user = user
        self.password = password
        self.auth_provider = None

        if self.user is not None and self.password is not None:
            self.auth_provider = PlainTextAuthProvider(username=self.user, password=self.password)

        return

    def create_keyspace(self, keyspace: str) -> None:
        """TODO."""
        with _SessionContext(self, keyspace=None) as session:
            query = (
                """
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
                """
                % keyspace
            )
            session.execute(query)


class _SessionContext:
    def __init__(self, client: CassandraClient, keyspace: Optional[str] = None):
        self.client = client
        self.keyspace = keyspace
        self.cluster = None
        self.session = None

    def __enter__(self) -> Session:
        self.cluster = Cluster(
            contact_points=self.client.hosts, auth_provider=self.client.auth_provider
        )
        self.session = self.cluster.connect()
        if self.keyspace:
            self.session.set_keyspace(self.keyspace)
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cluster:
            self.cluster.shutdown()
