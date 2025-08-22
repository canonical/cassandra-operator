#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database manager."""

import logging
from contextlib import contextmanager
from typing import Generator

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manager of Cassandra database."""

    def __init__(self, hosts: list[str], user: str, password: str):
        self.execution_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
        )
        self.auth_provider = (
            PlainTextAuthProvider(username=user, password=password) if user and password else None
        )
        self.hosts = hosts

        return

    def init_operator(self, password: str) -> None:
        """Create operator role with the specified password and remove default cassandra role.

        Grant operator role SUPERUSER and LOGIN. Use local connection.
        """
        with self._session(
            hosts=["127.0.0.1"],
            auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"),
        ) as session:
            session.execute(
                "CREATE ROLE operator WITH LOGIN = true and SUPERUSER = true and PASSWORD = %s",
                [password],
            )
        with self._session(
            hosts=["127.0.0.1"],
            auth_provider=PlainTextAuthProvider(username="operator", password=password),
        ) as session:
            session.execute("DROP ROLE cassandra")

    def update_role_password(self, user: str, password: str) -> None:
        """Change password of the specified role."""
        with self._session() as session:
            # TODO: increase replication factor of system_auth.
            session.execute(
                "ALTER ROLE %s WITH PASSWORD = %s",
                [user, password],
            )

    @contextmanager
    def _session(
        self,
        hosts: list[str] | None = None,
        auth_provider: PlainTextAuthProvider | None = None,
        keyspace: str | None = None,
    ) -> Generator[Session, None, None]:
        cluster = Cluster(
            auth_provider=auth_provider or self.auth_provider,
            contact_points=hosts or self.hosts,
            protocol_version=5,
            execution_profiles={EXEC_PROFILE_DEFAULT: self.execution_profile},
        )
        session = cluster.connect()
        if keyspace:
            session.set_keyspace(keyspace)
        try:
            yield session
        finally:
            session.shutdown()
            cluster.shutdown()
