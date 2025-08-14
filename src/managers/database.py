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

    def update_system_user_password(self, password: str) -> None:
        """Change password of cassandra system user."""
        with self._session() as session:
            # TODO: increase replication factor of system_auth.
            session.execute(
                "ALTER USER cassandra WITH PASSWORD %s",
                [password],
            )

    @contextmanager
    def _session(self, keyspace: str | None = None) -> Generator[Session, None, None]:
        cluster = Cluster(
            auth_provider=self.auth_provider,
            contact_points=self.hosts,
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
