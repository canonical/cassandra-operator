#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cassandra CQL client."""

import logging
from contextlib import contextmanager
from typing import Generator

from bcrypt import gensalt, hashpw
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

logger = logging.getLogger(__name__)


class CassandraClient:
    """Cassandra CQL client."""

    def __init__(self, hosts: list[str], user: str | None = None, password: str | None = None):
        self.execution_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
        )
        self.auth_provider = (
            PlainTextAuthProvider(username=user, password=password)
            if user is not None and password is not None
            else None
        )
        self.hosts = hosts

        return

    def change_superuser_password(self, user: str, password: str) -> None:
        """TODO."""
        with self._session() as session:
            session.execute(
                "UPDATE system_auth.roles SET salted_hash = %s WHERE role = %s",
                [
                    hashpw(password.encode(), gensalt(prefix=b"2a")).decode(),
                    user,
                ],
            )

    def change_user_password(self, user: str, password: str) -> None:
        """TODO."""
        with self._session() as session:
            session.execute("ALTER USER %s WITH PASSWORD %s", [user, password])

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
            cluster.shutdown()
