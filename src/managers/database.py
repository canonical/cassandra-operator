#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database manager."""

import logging
from contextlib import contextmanager
from typing import Generator

from cassandra import AuthenticationFailed
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    NoHostAvailable,
    Session,
)
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

from core.literals import CASSANDRA_ADMIN_USERNAME

logger = logging.getLogger(__name__)

_CASSANDRA_DEFAULT_CREDENTIALS = "cassandra"


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

    def check(self) -> bool:
        try:
            with self._session() as session:
                session.execute("SELECT release_version FROM system.local")
                return True
        except NoHostAvailable as e:
            if e.errors:
                for host_error in e.errors.values():
                    if isinstance(host_error, AuthenticationFailed):
                        return True
            return False
        except Exception:
            return False

    def init_admin(self, password: str) -> None:
        """Create operator role with the specified password and remove default cassandra role.

        Grant operator role SUPERUSER and LOGIN. Use local connection.
        """
        with self._session(
            hosts=["127.0.0.1"],
            auth_provider=PlainTextAuthProvider(
                username=_CASSANDRA_DEFAULT_CREDENTIALS, password=_CASSANDRA_DEFAULT_CREDENTIALS
            ),
        ) as session:
            session.execute(
                "CREATE ROLE %s WITH LOGIN = true and SUPERUSER = true and PASSWORD = %s",
                [CASSANDRA_ADMIN_USERNAME, password],
            )
        with self._session(
            hosts=["127.0.0.1"],
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=password
            ),
        ) as session:
            session.execute("DROP ROLE %s", [_CASSANDRA_DEFAULT_CREDENTIALS])

    def update_role_password(self, user: str, password: str) -> None:
        """Change password of the specified role."""
        with self._session() as session:
            # TODO: increase replication factor of system_auth.
            session.execute(
                "ALTER ROLE %s WITH PASSWORD = %s",
                [user, password],
            )

    def update_auth_replication(self, replication_factor: int) -> None:
        with self._session() as session:
            session.execute(
                "ALTER KEYSPACE system_auth"
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %s}",
                [replication_factor],
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
