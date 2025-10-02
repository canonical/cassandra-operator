#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database manager."""

import logging
from contextlib import contextmanager
from ssl import CERT_REQUIRED, PROTOCOL_TLS, SSLContext
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
from core.state import TLSScope
from core.workload import WorkloadBase
from managers.tls import TLSManager

logger = logging.getLogger(__name__)

_CASSANDRA_DEFAULT_CREDENTIALS = "cassandra"


class DatabaseManager:
    """Manager of Cassandra database."""

    def __init__(
        self,
        workload: WorkloadBase,
        tls_manager: TLSManager | None,
        hosts: list[str],
        user: str,
        password: str,
    ):
        self.execution_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
        )
        self.auth_provider = (
            PlainTextAuthProvider(username=user, password=password) if user and password else None
        )
        if tls_manager and tls_manager.client_tls_ready:
            self.ssl_context = SSLContext(PROTOCOL_TLS)
            self.ssl_context.load_cert_chain(
                certfile=workload.cassandra_paths.get_certificate(TLSScope.CLIENT).as_posix(),
                keyfile=workload.cassandra_paths.get_private_key(TLSScope.CLIENT).as_posix(),
            )
            self.ssl_context.verify_mode = CERT_REQUIRED
            self.ssl_context.load_verify_locations(
                cafile=workload.cassandra_paths.get_ca(TLSScope.CLIENT).as_posix()
            )
        else:
            self.ssl_context = None
        self.hosts = hosts

        return

    def check(self, hosts: list[str] | None = None) -> bool:
        """Check connectivity to the Cassandra.

        Returns positive even when cluster cannot achieve consistency level for the authentication.

        Returns:
            whether Cassandra service on this node is ready to accept connections.
        """
        try:
            with self._session(hosts=hosts) as session:
                session.execute("SELECT release_version FROM system.local")
                logger.debug(f"Reachability check success: {','.join(self.hosts)}")
                return True
        except NoHostAvailable as e:
            if e.errors:
                for host, host_error in e.errors.items():
                    if isinstance(host_error, AuthenticationFailed):
                        logger.debug(f"Reachability check success: {host} - {host_error}")
                        return True
                    else:
                        logger.debug(f"Reachability check failure: {host} - {host_error}")
            return False
        except Exception as e:
            logger.debug(f"Reachability check failure: {','.join(self.hosts)} - {e}")
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
            ssl_context=self.ssl_context,
        )
        session = cluster.connect()
        if keyspace:
            session.set_keyspace(keyspace)
        try:
            yield session
        finally:
            session.shutdown()
            cluster.shutdown()
