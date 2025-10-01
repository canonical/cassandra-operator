#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database manager."""

import logging
import re
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

from typing import Tuple

class Permissions:
    """Wrapper class around Cassandra permissions."""

    _valid_permissions = {
        "ALL",
        "ALTER",
        "AUTHORIZE",
        "DESCRIBE",
        "DROP",
        "MODIFY",
        "SELECT",
    }

    def __init__(self, *perms: str) -> None:
        invalid = [p for p in perms if p.upper() not in self._valid_permissions]
        if invalid:
            raise ValueError(f"Invalid permissions: {invalid}")

        self._perms: Tuple[str, ...] = tuple(p.upper() for p in perms)

    def __iter__(self):
        return iter(self._perms)

    def __repr__(self):
        return f"Permissions{self._perms}"

    def __str__(self):
        return ", ".join(self._perms)

    def __contains__(self, item: str) -> bool:
        return item.upper() in self._perms

    def is_all(self) -> bool:
        return "ALL" in self._perms

    def __len__(self) -> int:
        """Return number of permissions."""
        return len(self._perms)

    def __bool__(self) -> bool:
        """Allow direct truthiness check: if permissions:"""
        return bool(self._perms)    

    def get_all_valid_permissions(self) -> list[str]:
        return list(self._valid_permissions)

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
        """Check connectivity to the Cassandra.

        Returns positive even when cluster cannot achieve consistency level for the authentication.

        Returns:
            whether Cassandra service on this node is ready to accept connections.
        """
        try:
            with self._session() as session:
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
            
    def create_keyspace(self, ks: str, rf: int, op_password: str) -> None:
        """Create keyspace safely with replication factor."""
        
        valid_ks = self.validate_identifier(ks)
    
        cql = f"""
        CREATE KEYSPACE {valid_ks}
        WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': %s}}
        """
        
        with self._session(
            hosts=self.hosts,
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME,
                password=op_password
            ),
        ) as session:
            session.execute(cql, [rf])

    def remove_keyspace(self, ks: str, op_password: str) -> None:
        """Remove keyspace."""

        valid_ks = self.validate_identifier(ks)

        with self._session(
            hosts=self.hosts,
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=op_password
            ),
        ) as session:
            session.execute(
                f"DROP KEYSPACE IF EXISTS {valid_ks}",
            )
            

    def init_user(self, rolename: str, password: str, op_password: str) -> None:
        """Create user role with the specified password.

        Grant user role LOGIN.
        """

        valid_role = self.validate_identifier(rolename)
        
        with self._session(
            hosts=self.hosts,
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=op_password
            ),
        ) as session:
            session.execute(
                "CREATE ROLE IF NOT EXISTS %s WITH LOGIN = true and PASSWORD = %s",
                [valid_role, password],
            )

    def remove_user(self, rolename: str, op_password: str) -> None:
        """Remove keyspace."""
        
        with self._session(
            hosts=self.hosts,
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=op_password
            ),
        ) as session:
            session.execute(
                "DROP ROLE IF EXISTS %s",
                [rolename],
            )            

    def set_ks_permissions(self, rolename: str, ks: str, permissions: Permissions, op_password: str) -> None:
        """Grant user role on keyspace."""

        valid_ks = self.validate_identifier(ks)
        valid_role = self.validate_identifier(rolename)

        perms_str = str(permissions) if not permissions.is_all() else "ALL PERMISSIONS"
        query = f"""REWOKE {",".join(permissions.get_all_valid_permissions())} ON KEYSPACE {valid_ks} FROM %s"""

        logger.info(f"executing query: {query}")
        
        with self._session(
            hosts=self.hosts,
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=op_password
            ),
        ) as session:
            session.execute(
                query,
                [valid_role]
            )
            
            if len(permissions) != 0:
                session.execute(
                    f"GRANT {perms_str} ON KEYSPACE {valid_ks} TO %s",
                    [valid_role]
                )
        

    def update_role_password(self, user: str, op_password: str) -> None:
        """Change password of the specified role."""
        with self._session() as session:
            # TODO: increase replication factor of system_auth.
            session.execute(
                "ALTER ROLE %s WITH PASSWORD = %s",
                [user, op_password],
            )

    @staticmethod    
    def validate_identifier(name: str) -> str:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            raise ValueError(f"Invalid CQL identifier: {name}")
        return name
            

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
