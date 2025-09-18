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

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import datetime

logger = logging.getLogger(__name__)

_CASSANDRA_DEFAULT_CREDENTIALS = "cassandra"
_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE = "juju_remove_unit_lock"
_CASSANDRA_DECOMMISSION_LOCK_TABLE = "decommission_lock"
_CASSANDRA_DECOMMISSION_HOLD_LOCK_ENTRY = "hold"
_CASSANDRA_DECOMMISSION_REQUSET_LOCK_ENTRY = "request"


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

    def update_role_password(self, user: str, password: str) -> None:
        """Change password of the specified role."""
        with self._session() as session:
            # TODO: increase replication factor of system_auth.
            session.execute(
                "ALTER ROLE %s WITH PASSWORD = %s",
                [user, password],
            )

    def init_decommission_lock(self, password: str) -> None:
        """Create keyspace and table for decommission mutex lock"""
        with self._session(
            hosts=["127.0.0.1"],                
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=password
            ),
        ) as session:
            session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE}
            WITH replication = {{
            'class': 'NetworkTopologyStrategy',
            'replication_factor': {len(self.hosts)}
            }};
            """)

            session.execute(f"""
            CREATE TABLE IF NOT EXISTS {_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE}.{_CASSANDRA_DECOMMISSION_LOCK_TABLE} (
            id text PRIMARY KEY,
            owner text,
            ts timestamp
            );            
            """)

    def add_decommission_lock_request(
        self,
        name: str,
        password: str,
        timeout: float = 20.0,
    ) -> bool:
        """TODO."""
        entry_id = f"{_CASSANDRA_DECOMMISSION_REQUSET_LOCK_ENTRY}_{name}"
        logger.info(f"request_decommission_lock: attempting to request lock={entry_id} owner={CASSANDRA_ADMIN_USERNAME}")
    
        try:
            with self._session(
                auth_provider=PlainTextAuthProvider(username=CASSANDRA_ADMIN_USERNAME, password=password),
                keyspace=_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE,
            ) as session:
                cql = f"INSERT INTO {_CASSANDRA_DECOMMISSION_LOCK_TABLE} (id, owner, ts) VALUES (%s, %s, toTimestamp(now())) IF NOT EXISTS"
    
                stmt = SimpleStatement(cql, consistency_level=ConsistencyLevel.QUORUM)
                logger.info(f"add_decommission_lock_request: executing CQL: {cql} params=({entry_id}, {CASSANDRA_ADMIN_USERNAME})")
    
                # specify timeout to avoid hanging indefinitely
                result = session.execute(stmt, (entry_id, CASSANDRA_ADMIN_USERNAME), timeout=timeout)
                row = result.one()
                logger.info(f"add_decommission_lock_request: CQL result row={row}")
    
                if row is None:
                    logger.warning("add_decommission_lock_request: unexpected empty result")
                    return False
    
                # 'applied' is boolean returned by IF NOT EXISTS
                applied = getattr(row, "applied", None)
                if applied is None and isinstance(row, dict):
                    applied = row.get("applied", False)
    
                if applied:
                    logger.info(f"Decommission lock requested by {CASSANDRA_ADMIN_USERNAME}")
                    return True
                else:
                    current_owner = getattr(row, "owner", None)
                    logger.info(f"Decommission lock already requested by {current_owner}")
                    return False
    
        except Exception:
            logger.exception("add_decommission_lock_request: failed to request lock due to exception")
            return False

    def pop_decommission_lock_request(
        self,
        name: str,
        password: str,
        timeout: float = 20.0,
    ) -> bool:
        """Remove decommission lock request for the given entry_id."""
        entry_id = f"{_CASSANDRA_DECOMMISSION_REQUSET_LOCK_ENTRY}_{name}"
        logger.info(f"pop_decommission_lock_request: attempting to delete lock={entry_id} owner={CASSANDRA_ADMIN_USERNAME}")

        try:
            with self._session(
                auth_provider=PlainTextAuthProvider(username=CASSANDRA_ADMIN_USERNAME, password=password),
                keyspace=_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE,
            ) as session:
                cql = f"DELETE FROM {_CASSANDRA_DECOMMISSION_LOCK_TABLE} WHERE id=%s IF EXISTS"
                stmt = SimpleStatement(cql, consistency_level=ConsistencyLevel.QUORUM)
                logger.info(f"pop_decommission_lock_request: executing CQL: {cql} params=({entry_id},)")

                result = session.execute(stmt, (entry_id,), timeout=timeout)
                row = result.one()
                logger.info(f"pop_decommission_lock_request: CQL result row={row}")

                if row is None:
                    logger.warning("pop_decommission_lock_request: unexpected empty result")
                    return False

                applied = getattr(row, "applied", None)
                if applied is None and isinstance(row, dict):
                    applied = row.get("applied", False)

                if applied:
                    logger.info(f"Decommission lock request {entry_id} removed successfully")
                    return True
                else:
                    logger.info(f"Decommission lock request {entry_id} did not exist")
                    return False

        except Exception:
            logger.exception("pop_decommission_lock_request: failed to remove lock request due to exception")
            return False

    def has_active_decommission_lock_requests(
        self,
        password: str,
        timeout: float = 20.0,
    ) -> bool:
        """Check if there are any active decommission lock requests."""
        try:
            with self._session(
                auth_provider=PlainTextAuthProvider(username=CASSANDRA_ADMIN_USERNAME, password=password),
                keyspace=_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE,
            ) as session:
                cql = f"SELECT id FROM {_CASSANDRA_DECOMMISSION_LOCK_TABLE} LIMIT 1"
                stmt = SimpleStatement(cql, consistency_level=ConsistencyLevel.QUORUM)
                logger.info(f"has_active_decommission_lock_requests: executing CQL: {cql}")

                result = session.execute(stmt, timeout=timeout)
                row = result.one()

                if row:
                    logger.info("has_active_decommission_lock_requests: found at least one lock request")
                    return True
                else:
                    logger.info("has_active_decommission_lock_requests: no lock requests present")
                    return False

        except Exception as e:
            logger.exception(f"has_active_decommission_lock_requests: failed to query lock table: {e}")
            return False

        
        
    def hold_decommission_lock(
        self,
        password: str,
        timeout: float = 20.0,
    ) -> bool:
        """Try to acquire a distributed lock for decommission.
    
        Returns True if lock was acquired, False otherwise.
            """
        logger.info(f"hold_decommission_lock: attempting to acquire lock={_CASSANDRA_DECOMMISSION_HOLD_LOCK_ENTRY} owner={CASSANDRA_ADMIN_USERNAME}")
    
        try:
            with self._session(
                auth_provider=PlainTextAuthProvider(username=CASSANDRA_ADMIN_USERNAME, password=password),
                keyspace=_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE,
            ) as session:
                cql = f"INSERT INTO {_CASSANDRA_DECOMMISSION_LOCK_TABLE} (id, owner, ts) VALUES (%s, %s, toTimestamp(now())) IF NOT EXISTS"
    
                stmt = SimpleStatement(cql, consistency_level=ConsistencyLevel.QUORUM)
                logger.info(f"hold_decommission_lock: executing CQL: {cql} params=({_CASSANDRA_DECOMMISSION_HOLD_LOCK_ENTRY}, {CASSANDRA_ADMIN_USERNAME})")
    
                # specify timeout to avoid hanging indefinitely
                result = session.execute(stmt, (_CASSANDRA_DECOMMISSION_HOLD_LOCK_ENTRY, CASSANDRA_ADMIN_USERNAME), timeout=timeout)
                row = result.one()
                logger.info(f"hold_decommission_lock: CQL result row={row}")
    
                if row is None:
                    logger.warning("hold_decommission_lock: unexpected empty result")
                    return False
    
                # 'applied' is boolean returned by IF NOT EXISTS
                applied = getattr(row, "applied", None)
                if applied is None and isinstance(row, dict):
                    applied = row.get("applied", False)
    
                if applied:
                    logger.info(f"Decommission lock acquired by {CASSANDRA_ADMIN_USERNAME}")
                    return True
                else:
                    current_owner = getattr(row, "owner", None)
                    logger.info(f"Decommission lock held by {current_owner}")
                    return False
    
        except Exception as e:
            logger.error(f"hold_decommission_lock: failed to acquire lock due to exception: {e}")
            return False

    def release_decommission_lock(self, password: str) -> None:
        """Release previously acquired lock."""
        with self._session(
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_ADMIN_USERNAME, password=password
            ),
            keyspace=_CASSANDRA_DECOMMISSION_LOCK_KEYSPACE,
        ) as session:
            result = session.execute(
                f"SELECT owner FROM {_CASSANDRA_DECOMMISSION_LOCK_TABLE} WHERE id=%s",
                [_CASSANDRA_DECOMMISSION_HOLD_LOCK_ENTRY],
            )
            row = result.one()
            if row is not None:
                session.execute(
                    f"DELETE FROM {_CASSANDRA_DECOMMISSION_LOCK_TABLE} WHERE id=%s",
                    [_CASSANDRA_DECOMMISSION_HOLD_LOCK_ENTRY],
                )
                logger.info(f"Released decommission lock")
            else:
                logger.info(f"No decommission lock found to release")    

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
