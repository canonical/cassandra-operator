#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cassandra CQL client."""

import logging
from contextlib import contextmanager
from typing import Generator

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

logger = logging.getLogger(__name__)


class CassandraClient:
    """TODO."""

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
