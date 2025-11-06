#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
import logging
import subprocess
from contextlib import contextmanager
from ssl import CERT_NONE, PROTOCOL_TLS_CLIENT, SSLContext
from typing import Generator

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, ResultSet, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from tenacity import Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)

CLIENT_CA_CERT = "client-ca-cert-secret"


OPERATOR_PASSWORD = "operator-password"


@contextmanager
def connect_cql(
    hosts: list[str],
    password: str | None,
    username: str | None = None,
    keyspace: str | None = None,
    client_ca: str | None = None,
    timeout: float | None = None,
) -> Generator[Session, None, None]:
    """Connect to the Cassandra cluster and acquire CQL session."""
    if username is None:
        username = "operator"

    assert password
    assert len(hosts) > 0

    execution_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        request_timeout=timeout or 10,
    )
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    # TODO: get rid of retrying on connection.
    cluster = None
    session = None

    ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)
    if client_ca:
        logger.info(f"Loading SSL context with cert: {client_ca}")

        # TODO: change for mTLS
        ssl_context.check_hostname = False
        ssl_context.verify_mode = CERT_NONE

        ssl_context.load_verify_locations(cadata=client_ca)
    else:
        logger.info("SSL context is disabled")
        ssl_context = None

    for attempt in Retrying(wait=wait_fixed(10), stop=stop_after_delay(600), reraise=True):
        with attempt:
            cluster = Cluster(
                auth_provider=auth_provider,
                contact_points=hosts,
                protocol_version=5,
                execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
                ssl_context=ssl_context,
            )
            session = cluster.connect()
    assert cluster and session
    if keyspace:
        session.set_keyspace(keyspace)
    try:
        yield session
    finally:
        cluster.shutdown()


def check_tls(ip: str, port: int) -> bool:
    try:
        proc = subprocess.run(
            f"echo | openssl s_client -connect {ip}:{port}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        logger.debug(f"OpenSSL timeout on {ip}:{port}")
        return False

    output = proc.stdout + proc.stderr

    if proc.returncode != 0:
        logger.debug(f"OpenSSL exited with code {proc.returncode} on {ip}:{port}")

    return "TLSv1.2" in output or "TLSv1.3" in output


def get_db_users(
    hosts: list[str],
    password: str | None,
    username: str | None = None,
    client_ca: str | None = None,
) -> set[str]:
    """Return a set of all Cassandra user names for the given application."""
    users: set[str] = set()

    with connect_cql(
        hosts=hosts, password=password, username=username, client_ca=client_ca
    ) as session:
        rows = session.execute("SELECT role FROM system_auth.roles;")
        users = {row.role for row in rows}

    return users


def keyspace_exists(
    hosts: list[str],
    password: str | None,
    keyspace_name: str,
    username: str | None = None,
    client_ca: str | None = None,
) -> bool:
    """Check if the given Cassandra keyspace exists."""
    with connect_cql(
        hosts=hosts, password=password, username=username, client_ca=client_ca
    ) as session:
        query = """
        SELECT keyspace_name
        FROM system_schema.keyspaces
        WHERE keyspace_name = %s
        """
        result = session.execute(query, (keyspace_name,))
        return bool(result.one())


def table_exists(
    hosts: list[str],
    password: str | None,
    keyspace_name: str,
    table_name: str,
    username: str | None = None,
    client_ca: str | None = None,
) -> bool:
    """Check if the given table exists in the specified Cassandra keyspace."""
    with connect_cql(
        hosts=hosts, password=password, username=username, client_ca=client_ca
    ) as session:
        query = """
        SELECT table_name
        FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
        """
        result = session.execute(query, (keyspace_name, table_name))
        return bool(result.one())


def prepare_keyspace_and_table(
    hosts: list[str],
    password: str | None,
    username: str | None = None,
    ks: str = "test",
    table: str = "kv",
) -> tuple[str, str]:
    """Create test keyspace and table."""
    with connect_cql(hosts=hosts, password=password, username=username, timeout=300) as session:
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {ks} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
        )
        session.set_keyspace(ks)
        session.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, value TEXT)")
    return ks, table


def write_n_rows(
    hosts: list[str],
    password: str | None,
    username: str | None = None,
    ks: str = "test",
    table: str = "kv",
    n: int = 100,
) -> dict[int, str]:
    """Write n rows to the table."""
    with connect_cql(
        hosts=hosts, password=password, username=username, timeout=300, keyspace=ks
    ) as session:
        for i in range(n):
            session.execute(
                f"INSERT INTO {table} (id, value) VALUES (%s, %s)",
                (i, f"msg-{i}"),
            )

    return {i: f"msg-{i}" for i in range(n)}


def read_n_rows(
    hosts: list[str],
    password: str | None,
    username: str | None = None,
    ks: str = "test",
    table: str = "kv",
    n: int = 100,
) -> dict[int, str]:
    """Check that table have exactly n rows."""
    got = {}
    with connect_cql(
        hosts=hosts, password=password, username=username, timeout=300, keyspace=ks
    ) as session:
        res = session.execute(f"SELECT id, value FROM {table}")
        assert isinstance(res, ResultSet)
        rows = res.all()
        if len(rows) != n:
            return got

        got = {row.id: row.value for row in rows}

    return got


def assert_rows(wrote: dict[int, str], got: dict[int, str]) -> None:
    """Assert rows are equal."""
    assert len(got) == len(wrote), f"Expected {len(wrote)} rows, got {len(got)}"
    assert got == wrote, "Row data mismatch"


def get_user_permissions(
    hosts: list[str],
    password: str | None,
    target_username: str,
    username: str | None = None,
    client_ca: str | None = None,
) -> set[str]:
    """Return a set of permissions granted to the given Cassandra user."""
    with connect_cql(
        hosts=hosts,
        password=password,
        username=username,
        client_ca=client_ca,
    ) as session:
        rows = session.execute(f'LIST ALL PERMISSIONS OF "{target_username}";')
        return {row.permission for row in rows}
