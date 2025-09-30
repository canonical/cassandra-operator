#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import subprocess
from contextlib import contextmanager
from typing import Generator

import jubilant
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from helpers.juju import get_secrets_by_label
from tenacity import Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)

COS_METRICS_PORT = 7071


@contextmanager
def connect_cql(
    juju: jubilant.Juju,
    app_name: str,
    hosts: list[str] | None = None,
    username: str | None = None,
    password: str | None = None,
    keyspace: str | None = None,
    timeout: float | None = None,
) -> Generator[Session, None, None]:
    if hosts is None:
        hosts = [unit.public_address for unit in juju.status().apps[app_name].units.values()]
    if username is None:
        username = "operator"
    if password is None:
        secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
        assert len(secrets) == 1
        password = secrets[0]["operator-password"]

    execution_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        request_timeout=timeout or 10,
    )
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    # TODO: get rid of retrying on connection.
    cluster = None
    session = None
    for attempt in Retrying(wait=wait_fixed(2), stop=stop_after_delay(120), reraise=True):
        with attempt:
            cluster = Cluster(
                auth_provider=auth_provider,
                contact_points=hosts,
                protocol_version=5,
                execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
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
