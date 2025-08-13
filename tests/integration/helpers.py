#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
from contextlib import contextmanager
from typing import Generator

import jubilant
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

logger = logging.getLogger(__name__)


@contextmanager
def connect_cql(
    juju: jubilant.Juju,
    app_name: str,
    hosts: list[str],
    username: str | None = None,
    password: str | None = None,
    keyspace: str | None = None,
    timeout: float | None = None,
) -> Generator[Session, None, None]:
    if username is None:
        username = "cassandra"
    if password is None:
        secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
        assert len(secrets) == 1
        password = secrets[0]["cassandra-password"]

    execution_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        request_timeout=timeout or 10,
    )
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(
        auth_provider=auth_provider,
        contact_points=hosts,
        protocol_version=5,
        execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
    )
    session = cluster.connect()
    if keyspace:
        session.set_keyspace(keyspace)
    try:
        yield session
    finally:
        cluster.shutdown()


def get_secrets_by_label(juju: jubilant.Juju, label: str, owner: str) -> list[dict[str, str]]:
    secrets_meta_raw = juju.cli("secrets", "--format", "json", include_model=True)
    secrets_meta = json.loads(secrets_meta_raw)

    logger.info(f"raw secrets: {secrets_meta}")

    selected_secret_ids = []

    for secret_id in secrets_meta:
        if owner and not secrets_meta[secret_id]["owner"] == owner:
            continue
        if secrets_meta[secret_id]["label"] == label:
            selected_secret_ids.append(secret_id)

    if len(selected_secret_ids) == 0:
        return []

    logger.info(f"selected secrets ids: {selected_secret_ids}")

    secret_data_list = []

    for selected_secret_id in selected_secret_ids:
        secrets_data_raw = juju.cli(
            "show-secret", "--reveal", "--format", "json", selected_secret_id, include_model=True
        )

        logger.info(f"revealed secret {selected_secret_id}: {secrets_data_raw}")

        secret_data_list.append(
            json.loads(secrets_data_raw)[selected_secret_id]["content"]["Data"]
        )

    return secret_data_list


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


def get_address(juju: jubilant.Juju, app_name: str, unit_num) -> str:
    """Get the address for a unit."""
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_num}"].public_address
    return address


def check_node_is_up(juju: jubilant.Juju, app_name: str, unit_num: int, unit_addr: str) -> bool:
    nd_tool_status_raw = juju.ssh(
        target=f"{app_name}/{unit_num}",
        command="sudo snap run charmed-cassandra.nodetool status",
    )

    for line in nd_tool_status_raw.split("\n"):
        line = line.strip()
        if unit_addr in line:
            if line.startswith("UN "):
                return True
            else:
                return False

    return False
