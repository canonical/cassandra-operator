#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
import subprocess
import jubilant
from subprocess import PIPE, check_output
from contextlib import contextmanager
from typing import Generator

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

logger = logging.getLogger(__name__)

@contextmanager
def connect_cql(
    hosts: list[str],
    user: str | None = None,
    password: str | None = None,
    keyspace: str | None = None,
    timeout: float | None = None,
) -> Generator[Session, None, None]:
    execution_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        request_timeout=timeout or 10,
    )
    auth_provider = (
        PlainTextAuthProvider(username=user, password=password)
        if user is not None and password is not None
        else None
    )
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


def get_secret_by_label(juju: jubilant.Juju, label: str, owner: str) -> dict[str, str]:

    
    
    secrets_meta_raw = juju.cli("list-secrets --format json",include_model=True)
    secrets_meta = json.loads(secrets_meta_raw)
    secret_id = None

    for secret_id in secrets_meta:
        if owner and not secrets_meta[secret_id]["owner"] == owner:
            continue
        if secrets_meta[secret_id]["label"] == label:
            secret_id = secret_id
            break

    if not secret_id:
        return dict()
    
    secrets_data_raw = juju.cli(f"show-secret --reveal --format json {secret_id}",include_model=True)

    secret_data = json.loads(secrets_data_raw)
    return secret_data[secret_id]["content"]["Data"]
        

def check_tls(ip: str, port: int) -> bool:
    """Проверяет, поддерживает ли сервер TLS (v1.2 или v1.3) на заданном IP и порте."""
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

    output = proc.stdout + proc.stderr  # объединяем вывод, чтобы проверить TLS-версии

    if proc.returncode != 0:
        logger.debug(f"OpenSSL exited with code {proc.returncode} on {ip}:{port}")

    return "TLSv1.2" in output or "TLSv1.3" in output

def get_address(juju: jubilant.Juju, app_name: str, unit_num) -> str:
    """Get the address for a unit."""
    
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_num}"].public_address
    return address
    
def check_node_is_up(juju: jubilant.Juju, app_name: str, unit_num: int, unit_addr: str) -> bool:
    nd_tool_status_raw = juju.ssh(target=f"{app_name}/{unit_num}", command="sudo snap run charmed-cassandra.nodetool status")

    for line in nd_tool_status_raw.split('\n'):
        line = line.strip()
        if unit_addr in line:
            if line.startswith('UN '):
                return True
            else:
                return False    

    return False

