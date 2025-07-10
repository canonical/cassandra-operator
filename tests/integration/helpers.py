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
import yaml

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load((Path(__file__).resolve().parents[2] / "metadata.yaml").read_text())
APP_NAME = METADATA["name"]

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
    try:
        result = check_output(
            f"echo | openssl s_client -connect {ip}:{port}",
            stderr=PIPE,
            shell=True,
            universal_newlines=True,
        )
        return bool(result)

    except subprocess.CalledProcessError as e:
        logger.error(f"command '{e.cmd}' return with error (code {e.returncode}): {e.output}")
        return False

async def get_address(juju: jubilant.Juju, app_name=APP_NAME, unit_num=0) -> str:
    """Get the address for a unit."""
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_num}"].public_address
    return address
    
def check_node_is_up(juju: jubilant.Juju, unit_num: int, unit_addr: str, app_name: str) -> bool:
    nd_tool_status_raw = juju.ssh(target=f"{app_name}/{unit_num}", command="sudo snap run charmed-cassandra.nodetool status")

    for line in nd_tool_status_raw.split('\n'):
        line = line.strip()
        if unit_addr in line:
            # Проверяем, начинается ли строка с "UN "
            if line.startswith('UN '):
                return True
            else:
                return False    

    return False
