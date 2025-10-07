#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess
from contextlib import contextmanager
from typing import Generator

import jubilant
import requests
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from tenacity import Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)
COS_METRICS_PORT = 7071
DEFAULT_MICROK8S_CHANNEL = "1.32-strict"


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


def get_db_users(juju, app_name) -> set[str]:
    """Return a set of all Cassandra user names for the given application."""
    users: set[str] = set()

    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)]
    ) as session:
        rows = session.execute("SELECT role FROM system_auth.roles;")
        users = {row.role for row in rows}

    return users


def keyspace_exists(juju, app_name, keyspace_name: str) -> bool:
    """Check if the given Cassandra keyspace exists."""
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)]
    ) as session:
        query = """
        SELECT keyspace_name
        FROM system_schema.keyspaces
        WHERE keyspace_name = %s
        """
        result = session.execute(query, (keyspace_name,))
        return bool(result.one())


def table_exists(juju, app_name, keyspace_name: str, table_name: str) -> bool:
    """Check if the given table exists in the specified Cassandra keyspace."""
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)]
    ) as session:
        query = """
        SELECT table_name
        FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
        """
        result = session.execute(query, (keyspace_name, table_name))
        return bool(result.one())


def get_user_permissions(juju, app_name, username: str) -> set[str]:
    """Return a set of permissions granted to the given Cassandra user."""
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)]
    ) as session:
        rows = session.execute(f'LIST ALL PERMISSIONS OF "{username}";')
        return {row.permission for row in rows}


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


def run_script(script: str) -> None:
    """Run a script on Linux OS.

    Args:
       script (str): Bash script

    Raises:
       OSError: If the script run fails.
    """
    for line in script.split("\n"):
        command = line.strip()

        if not command or command.startswith("#"):
            continue

        logger.info(command)
        ret_code = os.system(command)

        if ret_code:
            raise OSError(f'command "{command}" failed with error code {ret_code}')


@contextmanager
def using_vm():
    old = os.environ.get("JUJU_CONTROLLER")
    os.environ["JUJU_CONTROLLER"] = get_vm_controller(jubilant.Juju())
    try:
        yield
    finally:
        if old is not None:
            os.environ["JUJU_CONTROLLER"] = old
        else:
            os.environ.pop("JUJU_CONTROLLER", None)


@contextmanager
def using_k8s():
    old = os.environ.get("JUJU_CONTROLLER")
    os.environ["JUJU_CONTROLLER"] = get_microk8s_controller(jubilant.Juju())
    try:
        yield
    finally:
        if old is not None:
            os.environ["JUJU_CONTROLLER"] = old
        else:
            os.environ.pop("JUJU_CONTROLLER", None)


def _get_microk8s_controller_name(juju: jubilant.Juju) -> str:
    controllers_raw = json.loads(
        jubilant.Juju().cli("controllers", "--format", "json", include_model=False)
    )

    for name, data in controllers_raw.get("controllers", {}).items():
        if data.get("cloud") == "microk8s":
            return name

    return ""


def get_vm_controller(juju: jubilant.Juju) -> str:
    """Return the localhost controller name, boots up a new one if not existent."""
    controllers_raw = json.loads(juju.cli("controllers", "--format", "json", include_model=False))

    for name, data in controllers_raw.get("controllers", {}).items():
        if data.get("cloud") == "localhost":
            logger.info(f"Localhost controller '{name}' exists, skipping setup...")
            return name

    jubilant.Juju().cli("bootstrap", "localhost", include_model=False)

    return _get_microk8s_controller_name(juju)


def get_microk8s_controller(juju: jubilant.Juju) -> str:
    """Return the microk8s controller name, boots up a new one if not existent."""
    controllers_raw = json.loads(juju.cli("controllers", "--format", "json", include_model=False))

    for name, data in controllers_raw.get("controllers", {}).items():
        if data.get("cloud") == "microk8s":
            logger.info(f"Microk8s controller '{name}' exists, skipping setup...")
            return name

    configure_microk8s()

    jubilant.Juju().cli("bootstrap", "microk8s", include_model=False)

    return _get_microk8s_controller_name(juju)


def configure_microk8s(channel: str = DEFAULT_MICROK8S_CHANNEL) -> None:
    """Install and configure MicroK8s with the given channel.

    Args:
        channel: The snap channel to install MicroK8s from (e.g., '1.32-strict').
    """
    user_env_var = os.environ.get("USER", "root")
    os.system("sudo apt install -y jq")

    ip_addr = subprocess.check_output(
        "ip -4 -j route get 2.2.2.2 | jq -r '.[] | .prefsrc'",
        shell=True,
        text=True,
        timeout=5,
        universal_newlines=True,
    ).strip()

    run_script(
        f"""
         # install microk8s
         sudo snap install microk8s --channel={channel}

         # configure microk8s
         sudo groupadd --non-unique --gid "$(getent group adm | cut -f3 -d:)" microk8s
         sudo usermod -a -G microk8s {user_env_var}
         mkdir -p ~/.kube
         chmod 0700 ~/.kube

         # ensure microk8s is up
         sudo microk8s status --wait-ready

         # enable required addons
         sudo microk8s enable dns
         sudo microk8s enable hostpath-storage
         sudo microk8s enable metallb:{ip_addr}-{ip_addr}

         # configure & bootstrap microk8s controller
         sudo mkdir -p /var/snap/juju/current/microk8s/credentials
         sudo microk8s config | sudo tee /var/snap/juju/current/microk8s/credentials/client.config
         sudo chown -R {user_env_var}:{user_env_var} /var/snap/juju/current/microk8s/credentials
         sleep 90
        """
    )


def prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{COS_METRICS_PORT}/metrics"
    logger.info(f"prometheus_exporter_data making request: {url}")
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.info(f"prometheus_exporter_data exception: {e}")
        return None

    if response.status_code == 200:
        return response.text

    return None


def all_prometheus_exporters_data(juju: jubilant.Juju, check_field: str, app_name: str) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    status = juju.status()
    for unit in status.apps[app_name].units.values():
        result = result and check_field in (prometheus_exporter_data(unit.public_address) or "")
    return result


def get_peer_app_data(juju: jubilant.Juju, app_name: str, peer_name: str) -> dict[str, str]:
    """Return peer relation application data for the given app as a dict."""
    unit_name = next(iter(juju.status().apps[app_name].units))

    result = juju.cli("show-unit", unit_name, "--format", "json")
    unit_info = json.loads(result)
    unit_data = unit_info[unit_name]

    logger.info(f"INFO: {unit_info}")

    for rel in unit_data.get("relation-info", []):
        if rel["endpoint"] == peer_name:
            app_data = rel.get("application-data", {})
            return {k: str(v) for k, v in app_data.items()}

    raise RuntimeError(f"No peer relation data found for application {app_name}")
