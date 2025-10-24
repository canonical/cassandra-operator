#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess
from contextlib import contextmanager
from ssl import CERT_NONE, PROTOCOL_TLS_CLIENT, SSLContext
from typing import Generator

import jubilant
import requests
import tenacity
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, ResultSet, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from tenacity import Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)
COS_METRICS_PORT = 7071
CLIENT_CA_CERT = "client-ca-cert-secret"
DEFAULT_MICROK8S_CHANNEL = "1.32-strict/stable"


@contextmanager
def connect_cql(
    juju: jubilant.Juju,
    app_name: str,
    hosts: list[str],
    username: str | None = None,
    password: str | None = None,
    keyspace: str | None = None,
    client_ca: str | None = None,
    timeout: float | None = None,
) -> Generator[Session, None, None]:
    if username is None:
        username = "operator"
    if password is None:
        secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
        assert len(secrets) == 1
        password = secrets[0]["operator-password"]

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

    for attempt in Retrying(wait=wait_fixed(2), stop=stop_after_delay(120), reraise=True):
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


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(AssertionError),
    stop=stop_after_delay(60),
    wait=wait_fixed(5),
    reraise=True,
)
def get_cluster_client_ca(juju: jubilant.Juju, app_name: str) -> str:
    certs: set[str] = set()

    for unit in get_unit_names(juju, app_name):
        client_ca = unit_secret_extract(
            juju,
            unit_name=unit,
            secret_name=CLIENT_CA_CERT,
        )

        if client_ca:
            certs.add(client_ca)

    if len(certs) != 1:
        logger.warning(
            f"Units have different or missing client CA certs ({len(certs)} found). Retrying..."
        )
        raise AssertionError(
            "Units have different client certificates, waiting for rotation to end"
        )

    return certs.pop()


def get_user_permissions(juju, app_name, username: str, client_ca: str | None = None) -> set[str]:
    """Return a set of permissions granted to the given Cassandra user."""
    with connect_cql(
        juju=juju,
        app_name=app_name,
        hosts=get_hosts(juju, app_name),
        client_ca=client_ca,
    ) as session:
        rows = session.execute(f'LIST ALL PERMISSIONS OF "{username}";')
        return {row.permission for row in rows}


def get_secrets_by_label(juju: jubilant.Juju, label: str, owner: str) -> list[dict[str, str]]:
    secrets_meta_raw = juju.cli("secrets", "--format", "json", include_model=True)
    secrets_meta = json.loads(secrets_meta_raw)

    selected_secret_ids = []

    for secret_id in secrets_meta:
        if owner and not secrets_meta[secret_id]["owner"] == owner:
            continue
        if secrets_meta[secret_id]["label"] == label:
            selected_secret_ids.append(secret_id)

    if len(selected_secret_ids) == 0:
        return []

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


def get_unit_address(juju: jubilant.Juju, app_name: str, unit_num) -> str:
    """Get the addresses for a units."""
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_num}"].public_address
    return address


def get_unit_names(juju: jubilant.Juju, app_name: str) -> list[str]:
    """Get the names for a units."""
    units = juju.status().apps[app_name].units.items()
    addresses = [u[0] for u in units]
    return addresses


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

    for rel in unit_data.get("relation-info", []):
        if rel["endpoint"] == peer_name:
            app_data = rel.get("application-data", {})
            return {k: str(v) for k, v in app_data.items()}

    raise RuntimeError(f"No peer relation data found for application {app_name}")


def unit_secret_extract(juju: jubilant.Juju, unit_name: str, secret_name: str) -> str | None:
    user_secret = get_secrets_by_label(
        juju,
        label=f"cassandra-peers.{unit_name.split('/')[0]}.unit",
        owner=unit_name,
    )

    for secret in user_secret:
        if found := secret.get(secret_name):
            return found

    return None


def app_secret_extract(juju: jubilant.Juju, cluster_name: str, secret_name: str) -> str | None:
    user_secret = get_secrets_by_label(
        juju,
        label=f"cassandra-peers.{cluster_name}.app",
        owner=cluster_name,
    )

    for secret in user_secret:
        if found := secret.get(secret_name):
            return found

    return None


def get_hosts(juju: jubilant.Juju, app_name: str, unit_name: str = "") -> list[str]:
    """Return list of host addresses for the given app.

    Adding unit_name will prioritize a specific unit host address.
    """
    units = juju.status().apps[app_name].units
    if unit_name:
        if unit_name not in units:
            raise ValueError(f"Unit {unit_name} not found in app {app_name}")
        return [units[unit_name].public_address]
    return [u.public_address for u in units.values()]


def get_db_users(juju, app_name, client_ca: str | None = None) -> set[str]:
    """Return a set of all Cassandra user names for the given application."""
    users: set[str] = set()

    with connect_cql(
        juju=juju, app_name=app_name, hosts=get_hosts(juju, app_name), client_ca=client_ca
    ) as session:
        rows = session.execute("SELECT role FROM system_auth.roles;")
        users = {row.role for row in rows}

    return users


def keyspace_exists(juju, app_name, keyspace_name: str, client_ca: str | None = None) -> bool:
    """Check if the given Cassandra keyspace exists."""
    with connect_cql(
        juju=juju, app_name=app_name, hosts=get_hosts(juju, app_name), client_ca=client_ca
    ) as session:
        query = """
        SELECT keyspace_name
        FROM system_schema.keyspaces
        WHERE keyspace_name = %s
        """
        result = session.execute(query, (keyspace_name,))
        return bool(result.one())


def table_exists(
    juju, app_name, keyspace_name: str, table_name: str, client_ca: str | None = None
) -> bool:
    """Check if the given table exists in the specified Cassandra keyspace."""
    with connect_cql(
        juju=juju, app_name=app_name, hosts=get_hosts(juju, app_name), client_ca=client_ca
    ) as session:
        query = """
        SELECT table_name
        FROM system_schema.tables
        WHERE keyspace_name = %s AND table_name = %s
        """
        result = session.execute(query, (keyspace_name, table_name))
        return bool(result.one())


def prepare_keyspace_and_table(
    juju: jubilant.Juju, app_name: str, ks: str = "test", table: str = "kv", unit_name: str = ""
) -> tuple[str, str]:
    """Create test keyspace and table."""
    hosts = get_hosts(juju, app_name, unit_name)

    with connect_cql(juju=juju, app_name=app_name, hosts=hosts, timeout=300) as session:
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {ks} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
        )
        session.set_keyspace(ks)
        session.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, value TEXT)")
    return ks, table


def write_n_rows(
    juju: jubilant.Juju, app_name: str, ks: str, table: str, n: int = 100, unit_name: str = ""
) -> dict[int, str]:
    """Write n rows to the table."""
    hosts = get_hosts(juju, app_name, unit_name)

    with connect_cql(
        juju=juju, app_name=app_name, hosts=hosts, timeout=300, keyspace=ks
    ) as session:
        for i in range(n):
            session.execute(
                f"INSERT INTO {table} (id, value) VALUES (%s, %s)",
                (i, f"msg-{i}"),
            )

    return {i: f"msg-{i}" for i in range(n)}


def read_n_rows(
    juju: jubilant.Juju, app_name: str, ks: str, table: str, n: int = 100, unit_name: str = ""
) -> dict[int, str]:
    """Check that table have exactly n rows."""
    hosts = get_hosts(juju, app_name, unit_name)

    got = {}
    with connect_cql(
        juju=juju, app_name=app_name, hosts=hosts, timeout=300, keyspace=ks
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


def get_leader_unit(juju, app_name: str) -> str:
    """Return the name of the leader unit for the given application.

    Raises:
        ValueError: If no leader unit is found.
    """
    app = juju.status().apps[app_name]
    for name, unit in app.units.items():
        if unit.leader:
            return name
    raise ValueError(f"No leader unit found for application '{app_name}'")


def get_non_leader_units(juju, app_name: str) -> list[str]:
    """Return a list of all non-leader units for the given application."""
    app = juju.status().apps[app_name]
    return [name for name, unit in app.units.items() if not unit.leader]
