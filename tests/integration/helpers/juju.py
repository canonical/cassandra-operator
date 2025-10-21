#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess
from contextlib import contextmanager

import jubilant
from jubilant.statustypes import UnitStatus

logger = logging.getLogger(__name__)


DEFAULT_MICROK8S_CHANNEL = "1.32-strict/stable"


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

