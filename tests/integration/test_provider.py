#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import time
import pytest
from dataclasses import dataclass
from pathlib import Path
from cassandra.cluster import NoHostAvailable

import jubilant
from help_types import IntegrationTestsCharms
from helpers import (
    get_cluster_client_ca,
    get_db_users,
    get_peer_app_data,
    get_user_permissions,
    keyspace_exists,
    table_exists,
)
from tenacity import Retrying, stop_after_delay, wait_fixed

logger = logging.getLogger(__name__)

REQUIRER_PEER_RELATION = "local"
CLIENT_RELATION = "cassandra-client"
KEYSPACE_NAME = "test_keyspace_one"
TABLE_NAME = "test_table_one"
USER_KEYSPACE_PERMISSIONS = {"SELECT", "MODIFY"}
USER_KEYSPACE_ALL_PERMISSIONS = {"ALTER", "AUTHORIZE", "DROP", "MODIFY", "SELECT", "CREATE"}


def test_deploy_with_requirer(
    juju: jubilant.Juju,
    cassandra_charm: Path,
    app_name: str,
    requirer_charm: Path,
    requirer_app_name: str,
) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=2,
    )

    juju.deploy(
        requirer_charm,
        app=requirer_app_name,
        config={
            "keyspace-name": KEYSPACE_NAME,
            "user-permissions": ",".join(USER_KEYSPACE_ALL_PERMISSIONS),
        },
        num_units=1,
    )

    juju.wait(jubilant.all_active, timeout=1200)


def test_integrate_client(juju: jubilant.Juju, app_name: str, requirer_app_name: str) -> None:
    old_users = get_db_users(juju, app_name)

    juju.integrate(f"{app_name}:{CLIENT_RELATION}", requirer_app_name)

    juju.wait(jubilant.all_active, delay=5, successes=5)

    with_new_users = get_db_users(juju, app_name)

    new_users = with_new_users - old_users

    # Assert that the admin user and keyspace were created after a successful integration.
    # The application charm makes two requests:
    # 1. For the resource itself (initial user and keyspace)
    # 2. For the resource entity (regular user)
    assert len(new_users) == 2
    assert keyspace_exists(juju, app_name, KEYSPACE_NAME)

    for user in new_users:
        user_perms = get_user_permissions(juju, app_name, user)
        assert USER_KEYSPACE_ALL_PERMISSIONS.issubset(user_perms), (
            f"{USER_KEYSPACE_ALL_PERMISSIONS} not in {user_perms}"
        )


def test_create_table(juju: jubilant.Juju, app_name: str, requirer_app_name: str) -> None:
    requirer_unit = next(iter(juju.status().apps[requirer_app_name].units))

    juju.run(requirer_unit, "create-table", {"table-name": TABLE_NAME})

    juju.wait(jubilant.all_active)

    assert table_exists(juju, app_name, KEYSPACE_NAME, TABLE_NAME)


def test_connection_updated_on_tls_enabled(
    juju: jubilant.Juju,
    app_name: str,
    requirer_app_name: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    table_prefix = "_tls_enabled"

    juju.deploy(
        **charm_versions.tls.deploy_dict(),
        config={"ca-common-name": "cassandra"},
    )

    juju.wait(jubilant.all_active)

    juju.integrate(f"{charm_versions.tls.app}:certificates", f"{app_name}:client-certificates")

    # Wait for client_certs rotation
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=6,
        timeout=1000,
    )

    ca = get_cluster_client_ca(juju, app_name)

    requirer_unit = next(iter(juju.status().apps[requirer_app_name].units))

    juju.run(requirer_unit, "create-table", {"table-name": TABLE_NAME + table_prefix})

    juju.wait(jubilant.all_active)

    assert table_exists(juju, app_name, KEYSPACE_NAME, TABLE_NAME + table_prefix, client_ca=ca)


def test_connection_no_tls_on_tls_enabled(
    juju: jubilant.Juju,
    app_name: str,
) -> None:
    with pytest.raises(NoHostAvailable) as exc_info:
        table_exists(juju, app_name, KEYSPACE_NAME, TABLE_NAME)

    err = exc_info.value
    assert isinstance(err, NoHostAvailable)
    
def test_connection_updated_on_tls_updated(
    juju: jubilant.Juju,
    app_name: str,
    requirer_app_name: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    table_prefix = "_tls_updated"

    tls_unit = next(iter(juju.status().apps[charm_versions.tls.app].units))

    juju.run(tls_unit, "rotate-private-key")

    time.sleep(20)

    # Wait for client_certs rotation
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=6,
        timeout=1000,
    )

    ca = get_cluster_client_ca(juju, app_name)

    requirer_unit = next(iter(juju.status().apps[requirer_app_name].units))

    juju.run(requirer_unit, "create-table", {"table-name": TABLE_NAME + table_prefix})

    juju.wait(jubilant.all_active)

    assert table_exists(juju, app_name, KEYSPACE_NAME, TABLE_NAME + table_prefix, client_ca=ca)


def test_connection_updated_on_tls_disabled(
    juju: jubilant.Juju,
    app_name: str,
    requirer_app_name: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    table_prefix = "_tls_disabled"

    juju.remove_relation(
        f"{charm_versions.tls.app}:certificates", f"{app_name}:client-certificates"
    )

    # Wait for client_certs rotation
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=5,
        successes=4,
        timeout=1000,
    )

    requirer_unit = next(iter(juju.status().apps[requirer_app_name].units))

    juju.run(requirer_unit, "create-table", {"table-name": TABLE_NAME + table_prefix})

    juju.wait(jubilant.all_active)

    assert table_exists(juju, app_name, KEYSPACE_NAME, TABLE_NAME + table_prefix)


def test_change_user_permissions(
    juju: jubilant.Juju, app_name: str, requirer_app_name: str
) -> None:
    juju.config(requirer_app_name, {"user-permissions": ",".join(USER_KEYSPACE_PERMISSIONS)})

    juju.wait(jubilant.all_active)

    user = _get_requested_user(juju, requirer_app_name)
    user_perms = get_user_permissions(juju, app_name, user.username)

    assert USER_KEYSPACE_PERMISSIONS.issubset(user_perms), (
        f"{USER_KEYSPACE_PERMISSIONS} not in {user_perms}"
    )


def test_remove_user_after_relation_broken(
    juju: jubilant.Juju, app_name: str, requirer_app_name: str
) -> None:
    requested_user = _get_requested_user(juju, requirer_app_name)
    initial_user = _get_initial_user(juju, requirer_app_name)

    juju.remove_relation(f"{requirer_app_name}:cassandra-client", f"{app_name}:cassandra-client")
    juju.wait(lambda status: jubilant.all_active(status, app_name))

    for attempt in Retrying(wait=wait_fixed(2), stop=stop_after_delay(120), reraise=True):
        with attempt:
            users_left = get_db_users(juju, app_name)
            assert requested_user.username not in users_left, (
                f"User {requested_user.username} still exists"
            )
            assert initial_user.username not in users_left, (
                f"User {initial_user.username} still exists"
            )


@dataclass
class User:
    username: str
    password: str


def _get_requested_user(juju: jubilant.Juju, requirer_app_name: str) -> User:
    """Read the user credentials from the peer relation of the requirer app."""
    data = get_peer_app_data(juju, requirer_app_name, REQUIRER_PEER_RELATION)
    username = data.get("ks_user_rolename", "")
    password = data.get("ks_user_password", "")

    return User(username=username, password=password)


def _get_initial_user(juju: jubilant.Juju, requirer_app_name: str) -> User:
    """Read the user credentials from the peer relation of the requirer app."""
    data = get_peer_app_data(juju, requirer_app_name, REQUIRER_PEER_RELATION)
    username = data.get("ks_owner_rolename", "")
    password = data.get("ks_owner_password", "")

    return User(username=username, password=password)
