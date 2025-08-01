#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from helpers import connect_cql, get_address, get_secrets_by_label

logger = logging.getLogger(__name__)


def test_deploy_bad_custom_secret(
    juju: jubilant.Juju, cassandra_charm: Path, app_name: str
) -> None:
    custom_secret = juju.add_secret("custom_secret", {"foo": "bar"})

    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing", "system_users": custom_secret},
    )

    juju.cli("grant-secret", "custom_secret", app_name)

    juju.wait(jubilant.all_active)


def test_update_custom_secret(juju: jubilant.Juju, app_name: str) -> None:
    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("cassandra-password") != "custom_password"
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)]
    ) as session:
        session.execute(
            "CREATE KEYSPACE test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace("test")
        session.execute("CREATE TABLE test(message TEXT PRIMARY KEY)")

    juju.cli("update-secret", "custom_secret", "cassandra-password=custom_password")

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("cassandra-password") == "custom_password"
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)], keyspace="test"
    ) as session:
        session.execute("INSERT INTO test(message) VALUES ('hello')")


def test_change_custom_secret(juju: jubilant.Juju, app_name: str) -> None:
    custom_secret_second = juju.add_secret(
        "custom_secret_second", {"cassandra-password": "custom_password_second"}
    )
    juju.cli("grant-secret", "custom_secret_second", app_name)

    juju.config(app=app_name, values={"profile": "testing", "system_users": custom_secret_second})
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("cassandra-password") == "custom_password_second"
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)], keyspace="test"
    ) as session:
        session.execute("INSERT INTO test(message) VALUES ('world')")


def test_remove_custom_secret(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app=app_name, values={"profile": "testing", "system_users": ""})
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    juju.cli("update-secret", "custom_secret_second", "cassandra-password=custom_password_third")
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("cassandra-password") == "custom_password_second"
    with connect_cql(
        juju=juju, app_name=app_name, hosts=[get_address(juju, app_name, 0)], keyspace="test"
    ) as session:
        session.execute("INSERT INTO test(message) VALUES ('!')")
