#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from pytest import raises

from integration.helpers.cassandra import connect_cql
from integration.helpers.juju import get_hosts, get_secrets_by_label

logger = logging.getLogger(__name__)


def test_deploy_bad_custom_secret(
    juju: jubilant.Juju, cassandra_charm: Path, app_name: str
) -> None:
    custom_secret = juju.add_secret("custom_secret", {"foo": "bar"})

    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing", "system-users": custom_secret},
    )

    juju.cli("grant-secret", "custom_secret", app_name)

    juju.wait(jubilant.all_blocked)


def test_update_custom_secret(juju: jubilant.Juju, app_name: str) -> None:
    juju.cli("update-secret", "custom_secret", "operator=custom_password")

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("operator-password") == "custom_password"
    with connect_cql(juju=juju, app_name=app_name, hosts=get_hosts(juju, app_name)) as session:
        session.execute(
            "CREATE KEYSPACE test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace("test")
        session.execute("CREATE TABLE test(message TEXT PRIMARY KEY)")


def test_change_custom_secret(juju: jubilant.Juju, app_name: str) -> None:
    custom_secret_second = juju.add_secret(
        "custom_secret_second", {"operator": "custom_password_second"}
    )
    juju.cli("grant-secret", "custom_secret_second", app_name)

    juju.config(app=app_name, values={"profile": "testing", "system-users": custom_secret_second})
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("operator-password") == "custom_password_second"
    with connect_cql(
        juju=juju, app_name=app_name, hosts=get_hosts(juju, app_name), keyspace="test"
    ) as session:
        session.execute("INSERT INTO test(message) VALUES ('hello')")


def test_remove_custom_secret(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app=app_name, values={"profile": "testing", "system-users": ""})
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    juju.cli("update-secret", "custom_secret_second", "operator=custom_password_third")
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=300,
    )

    secrets = get_secrets_by_label(juju, f"cassandra-peers.{app_name}.app", app_name)
    assert len(secrets) == 1 and secrets[0].get("operator-password") == "custom_password_second"
    with connect_cql(
        juju=juju, app_name=app_name, hosts=get_hosts(juju, app_name), keyspace="test"
    ) as session:
        session.execute("INSERT INTO test(message) VALUES ('world')")


def test_bad_credentials(juju: jubilant.Juju, app_name: str) -> None:
    with raises(match="AuthenticationFailed"):
        with connect_cql(
            juju=juju,
            app_name=app_name,
            hosts=get_hosts(juju, app_name),
            username="bad",
            keyspace="test",
        ) as session:
            session.execute("INSERT INTO test(message) VALUES ('bad')")

    with raises(match="AuthenticationFailed"):
        with connect_cql(
            juju=juju,
            app_name=app_name,
            hosts=get_hosts(juju, app_name),
            password="bad",
            keyspace="test",
        ) as session:
            session.execute("INSERT INTO test(message) VALUES ('password')")
