#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from pytest import raises

from integration.helpers.cassandra import (
    OPERATOR_PASSWORD,
    prepare_keyspace_and_table,
    write_n_rows,
)
from integration.helpers.juju import (
    app_secret_extract,
    get_hosts,
)

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

    assert (password := app_secret_extract(juju, app_name, OPERATOR_PASSWORD)) == "custom_password"
    prepare_keyspace_and_table(hosts=get_hosts(juju, app_name), password=password)


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

    assert (
        password := app_secret_extract(juju, app_name, OPERATOR_PASSWORD)
    ) == "custom_password_second"
    write_n_rows(hosts=get_hosts(juju, app_name), password=password)


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

    assert (
        password := app_secret_extract(juju, app_name, OPERATOR_PASSWORD)
    ) == "custom_password_second"
    write_n_rows(hosts=get_hosts(juju, app_name), password=password)


def test_bad_credentials(juju: jubilant.Juju, app_name: str) -> None:
    with raises(match="AuthenticationFailed"):
        write_n_rows(
            hosts=get_hosts(juju, app_name),
            username="bad",
            password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
        )

    with raises(match="AuthenticationFailed"):
        write_n_rows(
            hosts=get_hosts(juju, app_name),
            password="bad",
        )
