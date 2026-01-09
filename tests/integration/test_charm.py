#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant

from integration.helpers.cassandra import (
    OPERATOR_PASSWORD,
    assert_rows,
    prepare_keyspace_and_table,
    read_n_rows,
    write_n_rows,
)
from integration.helpers.juju import app_secret_extract, get_hosts

logger = logging.getLogger(__name__)


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
    )
    juju.wait(jubilant.all_active)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=600,
    )


def test_write_read(juju: jubilant.Juju, app_name: str) -> None:
    hosts = get_hosts(juju, app_name)
    password = app_secret_extract(juju, app_name, OPERATOR_PASSWORD)

    prepare_keyspace_and_table(hosts=hosts, password=password)
    wrote = write_n_rows(hosts=hosts, password=password)
    got = read_n_rows(hosts=hosts, password=password)

    assert_rows(wrote, got)
