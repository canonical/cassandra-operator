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
from integration.helpers.juju import (
    app_secret_extract,
    get_leader_unit,
    get_non_leader_units,
    get_unit_address,
)

logger = logging.getLogger(__name__)


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=3,
    )
    juju.wait(jubilant.all_active, timeout=1000)


def test_write_primary_read_secondary(juju: jubilant.Juju, app_name: str) -> None:
    password = app_secret_extract(juju, app_name, OPERATOR_PASSWORD)

    leader, _ = get_leader_unit(juju, app_name)
    leader_hosts = [get_unit_address(juju, app_name, leader)]
    prepare_keyspace_and_table(leader_hosts, password=password)
    wrote = write_n_rows(hosts=leader_hosts, password=password)

    got1 = read_n_rows(
        hosts=[get_unit_address(juju, app_name, get_non_leader_units(juju, app_name)[0])],
        password=password,
    )
    assert_rows(wrote, got1)

    got2 = read_n_rows(
        hosts=[get_unit_address(juju, app_name, get_non_leader_units(juju, app_name)[1])],
        password=password,
    )
    assert_rows(wrote, got2)
