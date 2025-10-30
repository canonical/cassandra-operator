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
from integration.helpers.continuous_writes import ContinuousWrites
from integration.helpers.juju import (
    app_secret_extract,
    get_hosts,
    get_unit_address,
    scale_sequentially_to,
)

logger = logging.getLogger(__name__)
TEST_ROW_NUM = 100


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
    )
    juju.wait(jubilant.all_active, timeout=1200)


def test_scale_up(juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites) -> None:
    continuous_writes.start(
        hosts=get_hosts(juju, app_name),
        password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
    )

    continuous_writes.assert_new_writes()

    scale_sequentially_to(juju, app_name, 3)

    continuous_writes.assert_new_writes(hosts=[get_unit_address(juju, app_name, 1)])

    continuous_writes.stop_and_assert_writes(hosts=[get_unit_address(juju, app_name, 2)])


def test_single_node_scale_down(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    continuous_writes.start(
        hosts=get_hosts(juju, app_name),
        password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
    )

    non_leader_units = [
        name for name, unit in juju.status().apps[app_name].units.items() if not unit.leader
    ]

    leader_unit = [
        name for name, unit in juju.status().apps[app_name].units.items() if unit.leader
    ][0]

    assert len(non_leader_units) == 2

    juju.remove_unit(non_leader_units[0])
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
        timeout=1200,
    )

    continuous_writes.assert_new_writes()

    juju.remove_unit(leader_unit)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
        timeout=1200,
    )

    new_leader_address = [
        unit.public_address
        for _, unit in juju.status().apps[app_name].units.items()
        if unit.leader
    ][0]

    assert new_leader_address

    continuous_writes.stop_and_assert_writes(hosts=[new_leader_address])


def test_single_node_scale_down_scale_up(juju: jubilant.Juju, app_name: str) -> None:
    non_leader_units = [
        name for name, unit in juju.status().apps[app_name].units.items() if not unit.leader
    ]

    assert len(non_leader_units) == 1

    non_leader_hosts = [get_unit_address(juju, app_name, non_leader_units[0].split("/")[1])]
    password = app_secret_extract(juju, app_name, OPERATOR_PASSWORD)

    ks, tb = prepare_keyspace_and_table(
        hosts=non_leader_hosts,
        password=password,
        ks="downupks",
        table="downuptbl",
    )
    wrote = write_n_rows(hosts=non_leader_hosts, password=password, ks=ks, table=tb)

    juju.remove_unit(non_leader_units[0])
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
    )

    # scale back up
    juju.add_unit(app_name, num_units=1)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
    )

    non_leader_units = [
        name for name, unit in juju.status().apps[app_name].units.items() if not unit.leader
    ]

    assert len(non_leader_units) == 1

    got = read_n_rows(
        hosts=[get_unit_address(juju, app_name, non_leader_units[0].split("/")[1])],
        password=password,
        ks=ks,
        table=tb,
    )
    assert_rows(wrote, got)
