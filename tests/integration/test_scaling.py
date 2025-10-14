#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from itertools import pairwise
from pathlib import Path

import jubilant
from helpers.cassandra import assert_rows, prepare_keyspace_and_table, read_n_rows, write_n_rows
from helpers.juju import scale_sequentially_to

logger = logging.getLogger(__name__)
TEST_ROW_NUM = 100


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
    )
    juju.wait(jubilant.all_active, timeout=1200)


def test_scale_up(juju: jubilant.Juju, app_name: str) -> None:
    ks, tb = prepare_keyspace_and_table(juju, app_name=app_name, ks="upks", table="uptbl")

    wrote = write_n_rows(juju, app_name, ks=ks, table=tb)
    got1 = read_n_rows(juju, app_name, ks=ks, table=tb)
    assert_rows(wrote, got1)

    old_units = set(juju.status().apps[app_name].units.keys())

    scale_sequentially_to(juju, app_name, 3)

    all_units = set(juju.status().apps[app_name].units.keys())
    new_units = list(all_units - old_units)
    assert new_units, "No new units detected after scale-up"

    got2 = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=new_units[0])
    assert_rows(wrote, got2)


def test_read_write_multinode(juju: jubilant.Juju, app_name: str) -> None:
    units = juju.status().apps[app_name].units.items()
    for unit_pair in pairwise(units):
        unit1_name: str = unit_pair[0][0]
        unit2_name: str = unit_pair[1][0]

        ks, tb = prepare_keyspace_and_table(
            juju,
            app_name=app_name,
            ks="multiks",
            table=f"""multitbl_{unit1_name.replace("/", "_")}_{unit2_name.replace("/", "_")}""",
        )

        assert len(unit_pair) > 1

        wrote = write_n_rows(juju, app_name, ks=ks, table=tb, unit_name=unit1_name)
        got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=unit2_name)
        assert_rows(wrote, got)


def test_single_node_scale_down(juju: jubilant.Juju, app_name: str) -> None:
    scale_sequentially_to(juju, app_name, 3)

    non_leader_units = [
        name for name, unit in juju.status().apps[app_name].units.items() if not unit.leader
    ]

    leader_unit = [
        name for name, unit in juju.status().apps[app_name].units.items() if unit.leader
    ][0]

    assert len(non_leader_units) == 2

    ks, tb = prepare_keyspace_and_table(juju, app_name=app_name, ks="downks", table="downtbl")
    wrote = write_n_rows(juju, app_name, ks=ks, table=tb, unit_name=non_leader_units[0])

    juju.remove_unit(non_leader_units[0])
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
        timeout=1200,
    )

    got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=non_leader_units[1])
    assert_rows(wrote, got)

    juju.remove_unit(leader_unit)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
        timeout=1200,
    )

    new_leader_unit = [
        name for name, unit in juju.status().apps[app_name].units.items() if unit.leader
    ][0]

    assert new_leader_unit

    got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=new_leader_unit)
    assert_rows(wrote, got)


def test_single_node_scale_down_scale_up(juju: jubilant.Juju, app_name: str) -> None:
    scale_sequentially_to(juju, app_name, 2)

    non_leader_units = [
        name for name, unit in juju.status().apps[app_name].units.items() if not unit.leader
    ]

    assert len(non_leader_units) == 1

    ks, tb = prepare_keyspace_and_table(juju, app_name=app_name, ks="downupks", table="downuptbl")
    wrote = write_n_rows(juju, app_name, ks=ks, table=tb, unit_name=non_leader_units[0])

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

    got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=non_leader_units[0])
    assert_rows(wrote, got)
