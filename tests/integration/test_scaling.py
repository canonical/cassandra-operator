#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from helpers import assert_rows, prepare_keyspace_and_table, read_n_rows, write_n_rows

logger = logging.getLogger(__name__)
TEST_ROW_NUM = 100


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=1,
    )
    juju.wait(jubilant.all_active)


def test_scale_up(juju: jubilant.Juju, app_name: str) -> None:
    ks, tb = prepare_keyspace_and_table(juju, app_name=app_name, ks="upks", table="uptbl")

    wrote = write_n_rows(juju, app_name, ks=ks, table=tb)
    got1 = read_n_rows(juju, app_name, ks=ks, table=tb)
    assert_rows(wrote, got1)

    juju.add_unit(app_name, num_units=2)
    juju.wait(jubilant.all_active)

    added_units = [u[0] for u in juju.status().apps[app_name].units.items() if not u[1].leader]

    got2 = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=added_units[0])
    assert_rows(wrote, got2)


def test_read_write_multinode(juju: jubilant.Juju, app_name: str) -> None:
    ks, tb = prepare_keyspace_and_table(juju, app_name=app_name, ks="multiks", table="multitbl")

    added_units = [u[0] for u in juju.status().apps[app_name].units.items() if not u[1].leader]

    assert len(added_units) > 1

    wrote = write_n_rows(juju, app_name, ks=ks, table=tb, unit_name=added_units[0])
    got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=added_units[1])
    assert_rows(wrote, got)


def test_single_node_scale_down(juju: jubilant.Juju, app_name: str) -> None:
    non_leader_units = [
        name for name, unit in juju.status().apps[app_name].units.items() if not unit.leader
    ]

    leader_unit = [
        name for name, unit in juju.status().apps[app_name].units.items() if unit.leader
    ][0]

    assert len(non_leader_units) != 0 and len(non_leader_units) > 1

    ks, tb = prepare_keyspace_and_table(juju, app_name=app_name, ks="downks", table="downtbl")
    wrote = write_n_rows(juju, app_name, ks=ks, table=tb, unit_name=non_leader_units[0])

    juju.remove_unit(non_leader_units[0])
    juju.wait(jubilant.all_active)

    got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=non_leader_units[1])
    assert_rows(wrote, got)

    juju.remove_unit(leader_unit)
    juju.wait(jubilant.all_active)

    new_leader_unit = [
        name for name, unit in juju.status().apps[app_name].units.items() if unit.leader
    ][0]

    assert new_leader_unit

    got = read_n_rows(juju, app_name, ks=ks, table=tb, unit_name=new_leader_unit)
    assert_rows(wrote, got)
