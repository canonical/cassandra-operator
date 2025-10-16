#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from cassandra.cluster import ResultSet
from helpers import (
    assert_rows,
    connect_cql,
    get_leader_unit,
    get_non_leader_units,
    prepare_keyspace_and_table,
    read_n_rows,
    write_n_rows,
)

logger = logging.getLogger(__name__)


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
    )
    juju.wait(jubilant.all_active)


def test_write(juju: jubilant.Juju, app_name: str) -> None:
    leader = get_leader_unit(juju, app_name)
    host = juju.status().apps[app_name].units[leader].public_address
    with connect_cql(juju=juju, app_name=app_name, hosts=[host]) as session:
        session.execute(
            "CREATE KEYSPACE test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace("test")
        session.execute("CREATE TABLE test(message TEXT PRIMARY KEY)")
        session.execute("INSERT INTO test(message) VALUES ('hello')")


def test_read(juju: jubilant.Juju, app_name: str) -> None:
    leader = get_leader_unit(juju, app_name)
    host = juju.status().apps[app_name].units[leader].public_address
    with connect_cql(juju=juju, app_name=app_name, hosts=[host], keyspace="test") as session:
        res = session.execute("SELECT message FROM test")
        assert (
            isinstance(res, ResultSet)
            and len(res_list := res.all()) == 1
            and res_list[0].message == "hello"
        ), "test data written prior aren't found"


def test_write_primary_read_secondary(juju: jubilant.Juju, app_name: str) -> None:
    juju.add_unit(app_name, num_units=2)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
    )

    leader = get_leader_unit(juju, app_name)
    secodaries = get_non_leader_units(juju, app_name)

    ks, tbl = prepare_keyspace_and_table(juju, app_name, unit_name=leader)
    wrote_leader_rows = write_n_rows(juju, app_name, ks=ks, table=tbl, unit_name=leader)
    got_leader_rows = read_n_rows(juju, app_name, ks=ks, table=tbl, unit_name=leader)

    assert_rows(wrote_leader_rows, got_leader_rows)

    for secondary in secodaries:
        got_secondary_rows = read_n_rows(juju, app_name, ks=ks, table=tbl, unit_name=secondary)
        assert_rows(wrote_leader_rows, got_secondary_rows)
