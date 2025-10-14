#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from cassandra.cluster import ResultSet
from helpers.cassandra import connect_cql

logger = logging.getLogger(__name__)


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
    )
    juju.wait(jubilant.all_active)


def test_write(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(juju=juju, app_name=app_name, hosts=[host]) as session:
        session.execute(
            "CREATE KEYSPACE test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace("test")
        session.execute("CREATE TABLE test(message TEXT PRIMARY KEY)")
        session.execute("INSERT INTO test(message) VALUES ('hello')")


def test_read(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(juju=juju, app_name=app_name, hosts=[host], keyspace="test") as session:
        res = session.execute("SELECT message FROM test")
        assert (
            isinstance(res, ResultSet)
            and len(res_list := res.all()) == 1
            and res_list[0].message == "hello"
        ), "test data written prior aren't found"
