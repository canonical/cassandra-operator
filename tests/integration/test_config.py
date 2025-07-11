#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from cassandra.cluster import ResultSet
from helpers import connect_cql

logger = logging.getLogger(__name__)


def test_deploy_bad_config(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "bad_value"},
        num_units=1,
    )
    juju.wait(jubilant.all_blocked)


def test_deploy_resolve_config(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, values={"profile": "testing"})
    juju.wait(jubilant.all_active)


def test_write(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(hosts=[host], timeout=300) as session:
        session.execute(
            "CREATE KEYSPACE test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        )
        session.set_keyspace("test")
        session.execute("CREATE TABLE test(message TEXT PRIMARY KEY)")
        session.execute("INSERT INTO test(message) VALUES ('hello')")


def test_read(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(hosts=[host], keyspace="test") as session:
        res = session.execute("SELECT message FROM test")
        assert (
            isinstance(res, ResultSet)
            and len(res_list := res.all()) == 1
            and res_list[0].message == "hello"
        ), "test data written prior aren't found"


def test_bad_config(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, values={"profile": "bad_value"})
    juju.wait(jubilant.all_blocked)


def test_read_bad_config(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(hosts=[host], keyspace="test") as session:
        res = session.execute("SELECT message FROM test")
        assert (
            isinstance(res, ResultSet)
            and len(res_list := res.all()) == 1
            and res_list[0].message == "hello"
        ), "test data written prior aren't found"


def test_resolve_config(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, values={"profile": "testing"})
    juju.wait(jubilant.all_active)


def test_read_after_resolve(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(hosts=[host], keyspace="test") as session:
        res = session.execute("SELECT message FROM test")
        assert (
            isinstance(res, ResultSet)
            and len(res_list := res.all()) == 1
            and res_list[0].message == "hello"
        ), "test data written prior aren't found"
