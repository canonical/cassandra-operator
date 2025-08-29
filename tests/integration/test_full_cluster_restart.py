#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path

import jubilant
from helpers import connect_cql

logger = logging.getLogger(__name__)


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=3,
    )
    juju.wait(jubilant.all_active)


def test_lxc_restart(juju: jubilant.Juju) -> None:
    subprocess.check_call(["lxc", "restart", "--all"])
    juju.wait(jubilant.all_active)


def test_write(juju: jubilant.Juju, app_name: str) -> None:
    host = juju.status().apps[app_name].units[f"{app_name}/0"].public_address
    with connect_cql(juju=juju, app_name=app_name, hosts=[host], timeout=300) as session:
        session.execute(
            "CREATE KEYSPACE test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
        )
        session.set_keyspace("test")
        session.execute("CREATE TABLE test(message TEXT PRIMARY KEY)")
        session.execute("INSERT INTO test(message) VALUES ('hello')")
