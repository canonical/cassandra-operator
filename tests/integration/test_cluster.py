#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from dataclasses import dataclass

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

@dataclass
class ClusterConfig:
    app_name: str
    config: dict

def cassandra_federation_config(cluster_num: int, app_name: str) -> list[ClusterConfig]:
    return [
        ClusterConfig(
            app_name=app_name + "-" +str(i),
            config={"profile": "testing"}
        ) for i in range(cluster_num)
    ]

def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    clusters = cassandra_federation_config(
        cluster_num=2,
        app_name=app_name,
    )

    for cluster in clusters:
        juju.deploy(
            cassandra_charm,
            app=cluster.app_name,
            config=cluster.config,
        )
    juju.wait(jubilant.all_active)
