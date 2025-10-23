#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import uuid
from pathlib import Path
from dataclasses import dataclass
import hashlib
import string

import jubilant
from helpers import (
    assert_rows,
    get_leader_unit,
    prepare_keyspace_and_table,
    read_n_rows,
    write_n_rows,
)

logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    app_name: str
    config: dict

def letter_hash(num: int, length: int = 8) -> str:
    h = hashlib.sha256(str(num).encode()).digest()
    alphabet = string.ascii_lowercase
    return ''.join(alphabet[b % len(alphabet)] for b in h[:length])    

def cassandra_federation_config(cluster_num: int, app_name: str) -> list[ClusterConfig]:
    """Generate a list of independent cluster configs for federation testing."""
    return [
        ClusterConfig(app_name=f"{app_name}-{letter_hash(i)}", config={"profile": "testing"})
        for i in range(cluster_num)
    ]


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    clusters = cassandra_federation_config(cluster_num=2, app_name=app_name)

    for cluster in clusters:
        logger.info(f"Deploying cluster {cluster.app_name}")
        juju.deploy(
            cassandra_charm,
            app=cluster.app_name,
            config=cluster.config,
            num_units=1,
        )

    logger.info("Waiting for all clusters to become active...")
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
        timeout=1200,
    )
    logger.info("All clusters are active.")


def test_writes_not_replicated(juju: jubilant.Juju, app_name: str) -> None:
    """Verify that data written in one Cassandra cluster is not replicated to another."""
    fed = cassandra_federation_config(cluster_num=2, app_name=app_name)
    cluster_rows = [128, 256]
    ks = f"test_ks_{uuid.uuid4().hex[:6]}"
    tbl = f"test_tbl_{uuid.uuid4().hex[:6]}"
    cluster_wrote_rows: list[dict[int, str]] = []

    # Writing unique data into each cluster
    for i, cluster in enumerate(fed):
        leader = get_leader_unit(juju, cluster.app_name)
        logger.info(f"Preparing keyspace/table on {cluster.app_name} leader {leader}")
        prepare_keyspace_and_table(juju, cluster.app_name, unit_name=leader, ks=ks, table=tbl)

        logger.info(f"Writing {cluster_rows[i]} rows to {cluster.app_name}")
        rows = write_n_rows(
            juju,
            cluster.app_name,
            ks=ks,
            table=tbl,
            unit_name=leader,
            n=cluster_rows[i],
        )
        cluster_wrote_rows.append(rows)

    # Verifying each cluster can read its own data
    for i, cluster in enumerate(fed):
        leader = get_leader_unit(juju, cluster.app_name)
        got_rows = read_n_rows(
            juju,
            cluster.app_name,
            ks=ks,
            table=tbl,
            unit_name=leader,
            n=cluster_rows[i],
        )
        logger.info(f"Cluster {cluster.app_name} read {len(got_rows)} rows.")
        assert_rows(cluster_wrote_rows[i], got_rows)

    # Ensuring no cross-cluster replication
    leader_0 = get_leader_unit(juju, fed[0].app_name)
    got_0 = read_n_rows(
        juju,
        fed[0].app_name,
        ks=ks,
        table=tbl,
        unit_name=leader_0,
        n=cluster_rows[1],
    )
    assert got_0 == {}, f"Unexpected rows from cluster 1 visible in cluster 0: {got_0}"

    leader_1 = get_leader_unit(juju, fed[1].app_name)
    got_1 = read_n_rows(
        juju,
        fed[1].app_name,
        ks=ks,
        table=tbl,
        unit_name=leader_1,
        n=cluster_rows[0],
    )
    assert got_1 == {}, f"Unexpected rows from cluster 0 visible in cluster 1: {got_1}"
