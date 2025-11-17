#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant

from integration.helpers.juju import get_leader_unit
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
 )

logger = logging.getLogger(__name__)
TEST_ROW_NUM = 100

def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=3,
    )
    juju.wait(jubilant.all_active, timeout=1800)

def test_in_place_refresh(juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites, cassandra_charm: Path) -> None:
    logger.info("Producing writes before refresh")
    continuous_writes.start(
        hosts=get_hosts(juju, app_name),
        password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
    )
    
    leader, _ = get_leader_unit(juju, app_name)

    continuous_writes.assert_new_writes()

    logger.info("Calling pre-refresh-check")
    res = juju.run(leader, "pre-refresh-check")
    assert res.success

    logger.info("Refreshing Cassandra")
    juju.refresh(app_name, path=cassandra_charm)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=3,
        successes=5,
        timeout=1800,
    )

    continuous_writes.assert_new_writes(hosts=[get_unit_address(juju, app_name, 1)])

    continuous_writes.stop_and_assert_writes(hosts=[get_unit_address(juju, app_name, 2)])

