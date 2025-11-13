#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant

from integration.helpers.cassandra import OPERATOR_PASSWORD
from integration.helpers.continuous_writes import ContinuousWrites
from integration.helpers.juju import (
    app_secret_extract,
    get_hosts,
    get_leader_unit,
    get_non_leader_units,
    scale_sequentially_to,
)

logger = logging.getLogger(__name__)


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=3,
    )
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=2400,
    )


def test_kill_primary(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    continuous_writes.start(
        hosts=get_hosts(juju, app_name),
        password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
        replication_factor=3,
    )

    leader, _ = get_leader_unit(juju, app_name)
    kill_unit(juju, leader)

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )
    continuous_writes.assert_new_writes()


def test_kill_subordinate(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    scale_sequentially_to(juju, app_name, 3)

    subordinate = get_non_leader_units(juju, app_name)[0]
    kill_unit(juju, subordinate)

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1200,
    )
    continuous_writes.stop_and_assert_writes()


def kill_unit(juju: jubilant.Juju, unit: str) -> None:
    juju.cli("remove-unit", "--force", "--no-wait", "--no-prompt", "--destroy-storage", unit)
