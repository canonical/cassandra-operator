#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path
from time import sleep

import jubilant

from integration.helpers.continuous_writes import ContinuousWrites

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
        timeout=1800,
    )


def test_lxc_restart(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    continuous_writes.start(juju, app_name)

    subprocess.check_call(["lxc", "restart", "--all"])
    sleep(10)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )

    continuous_writes.stop_and_assert_writes()
