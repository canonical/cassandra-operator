#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path
from time import sleep
from typing import Literal

import jubilant
from jubilant.statustypes import UnitStatus

from integration.helpers.continuous_writes import ContinuousWrites
from integration.helpers.juju import get_unit_address

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


def test_graceful_restart_unit(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    # TODO: revert safe_unit workaround after proper scaling of system_auth replication factor
    unit_name, unit_status = safe_unit(juju, app_name)

    continuous_writes.start(juju, app_name, replication_factor=3)

    juju.ssh(
        unit_name,
        "sudo charmed-cassandra.nodetool drain && sudo snap restart charmed-cassandra.daemon",
    )

    sleep(60)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=600,
    )

    continuous_writes.stop_and_assert_writes([unit_status.public_address])


def test_kill_process(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    unit_name, unit_status = safe_unit(juju, app_name)

    continuous_writes.start(juju, app_name, replication_factor=3)

    send_control_signal(juju, unit_name, "SIGKILL")

    sleep(60)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=600,
    )

    continuous_writes.stop_and_assert_writes([unit_status.public_address])


def test_freeze_process(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    unit_name, unit_status = safe_unit(juju, app_name)
    _, other_unit_status = other_unit(juju, app_name, unit_name)

    continuous_writes.start(
        juju, app_name, [other_unit_status.public_address], replication_factor=3
    )

    continuous_writes.assert_new_writes([unit_status.public_address])

    send_control_signal(juju, unit_name, "SIGSTOP")

    sleep(10)

    continuous_writes.assert_new_writes([other_unit_status.public_address])

    send_control_signal(juju, unit_name, "SIGCONT")

    sleep(10)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=600,
    )

    continuous_writes.stop_and_assert_writes([unit_status.public_address])


def test_graceful_restart_cluster(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    for i in range(0, 2):
        juju.ssh(
            f"{app_name}/{i}",
            "sudo charmed-cassandra.nodetool drain && sudo snap restart charmed-cassandra.daemon",
        )

    sleep(60)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )

    continuous_writes.start(juju, app_name, [get_unit_address(juju, app_name, 0)])
    continuous_writes.assert_new_writes([get_unit_address(juju, app_name, 1)])
    continuous_writes.stop_and_assert_writes([get_unit_address(juju, app_name, 2)])


def test_lxc_restart_cluster(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    continuous_writes.start(juju, app_name, replication_factor=3)

    subprocess.check_call(["lxc", "restart", "--all"])

    sleep(60)
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )

    continuous_writes.assert_new_writes([get_unit_address(juju, app_name, 0)])
    continuous_writes.assert_new_writes([get_unit_address(juju, app_name, 1)])
    continuous_writes.stop_and_assert_writes([get_unit_address(juju, app_name, 2)])


def send_control_signal(
    juju: jubilant.Juju,
    unit_name: str,
    signal: Literal["SIGKILL", "SIGSTOP", "SIGCONT"],
) -> None:
    subprocess.check_call(
        [
            "lxc",
            "exec",
            juju.ssh(unit_name, "hostname").strip(),
            "--",
            "pkill",
            "--signal",
            signal,
            "-f",
            "javaagent:/snap/charmed-cassandra/",
        ]
    )


def safe_unit(juju: jubilant.Juju, app_name: str) -> tuple[str, UnitStatus]:
    for name, status in juju.status().apps[app_name].units.items():
        if status.leader:
            continue
        return name, status
    raise RuntimeError("cannot find safe unit to shutdown")


def other_unit(juju: jubilant.Juju, app_name: str, avoid_unit: str) -> tuple[str, UnitStatus]:
    for name, status in juju.status().apps[app_name].units.items():
        if name == avoid_unit:
            continue
        return name, status
    raise RuntimeError(f"cannot find unit other than {avoid_unit}")
