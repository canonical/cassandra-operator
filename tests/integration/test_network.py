#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from time import sleep

import jubilant
from tenacity import Retrying, stop_after_delay, wait_fixed

from integration.helpers.continuous_writes import ContinuousWrites
from integration.helpers.ha import (
    get_machine_name,
    make_unit_checker,
    network_cut,
    network_release,
    network_restore,
    network_throttle,
)
from integration.helpers.juju import check_node_is_up, get_hosts, get_leader_unit

logger = logging.getLogger(__name__)

REELECTION_TIME = 300


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


def test_network_cut_without_ip_change(
    juju: jubilant.Juju, continuous_writes: ContinuousWrites, app_name: str
) -> None:
    stopped_unit_name, stopped_unit_status = get_leader_unit(juju, app_name)
    stoped_unit_host = get_hosts(juju, app_name, stopped_unit_name)[0]
    stoped_machine = get_machine_name(juju, stopped_unit_name)
    no_stoped_unit_hosts = list(set(get_hosts(juju, app_name)) - set(stoped_unit_host))

    continuous_writes.start(
        juju,
        app_name,
        hosts=no_stoped_unit_hosts,
        replication_factor=3,
    )

    network_throttle(stoped_machine)
    logger.info("Waiting for unit to go down")

    # wait for idle unit to become throttled
    juju.wait(
        ready=make_unit_checker(
            app_name,
            stopped_unit_name,
            machine_id=stopped_unit_status.machine,
            workload="unknown",
            machine="down",
        ),
        delay=20,
        timeout=1800,
    )

    sleep(REELECTION_TIME)

    # check node is down
    for attempt in Retrying(wait=wait_fixed(10), stop=stop_after_delay(180), reraise=True):
        with attempt:
            for name, unit in juju.status().apps[app_name].units.items():
                if name == stopped_unit_name:
                    continue

                logger.info(
                    f"Checking: {app_name}/{unit.machine} if node {stoped_unit_host} is down"
                )
                assert not check_node_is_up(juju, app_name, int(unit.machine), stoped_unit_host), (
                    "node is still UN in cluster"
                )

    new_leader, _ = get_leader_unit(juju, app_name)

    assert stopped_unit_name != new_leader

    continuous_writes.assert_new_writes(hosts=no_stoped_unit_hosts)

    network_release(stoped_machine)

    sleep(REELECTION_TIME)

    # wait for throttled unit to become idle
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )

    # check node is up and ip not changed
    assert check_node_is_up(juju, app_name, int(stopped_unit_name.split("/")[1]), stoped_unit_host)

    continuous_writes.stop_and_assert_writes([stoped_unit_host])


def test_network_cut(
    juju: jubilant.Juju, continuous_writes: ContinuousWrites, app_name: str
) -> None:
    stopped_unit_name, stopped_unit_status = get_leader_unit(juju, app_name)
    stoped_unit_host = get_hosts(juju, app_name, stopped_unit_name)[0]
    stoped_machine = get_machine_name(juju, stopped_unit_name)
    no_stoped_hosts = list(set(get_hosts(juju, app_name)) - set(stoped_unit_host))

    continuous_writes.start(juju, app_name, hosts=no_stoped_hosts, replication_factor=3)

    network_cut(stoped_machine)
    logger.info("Waiting for unit to go down")

    # wait for idle unit to become cut
    juju.wait(
        ready=make_unit_checker(
            app_name,
            stopped_unit_name,
            machine_id=stopped_unit_status.machine,
            workload="unknown",
            machine="down",
        ),
        delay=20,
        timeout=1800,
    )

    sleep(REELECTION_TIME)

    # check node is down
    for attempt in Retrying(wait=wait_fixed(10), stop=stop_after_delay(180), reraise=True):
        with attempt:
            for name, unit in juju.status().apps[app_name].units.items():
                if name == stopped_unit_name:
                    continue

                logger.info(
                    f"""
                    Checking: {app_name}/{unit.machine}
                    if node {stopped_unit_name}:{stoped_unit_host} is down
                    """
                )
                assert not check_node_is_up(juju, app_name, int(unit.machine), stoped_unit_host), (
                    "node is still UN in cluster"
                )

    new_leader, _ = get_leader_unit(juju, app_name)

    assert stopped_unit_name != new_leader

    continuous_writes.assert_new_writes(hosts=no_stoped_hosts)

    network_restore(stoped_machine)

    sleep(REELECTION_TIME)

    # wait for throttled unit to become idle
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )

    new_stoped_unit_host = get_hosts(juju, app_name, stopped_unit_name)[0]

    # check node is up and ip is changed
    assert new_stoped_unit_host != stoped_unit_host
    assert check_node_is_up(
        juju, app_name, int(stopped_unit_name.split("/")[1]), new_stoped_unit_host
    )

    continuous_writes.stop_and_assert_writes([new_stoped_unit_host])
