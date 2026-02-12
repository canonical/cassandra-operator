#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant

from integration.helpers.cassandra import (
    OPERATOR_PASSWORD,
)
from integration.helpers.charm import get_target_workload_version
from integration.helpers.continuous_writes import ContinuousWrites
from integration.helpers.ha import disable_node_gossip, enable_node_gossip
from integration.helpers.juju import (
    app_secret_extract,
    get_hosts,
    get_leader_unit,
    get_unit_names,
    get_workload_version,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "test_table_one"
CLIENT_RELATION = "cassandra-client"
KEYSPACE_NAME = "test_keyspace_one"
USER_KEYSPACE_ALL_PERMISSIONS = {"ALTER", "AUTHORIZE", "DROP", "MODIFY", "SELECT", "CREATE"}
CHARM_CHANNEL = "5/edge"
CHARM_REVISION = 73


def test_deploy(
    juju: jubilant.Juju,
    app_name: str,
) -> None:
    juju.deploy(
        app_name,
        config={"profile": "testing"},
        channel=CHARM_CHANNEL,
        revision=CHARM_REVISION,
        num_units=2,
    )

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=1800,
    )


def test_fail_upgrade_and_rollback(
    juju: jubilant.Juju, app_name: str, cassandra_charm: Path, continuous_writes: ContinuousWrites
):
    leader, _ = get_leader_unit(juju, app_name)
    unit_hosts = get_hosts(juju, app_name)

    old_workload = get_workload_version(juju, leader)

    continuous_writes.start(
        hosts=unit_hosts,
        password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
        replication_factor=2,
    )

    logger.info("Calling pre-refresh-check")
    res = juju.run(leader, "pre-refresh-check")

    assert res.status == "completed", f"pre-refresh-check failed: {res.message}"

    # Refresh always happens from highest to lowest unit number
    refresh_order = sorted(
        get_unit_names(juju, app_name),
        key=lambda unit: int(unit.split("/")[1]),
        reverse=True,
    )

    logger.info("Upgrading Cassandra...")
    juju.refresh(app_name, path=cassandra_charm)

    logger.info(f"Pause gossip on unit {refresh_order[-1]} to force upgrade to fail")
    disable_node_gossip(juju, refresh_order[-1])

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status),
        delay=10,
        timeout=1800,
    )

    # versions will always be marked "incompatible" if refresh to a local version
    # this will not be the case when the PR is released
    # see: https://github.com/canonical/charm-refresh/blob/main/charm_refresh/_main.py#L182-L185
    if "incompatible" in juju.status().apps[app_name].app_status.message:
        logger.info("Upgrade is blocked due to incompatibility")
        logger.info(f"Continue refresh on unit {refresh_order[0]}")
        logger.info("Running `force-refresh-start` action with check-compatibility=false")

        res = juju.run(
            refresh_order[0],
            "force-refresh-start",
            {
                "check-compatibility": False,
                "run-pre-refresh-checks": False,
            },
        )

        assert res.status == "completed", f"force-refresh-start failed: {res.message}"

    continuous_writes.assert_new_writes(unit_hosts)

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status),
        delay=10,
        timeout=1800,
    )

    s = juju.wait(jubilant.any_blocked)
    assert "resume-refresh" in s.apps[app_name].app_status.message, (
        "Refresh should wait for user to continue with `resume-refresh` action"
    )

    logger.info(f"Upgrade failed - roll back to previous version v{old_workload}")
    logger.info(f"Continue Cassandra service on unit {refresh_order[-1]}")
    enable_node_gossip(juju, refresh_order[-1])

    # ops_test.application.refresh can't refresh from local to published charm, use command line
    # in `juju refresh`, --switch and --revision are mutually exclusive
    # we can only roll back to the latest released revision from a local charm
    juju.cli("refresh", app_name, "--channel", CHARM_CHANNEL, "--switch", app_name)

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status),
        delay=30,
        timeout=1800,
        successes=4,
    )

    app_status_message = juju.status().apps[app_name].app_status.message

    if "incompatible" in app_status_message:
        # will be marked "incompatible" if rollback is not to the same rev as initially deployed
        logger.info("Rollback is blocked due to incompatibility")

        logger.info("Running `force-refresh-start` action with check-compatibility=false")
        res = juju.run(refresh_order[0], "force-refresh-start", {"check-compatibility": False})
        assert res.status == "completed", f"force-refresh-start failed: {res.message}"
    elif "Refreshing" in app_status_message:
        logger.info("Rolling back to previous revision")

        juju.refresh(app_name, channel=CHARM_CHANNEL, revision=CHARM_REVISION)

    continuous_writes.assert_new_writes(unit_hosts)

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=1800,
    )

    for unit in refresh_order:
        assert get_workload_version(juju, unit) == old_workload, (
            f"unit {unit} failed to rollback to previous version"
        )

    continuous_writes.stop_and_assert_writes(unit_hosts)


def test_upgrade_to_local(
    juju: jubilant.Juju, app_name: str, cassandra_charm: Path, continuous_writes: ContinuousWrites
):
    leader, _ = get_leader_unit(juju, app_name)
    unit_hosts = get_hosts(juju, app_name)

    continuous_writes.start(
        hosts=unit_hosts,
        password=app_secret_extract(juju, app_name, OPERATOR_PASSWORD),
        replication_factor=2,
    )

    logger.info("Calling pre-refresh-check")
    res = juju.run(leader, "pre-refresh-check")

    assert res.status == "completed", f"pre-refresh-check failed: {res.message}"

    # Refresh always happens from highest to lowest unit number
    refresh_order = sorted(
        get_unit_names(juju, app_name),
        key=lambda unit: int(unit.split("/")[1]),
        reverse=True,
    )

    logger.info("Upgrading Cassandra...")
    juju.refresh(app_name, path=cassandra_charm)

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status),
        delay=30,
        timeout=1800,
        successes=4,
    )

    # versions will always be marked "incompatible" if refresh to a local version
    # this will not be the case when the PR is released
    # see: https://github.com/canonical/charm-refresh/blob/main/charm_refresh/_main.py#L182-L185
    if "incompatible" in juju.status().apps[app_name].app_status.message:
        logger.info("Upgrade is blocked due to incompatibility")
        logger.info(f"Continue refresh on unit {refresh_order[0]}")
        logger.info("Running `force-refresh-start` action with check-compatibility=false")

        res = juju.run(refresh_order[0], "force-refresh-start", {"check-compatibility": False})
        assert res.status == "completed", f"force-refresh-start failed: {res.message}"

    continuous_writes.assert_new_writes(unit_hosts)

    s = juju.wait(jubilant.any_blocked)
    assert "resume-refresh" in s.apps[app_name].app_status.message, (
        "Refresh should wait for user to continue with `resume-refresh` action"
    )

    logger.info("Continue refresh on all other units with `resume-refresh` action")
    res = juju.run(refresh_order[1], "resume-refresh")
    assert res.status == "completed", (
        f"resume-refresh on unit {refresh_order[1]} failed: {res.message}"
    )

    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=10,
        timeout=1800,
    )

    for unit in refresh_order:
        assert get_workload_version(juju, unit) == get_target_workload_version(), (
            f"unit {unit} was not upgraded"
        )

    continuous_writes.stop_and_assert_writes(unit_hosts)
