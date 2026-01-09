#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant

from integration.helpers.cassandra import (
    OPERATOR_PASSWORD,
)
from integration.helpers.continuous_writes import ContinuousWrites
from integration.helpers.juju import app_secret_extract, get_hosts

logger = logging.getLogger(__name__)


def test_deploy_bad_config(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "bad_value"},
    )
    juju.wait(jubilant.all_blocked)


def test_deploy_resolve_config(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    juju.config(app_name, values={"profile": "testing"})
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        timeout=300,
    )

    continuous_writes.start(
        get_hosts(juju, app_name), app_secret_extract(juju, app_name, OPERATOR_PASSWORD)
    )


def test_bad_config(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    juju.config(app_name, values={"profile": "bad_value"})
    juju.wait(jubilant.all_blocked)

    continuous_writes.assert_new_writes()


def test_resolve_config(
    juju: jubilant.Juju, app_name: str, continuous_writes: ContinuousWrites
) -> None:
    juju.config(app_name, values={"profile": "testing"})
    juju.wait(jubilant.all_active)

    continuous_writes.stop_and_assert_writes()
