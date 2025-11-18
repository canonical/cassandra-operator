#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant

logger = logging.getLogger(__name__)

def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=3,
    )
    juju.wait(jubilant.all_active, timeout=1800)
