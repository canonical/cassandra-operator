#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./charmcraft.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path) -> None:
    juju.deploy(
        cassandra_charm,
        app=APP_NAME,
        config={"profile": "testing"},
    )
    juju.wait(jubilant.all_active)
