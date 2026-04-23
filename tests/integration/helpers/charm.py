#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

import toml


def get_target_workload_version() -> str:
    refresh_versions = toml.loads(Path("./refresh_versions.toml").read_text())
    return refresh_versions["workload"]
