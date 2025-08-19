#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm config."""

from typing import Literal

from charms.data_platform_libs.v1.data_models import BaseConfigModel

ConfigProfile = Literal["testing", "production"]


class CharmConfig(BaseConfigModel):
    """Structured charm config."""

    system_users: str
    profile: ConfigProfile
