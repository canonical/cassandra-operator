#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm config definition."""

from typing import Literal

from charms.data_platform_libs.v1.data_models import BaseConfigModel
from pydantic import field_validator

ConfigProfile = Literal["testing", "production"]


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    profile: ConfigProfile
    cluster_name: str

    @field_validator("cluster_name")
    @classmethod
    def cluster_name_values(cls, value: str) -> str:
        """TODO."""
        if len(value) == 0:
            raise ValueError("cluster_name cannot be empty")

        return value
