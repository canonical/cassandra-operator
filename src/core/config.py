#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm config."""

from typing import Literal

from charms.data_platform_libs.v1.data_models import BaseConfigModel
from pydantic import field_validator

ConfigProfile = Literal["testing", "production"]


class CharmConfig(BaseConfigModel):
    """Structured charm config."""

    cluster_name: str
    profile: ConfigProfile

    @field_validator("cluster_name")
    @classmethod
    def cluster_name_values(cls, value: str) -> str:
        """Validate cluster_name."""
        if len(value) == 0:
            raise ValueError("cluster_name cannot be empty")

        return value
