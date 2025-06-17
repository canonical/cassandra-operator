#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm config definition."""

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import field_validator


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    profile: str
    cluster_name: str

    @field_validator("profile")
    @classmethod
    def profile_values(cls, value: str) -> str:
        """Check profile config option is one of `testing` or `production`."""
        if value not in ["testing", "production"]:
            raise ValueError("profile should be 'testing' or 'production'")

        return value

    @field_validator("cluster_name")
    @classmethod
    def cluster_name_values(cls, value: str) -> str:
        """TODO."""
        if len(value) == 0:
            raise ValueError("cluster_name cannot be empty")

        return value
