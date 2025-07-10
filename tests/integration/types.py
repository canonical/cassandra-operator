#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
from pydantic import BaseModel

class TestCharm(BaseModel):
    """An abstraction of metadata of a charm to be deployed.

    Attrs:
        name: str, representing the charm to be deployed
        channel: str, representing the channel to be used
        series: str, representing the series of the system for the container where the charm
            is deployed to
        num_units: int, number of units for the deployment
        alias: str (Optional), alias to be used for the charm
    """

    name: str
    channel: str
    base: str
    revision: int
    num_units: int = 1
    alias: str | None = None
    trust: bool | None = False

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    @property
    def app(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "charm": self.name,
            "channel": self.channel,
            "base": self.base,
            "revision": self.revision,
            "num_units": self.num_units,
            "app": self.application_name,
            "trust": self.trust,
        }


class IntegrationTestsCharms(BaseModel):
    tls: TestCharm
