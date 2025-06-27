#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from ops import CollectStatusEvent, main

from core.config import CharmConfig
from core.state import ApplicationState
from core.statuses import Status
from events.cassandra import CassandraEvents
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from workload import CassandraWorkload

logger = logging.getLogger(__name__)


class CassandraCharm(TypedCharmBase[CharmConfig]):
    """Charm the application."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        state = ApplicationState(self)
        workload = CassandraWorkload()
        cluster_manager = ClusterManager(workload=workload)
        config_manager = ConfigManager(workload=workload)

        self.framework.observe(self.on.collect_unit_status, self._on_collect_unit_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_app_status)

        self.cassandra_events = CassandraEvents(
            self,
            state=state,
            workload=workload,
            cluster_manager=cluster_manager,
            config_manager=config_manager,
        )

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        event.add_status(Status.ACTIVE.value)

    def _on_collect_app_status(self, event: CollectStatusEvent) -> None:
        event.add_status(Status.ACTIVE.value)


if __name__ == "__main__":  # pragma: nocover
    main(CassandraCharm)
