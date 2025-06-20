#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops import CollectStatusEvent

from common.statuses import Status
from common.workload import WorkloadBase
from core.config import CharmConfig
from core.state import ApplicationState
from managers.cluster import ClusterManager
from managers.config import ConfigManager

logger = logging.getLogger(__name__)


class CassandraCharmBase(TypedCharmBase[CharmConfig]):
    """Charm the application."""

    config_type = CharmConfig

    def __init__(self, workload: WorkloadBase, *args):
        super().__init__(*args)
        self.workload = workload
        self.state = ApplicationState(self)

        self.cluster_manager = ClusterManager(state=self.state, workload=self.workload)
        self.config_manager = ConfigManager(workload=self.workload)

        self.framework.observe(self.on.collect_unit_status, self._on_collect_unit_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_app_status)

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        event.add_status(Status.ACTIVE.value)

    def _on_collect_app_status(self, event: CollectStatusEvent) -> None:
        event.add_status(Status.ACTIVE.value)
