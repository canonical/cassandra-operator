#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops import StatusBase, CollectStatusEvent

from common.literals import SUBSTRATE, CharmConfig, DebugLevel, Status
from common.workload import WorkloadBase
from core.cluster import ApplicationState
from managers.cluster import ClusterManager
from managers.config import ConfigManager

logger = logging.getLogger(__name__)


class CassandraCharmBase(TypedCharmBase[CharmConfig]):
    """Charm the application."""

    config_type = CharmConfig

    def __init__(self, workload: WorkloadBase, *args):
        super().__init__(*args)
        self.workload = workload
        self.state = ApplicationState(self, substrate=SUBSTRATE)

        self.pending_inactive_statuses: list[Status] = []

        self.cluster_manager = ClusterManager(state=self.state, workload=self.workload)
        self.config_manager = ConfigManager(
            state=self.state, workload=self.workload, config=self.config
        )

        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_status)

        
    def set_status(self, key: Status) -> None:
        """Set charm status."""
        status: StatusBase = key.value.status
        log_level: DebugLevel = key.value.log_level

        getattr(logger, log_level.lower())(status.message)
        self.pending_inactive_statuses.append(key)

        
    def _on_collect_status(self, event: CollectStatusEvent) -> None:
        if self.app.planned_units() == 0:
            event.add_status(Status.REMOVED.value.status)
            return

        # compute cluster status
        for status in self.cluster_manager.compute_component_status():
            event.add_status(status.value.status)

        # add all other statuses collected during the current hook
        for status in self.pending_inactive_statuses + [Status.ACTIVE]:
            event.add_status(status.value.status)
        
