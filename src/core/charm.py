#!/usr/bin/env python3
"""TODO."""

import logging

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops import StatusBase

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

    def set_status(self, key: Status) -> None:
        """Set charm status."""
        status: StatusBase = key.value.status
        log_level: DebugLevel = key.value.log_level

        getattr(logger, log_level.lower())(status.message)
        self.pending_inactive_statuses.append(key)
