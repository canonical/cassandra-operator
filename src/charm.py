#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm definition."""

import logging

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.rolling_ops.v0.rollingops import RollingOpsManager, RunWithLock
from ops import main
from tenacity import Retrying, wait_fixed

from core.config import CharmConfig
from core.state import ApplicationState, ClusterState, UnitWorkloadState
from events.cassandra import CassandraEvents
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from workload import CassandraWorkload

logger = logging.getLogger(__name__)


class CassandraCharm(TypedCharmBase[CharmConfig]):
    """Application charm."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.state = ApplicationState(self)
        self.workload = CassandraWorkload()
        self.cluster_manager = ClusterManager(workload=self.workload)

        config_manager = ConfigManager(
            workload=self.workload,
            cluster_name=self.state.cluster.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
        )
        bootstrap_manager = RollingOpsManager(
            charm=self, relation="bootstrap", callback=self.bootstrap
        )

        self.cassandra_events = CassandraEvents(
            self,
            state=self.state,
            workload=self.workload,
            cluster_manager=self.cluster_manager,
            config_manager=config_manager,
            bootstrap_manager=bootstrap_manager,
        )

    def bootstrap(self, event: RunWithLock) -> None:
        """Start workload and join this unit to the cluster."""
        # TODO: add leader elected hook
        self.state.unit.workload_state = UnitWorkloadState.STARTING

        self.workload.restart()

        for _ in Retrying(wait=wait_fixed(10)):
            if self.cluster_manager.is_healthy:
                self.state.unit.workload_state = UnitWorkloadState.ACTIVE
                if self.unit.is_leader():
                    self.state.cluster.state = ClusterState.ACTIVE
                return


if __name__ == "__main__":  # pragma: nocover
    main(CassandraCharm)
