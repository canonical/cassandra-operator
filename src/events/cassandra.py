#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from ops import (
    CollectStatusEvent,
    ConfigChangedEvent,
    InstallEvent,
    Object,
    StartEvent,
    UpdateStatusEvent,
)
from pydantic import ValidationError

from core.config import CharmConfig
from core.state import ApplicationState, ClusterState, UnitWorkloadState
from core.statuses import Status
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager

logger = logging.getLogger(__name__)


class CassandraEvents(Object):
    """Handle all base and cassandra related events."""

    def __init__(
        self,
        charm: TypedCharmBase[CharmConfig],
        state: ApplicationState,
        workload: WorkloadBase,
        cluster_manager: ClusterManager,
        config_manager: ConfigManager,
    ):
        super().__init__(charm, key="cassandra_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.cluster_manager = cluster_manager
        self.config_manager = config_manager

        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.collect_unit_status, self._on_collect_unit_status)

    def _on_install(self, _: InstallEvent) -> None:
        self.workload.install()
        self.state.unit.workload_state = UnitWorkloadState.STARTING.value

    def _on_start(self, event: StartEvent) -> None:
        self._update_network_address()
        try:
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None
            )
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            event.defer()
            return

        if (
            not self.charm.unit.is_leader()
            and self.state.cluster.state != ClusterState.ACTIVE.value
        ):
            logger.debug("Deferring on_start for unit due to cluster isn't initialized yet")
            event.defer()
            return

        self.workload.restart()
        self.state.unit.workload_state = UnitWorkloadState.ACTIVE.value
        if self.charm.unit.is_leader():
            self.state.cluster.state = ClusterState.ACTIVE.value

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        try:
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None
            )
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            return
        if not self.state.unit.workload_state == UnitWorkloadState.ACTIVE.value:
            return
        self.workload.restart()

    def _on_update_status(self, event: UpdateStatusEvent) -> None:
        self._update_network_address()

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        try:
            self.charm.config
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)

        if self.state.unit.workload_state == "":
            event.add_status(Status.INSTALLING.value)

        if self.state.unit.workload_state == UnitWorkloadState.STARTING.value:
            event.add_status(Status.STARTING.value)

        if (
            self.state.unit.workload_state == UnitWorkloadState.ACTIVE.value
            and not self.cluster_manager.is_healthy
        ):
            event.add_status(Status.STARTING.value)

    def _update_network_address(self) -> bool:
        """TODO."""
        old_ip = self.state.unit.ip
        old_hostname = self.state.unit.hostname
        self.state.unit.ip, self.state.unit.hostname = self.cluster_manager.network_address()
        return (
            old_ip is not None
            and old_hostname is not None
            and (old_ip != self.state.unit.ip or old_hostname != self.state.unit.hostname)
        )
