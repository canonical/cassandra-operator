#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging

from ops import (
    CollectStatusEvent,
    ConfigChangedEvent,
    InstallEvent,
    Object,
    StartEvent,
    UpdateStatusEvent,
)
from pydantic import ValidationError

from core.charm import CassandraCharmBase
from core.state import ClusterState, UnitWorkloadState
from core.statuses import Status

logger = logging.getLogger(__name__)


class CassandraEvents(Object):
    """Handle all base and cassandra related events."""

    def __init__(self, charm: CassandraCharmBase):
        super().__init__(charm, key="cassandra_events")
        self.charm = charm

        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.collect_unit_status, self._on_collect_unit_status)

    def _on_install(self, _: InstallEvent) -> None:
        self.charm.workload.install()
        self.charm.state.unit.workload_state = UnitWorkloadState.STARTING.value

    def _on_start(self, event: StartEvent) -> None:
        self.charm.cluster_manager.update_network_address()
        try:
            self.charm.config_manager.render_cassandra_env_config(
                1024 if self.charm.config.profile == "testing" else None
            )
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            event.defer()
            return

        if (
            not self.charm.unit.is_leader()
            and self.charm.state.cluster.state != ClusterState.ACTIVE.value
        ):
            logger.debug("Deferring on_start for unit due to cluster isn't initialized yet")
            event.defer()
            return

        self.charm.cluster_manager.start_node()
        self.charm.state.unit.workload_state = UnitWorkloadState.ACTIVE.value
        if self.charm.unit.is_leader():
            self.charm.state.cluster.state = ClusterState.ACTIVE.value

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        try:
            self.charm.config_manager.render_cassandra_env_config(
                1024 if self.charm.config.profile == "testing" else None
            )
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            return
        if not self.charm.state.unit.workload_state == UnitWorkloadState.ACTIVE.value:
            return
        self.charm.cluster_manager.restart_node()

    def _on_update_status(self, event: UpdateStatusEvent) -> None:
        self.charm.cluster_manager.update_network_address()

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        try:
            self.charm.config
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)

        if self.charm.state.unit.workload_state == "":
            event.add_status(Status.INSTALLING.value)

        if self.charm.state.unit.workload_state == UnitWorkloadState.STARTING.value:
            event.add_status(Status.STARTING.value)

        if (
            self.charm.state.unit.workload_state == UnitWorkloadState.ACTIVE.value
            and not self.charm.cluster_manager.is_healthy
        ):
            event.add_status(Status.STARTING.value)
