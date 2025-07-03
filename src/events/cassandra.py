#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for main Cassandra charm events."""

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

from common.cassandra_client import CassandraClient
from core.config import CharmConfig
from core.state import ApplicationState, ClusterState, UnitWorkloadState
from core.statuses import Status
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager

logger = logging.getLogger(__name__)


class CassandraEvents(Object):
    """Handler for main Cassandra charm events."""

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
        self.framework.observe(self.charm.on.collect_app_status, self._on_collect_app_status)

    def _on_install(self, _: InstallEvent) -> None:
        self.workload.install()

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

        if not self.charm.unit.is_leader():
            self._start_subordinate(event)
            return

        self.state.cluster.seeds = [self.state.unit.peer_url]

        if self.state.unit.workload_state == UnitWorkloadState.CHANGING_PASSWORD:
            self._finalize_password_change(event)
            return

        if not self.state.unit.workload_state:
            if self.state.cluster.cassandra_password_secret:
                self._start_leader()
            else:
                self._start_password_change(event)

    def _start_subordinate(self, event: StartEvent) -> None:
        if not self.state.cluster.is_active:
            self.state.unit.workload_state = UnitWorkloadState.WAITING_FOR_START
            logger.debug("Deferring on_start for unit due to cluster isn't initialized yet")
            event.defer()
            return
        self.config_manager.render_cassandra_config(
            cluster_name=self.charm.config.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            authentication=True,
        )
        self.workload.start()
        self.state.unit.workload_state = UnitWorkloadState.STARTING

    def _start_leader(self) -> None:
        self.config_manager.render_cassandra_config(
            cluster_name=self.charm.config.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            authentication=True,
        )
        self.workload.start()
        self.state.unit.workload_state = UnitWorkloadState.STARTING

    def _start_password_change(self, event: StartEvent) -> None:
        self.config_manager.render_cassandra_config(
            cluster_name=self.charm.config.cluster_name,
            listen_address="127.0.0.1",
            seeds=["127.0.0.1:7000"],
            authentication=False,
        )
        self.workload.start()
        self.state.unit.workload_state = UnitWorkloadState.CHANGING_PASSWORD
        event.defer()

    def _finalize_password_change(self, event: StartEvent) -> None:
        if not self.cluster_manager.is_healthy:
            event.defer()
            return
        password = self.workload.generate_password()
        self._cassandra.change_superuser_password("cassandra", password)
        self.state.cluster.cassandra_password_secret = password
        self.config_manager.render_cassandra_config(
            cluster_name=self.charm.config.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            authentication=True,
        )
        self.workload.restart()
        self.state.unit.workload_state = UnitWorkloadState.STARTING

    def _on_config_changed(self, _: ConfigChangedEvent) -> None:
        if self.state.unit.workload_state not in [
            UnitWorkloadState.STARTING,
            UnitWorkloadState.ACTIVE,
        ]:
            return
        try:
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None
            )
            self.config_manager.render_cassandra_config(
                cluster_name=self.charm.config.cluster_name,
                listen_address=self.state.unit.ip,
                seeds=self.state.cluster.seeds,
                authentication=True,
            )
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            return
        self.workload.restart()
        if self.state.unit.workload_state == UnitWorkloadState.ACTIVE:
            self.state.unit.workload_state = UnitWorkloadState.STARTING

    def _on_update_status(self, _: UpdateStatusEvent) -> None:
        if (
            self._update_network_address()
            and self.state.unit.workload_state == UnitWorkloadState.ACTIVE
        ):
            self.workload.restart()
            self.state.unit.workload_state = UnitWorkloadState.STARTING
            return

        if (
            self.state.unit.workload_state == UnitWorkloadState.STARTING
            and self.cluster_manager.is_healthy
        ):
            self.state.unit.workload_state = UnitWorkloadState.ACTIVE
            if self.charm.unit.is_leader():
                self.state.cluster.state = ClusterState.ACTIVE

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        try:
            self.charm.config
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)

        if self.state.unit.workload_state == UnitWorkloadState.INSTALLING:
            event.add_status(Status.INSTALLING.value)

        if (
            self.state.unit.workload_state == UnitWorkloadState.WAITING_FOR_START
            and not self.charm.unit.is_leader()
            and not self.state.cluster.is_active
        ):
            event.add_status(Status.WAITING_FOR_CLUSTER.value)

        if self.state.unit.workload_state in [
            UnitWorkloadState.WAITING_FOR_START,
            UnitWorkloadState.STARTING,
        ] and (self.charm.unit.is_leader() or self.state.cluster.is_active):
            event.add_status(Status.STARTING.value)

        if self.state.unit.workload_state == UnitWorkloadState.CHANGING_PASSWORD:
            event.add_status(Status.CHANGING_PASSWORD.value)

        event.add_status(Status.ACTIVE.value)

    def _on_collect_app_status(self, event: CollectStatusEvent) -> None:
        try:
            self.charm.config
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)

        event.add_status(Status.ACTIVE.value)

    def _update_network_address(self) -> bool:
        """Update hostname & ip in this unit context.

        Returns:
            whether the hostname or ip changed.
        """
        old_ip = self.state.unit.ip
        old_hostname = self.state.unit.hostname
        self.state.unit.ip, self.state.unit.hostname = self.cluster_manager.network_address()
        return (
            old_ip is not None
            and old_hostname is not None
            and (old_ip != self.state.unit.ip or old_hostname != self.state.unit.hostname)
        )

    @property
    def _cassandra(self) -> CassandraClient:
        return CassandraClient(
            [self.state.unit.ip if self.state.cluster.cassandra_password_secret else "127.0.0.1"],
            "cassandra",
            self.state.cluster.cassandra_password_secret or None,
        )
