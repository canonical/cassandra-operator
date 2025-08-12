#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for main Cassandra charm events."""

import logging
from typing import Callable

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops import (
    CollectStatusEvent,
    ConfigChangedEvent,
    InstallEvent,
    ModelError,
    Object,
    SecretChangedEvent,
    SecretNotFoundError,
    StartEvent,
    UpdateStatusEvent,
)
from pydantic import ValidationError

from core.config import CharmConfig
from core.state import ApplicationState, UnitWorkloadState
from core.statuses import Status
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from managers.database import DatabaseManager
from managers.tls import Sans, TLSManager

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
        bootstrap_manager: RollingOpsManager,
        tls_manager: TLSManager,
        setup_internal_certificates: Callable[[Sans], bool],
        database_manager: DatabaseManager,
    ):
        super().__init__(charm, key="cassandra_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.cluster_manager = cluster_manager
        self.config_manager = config_manager
        self.bootstrap_manager = bootstrap_manager
        self.tls_manager = tls_manager
        self.setup_internal_certificates = setup_internal_certificates
        self.database_manager = database_manager

        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.collect_unit_status, self._on_collect_unit_status)
        self.framework.observe(self.charm.on.collect_app_status, self._on_collect_app_status)

    def _on_install(self, _: InstallEvent) -> None:
        self.workload.install()

    def _on_start(self, event: StartEvent) -> None:
        self._update_network_address()

        if not self.charm.unit.is_leader() and not self.state.cluster.is_active:
            self.state.unit.workload_state = UnitWorkloadState.WAITING_FOR_START
            logger.debug("Deferring on_start for unit due to cluster isn't initialized yet")
            event.defer()
            return

        if not self.setup_internal_certificates(
            self.tls_manager.build_sans(
                sans_ip=[self.state.unit.ip],
                sans_dns=[self.state.unit.hostname, self.charm.unit.name],
            )
        ):
            event.defer()
            return

        if self.state.unit.workload_state == UnitWorkloadState.CHANGING_PASSWORD:
            self._finalize_password_change(event)
            return

        try:
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None,
            )
            if self.charm.unit.is_leader():
                self.state.cluster.cluster_name = self.charm.config.cluster_name
                self.state.cluster.seeds = [self.state.unit.peer_url]
                self.state.cluster.cassandra_password_secret = self._acquire_cassandra_password()
                self._start_password_change(event)
                return
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            event.defer()
            return

        self.config_manager.render_cassandra_config(
            cluster_name=self.state.cluster.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
            authentication=True,
        )
        self.charm.on[str(self.bootstrap_manager.name)].acquire_lock.emit()

    def _acquire_cassandra_password(self) -> str:
        if self.charm.config.system_users:
            try:
                if (
                    password := self.model.get_secret(id=self.charm.config.system_users)
                    .get_content(refresh=True)
                    .get("cassandra-password")
                ):
                    return password
            except SecretNotFoundError:
                # TODO: logging.
                pass
            except ModelError:
                pass
        if self.state.cluster.cassandra_password_secret:
            return self.state.cluster.cassandra_password_secret
        return self.workload.generate_password()

    def _start_password_change(self, event: StartEvent) -> None:
        self.config_manager.render_cassandra_config(
            cluster_name=self.state.cluster.cluster_name,
            listen_address="127.0.0.1",
            seeds=["127.0.0.1:7000"],
        )
        self.workload.start()
        self.state.unit.workload_state = UnitWorkloadState.CHANGING_PASSWORD
        event.defer()

    def _finalize_password_change(self, event: StartEvent) -> None:
        if not self.cluster_manager.is_healthy:
            event.defer()
            return
        self.database_manager.update_system_user_password(
            self.state.cluster.cassandra_password_secret
        )
        self.cluster_manager.prepare_shutdown()
        self.config_manager.render_cassandra_config(
            cluster_name=self.state.cluster.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
        )
        self.charm.on[str(self.bootstrap_manager.name)].acquire_lock.emit()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        if self.state.unit.workload_state == UnitWorkloadState.INSTALLING:
            return
        if self.state.unit.workload_state != UnitWorkloadState.ACTIVE:
            event.defer()
            return
        try:
            if self.charm.unit.is_leader() and self.state.cluster.cassandra_password_secret != (
                password := self._acquire_cassandra_password()
            ):
                self.database_manager.update_system_user_password(password)
                self.state.cluster.cassandra_password_secret = password
                self.cluster_manager.prepare_shutdown()
            # TODO: cluster_name change
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None
            )
            self.config_manager.render_cassandra_config()
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            return

        self.charm.on[str(self.bootstrap_manager.name)].acquire_lock.emit()

    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            return

        if self.state.unit.workload_state == UnitWorkloadState.INSTALLING:
            return

        try:
            if event.secret.id != self.charm.config.system_users:
                return

            if self.state.unit.workload_state != UnitWorkloadState.ACTIVE:
                event.defer()
                return

            if self.state.cluster.cassandra_password_secret != (
                password := self._acquire_cassandra_password()
            ):
                self.database_manager.update_system_user_password(password)
                self.state.cluster.cassandra_password_secret = password
        except ValidationError:
            return

    def _on_update_status(self, _: UpdateStatusEvent) -> None:
        # TODO: add peer relation change hook for subordinates to update leader address too
        if (
            self._update_network_address()
            and self.state.unit.workload_state == UnitWorkloadState.ACTIVE
        ):
            if self.charm.unit.is_leader():
                self.state.cluster.seeds = [self.state.unit.peer_url]

            self.config_manager.render_cassandra_config(
                listen_address=self.state.unit.ip,
                seeds=self.state.cluster.seeds,
            )

            self.charm.on[str(self.bootstrap_manager.name)].acquire_lock.emit()

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        try:
            self.charm.config
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)

        if self.state.unit.workload_state == UnitWorkloadState.INSTALLING:
            event.add_status(Status.INSTALLING.value)

        if self.state.unit.peer_tls.ready and self.state.unit.peer_tls.rotation:
            event.add_status(Status.ROTATING_PEER_TLS.value)

        if self.state.unit.client_tls.ready and self.state.unit.client_tls.rotation:
            event.add_status(Status.ROTATING_CLIENT_TLS.value)

        if not self.state.cluster.internal_ca:
            event.add_status(Status.WAITING_FOR_INTERNAL_TLS.value)

        if self.state.cluster.tls_state and not self.tls_manager.client_tls_ready:
            event.add_status(Status.WAITING_FOR_TLS.value)

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
