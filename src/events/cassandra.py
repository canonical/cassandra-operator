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
from core.state import ApplicationState, ClusterState, TLSScope, UnitWorkloadState
from core.statuses import Status
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from core import state
from managers.tls import TLSManager
from common.cassandra_client import CassandraClient

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
        tls_manager: TLSManager,
    ):
        super().__init__(charm, key="cassandra_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.cluster_manager = cluster_manager
        self.config_manager = config_manager
        self.tls_manager = tls_manager.with_client(self._cassandra)

        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.collect_unit_status, self._on_collect_unit_status)

    def _on_install(self, _: InstallEvent) -> None:
        self.workload.install()
        self.state.unit.workload_state = UnitWorkloadState.STARTING.value

        # TODO: move to snap?
        self.workload.cassandra_paths.tls_directory.mkdir(exist_ok=True)

    def _on_start(self, event: StartEvent) -> None:
        self._update_network_address()

        if not self.state.unit.peer_tls.ready and not self.state.cluster.internal_ca:
            if not self.charm.unit.is_leader():
                event.defer()
                return

            self._setup_internal_ca()


        self._setup_internal_credentials()


        if self.charm.unit.is_leader():
            self.state.cluster.seeds = [self.state.unit.peer_url]        

        try:
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None
            )

            self.config_manager.render_cassandra_config(
                cluster_name=self.charm.config.cluster_name,
                listen_address=self.state.unit.ip,
                seeds=self.state.cluster.seeds,
                enable_tls=True,
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

        if self.state.unit.workload_state == UnitWorkloadState.ACTIVE.value and (
            not self.workload.alive() or not self.cluster_manager.is_healthy
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

    def _setup_internal_ca(self) -> None:
        if not self.state.unit.unit.is_leader():
            return

        ca, pk = self.tls_manager.generate_internal_ca(common_name=self.state.unit.unit.app.name)
        
        self.state.cluster.internal_ca = ca
        self.state.cluster.internal_ca_key = pk

        
    def _setup_internal_credentials(self, is_leader: bool = False) -> None:
        ca = self.state.cluster.internal_ca
        ca_key = self.state.cluster.internal_ca_key
        
        if ca is None or ca_key is None:
            logger.error("Internal CA is not set up yet.")
            return

        if self.state.unit.peer_tls.ready:
            logger.debug("No need to set up internal credentials...")
            self._configure_internal_tls()
            return

        host_mapping = self.cluster_manager.get_host_mapping()
        provider_crt, pk = self.tls_manager.generate_internal_credentials(
            ca=ca,
            ca_key=ca_key,
            common_name=self.state.unit.unit.app.name,
            sans_ip=frozenset({host_mapping["ip"]}),
            sans_dns=frozenset({self.charm.unit.name, host_mapping["hostname"]}),
        )

        if not pk:
            logger.error("private key for internal tls is empty")
            return

        self.state.unit.peer_tls.certificate = provider_crt[0].certificate
        self.state.unit.peer_tls.csr = provider_crt[0].certificate_signing_request
        self.state.unit.peer_tls.private_key = pk
        self.state.unit.peer_tls.ca = ca
        self.state.unit.peer_tls.chain = provider_crt[0].chain

        self._configure_internal_tls()

        if is_leader:
            self.state.cluster.peer_cluster_ca = self.state.unit.peer_tls.bundle

    def _configure_internal_tls(self) -> None:
        if not self.state.unit.peer_tls.ready:
            return

        resolved = self.state.unit.peer_tls.resolved()
        
        self.tls_manager.configure(
            pk=resolved.private_key,
            ca=resolved.ca,
            chain=resolved.chain,
            certificate=resolved.certificate,
            bundle=resolved.bundle,
            pk_password="",
            keystore_password="myStorePass",
            trust_password="myStorePass",
        )            

    @property
    def _cassandra(self) -> CassandraClient:
        return CassandraClient(
            [self.state.unit.ip]
        )        
