#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TLS event handling and related logic for the charm."""

import logging
from typing import Callable

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from charms.tls_certificates_interface.v4.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from ops.charm import RelationBrokenEvent, RelationCreatedEvent
from ops.framework import EventBase, EventSource, Object

from core.config import CharmConfig
from core.state import (
    CLIENT_TLS_RELATION,
    PEER_TLS_RELATION,
    ApplicationState,
    TLSContext,
    TLSScope,
    TLSState,
    UnitWorkloadState,
)
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from managers.tls import Sans, TLSManager

logger = logging.getLogger(__name__)


class RefreshTLSCertificatesEvent(EventBase):
    """Event for refreshing TLS certificates."""


class TLSEvents(Object):
    """Manage TLS-related events and event sources for the charm."""

    refresh_tls_certificates = EventSource(RefreshTLSCertificatesEvent)

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
    ):
        super().__init__(charm, key="tls_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.config_manager = config_manager
        self.bootstrap_manager = bootstrap_manager
        self.tls_manager = tls_manager
        self.setup_internal_certificates = setup_internal_certificates

        ip, hostname = cluster_manager.network_address()
        self.sans = self.tls_manager.build_sans(
            sans_ip=[ip],
            sans_dns=[hostname, self.charm.unit.name],
        )

        self.common_name = f"{self.charm.unit.name}-{self.charm.model.uuid}"

        peer_private_key = self.state.unit.peer_tls.private_key
        client_private_key = self.state.unit.client_tls.private_key

        self.client_certificate = TLSCertificatesRequiresV4(
            self.charm,
            CLIENT_TLS_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.common_name,
                    sans_ip=frozenset(self.sans.sans_ip),
                    sans_dns=frozenset(self.sans.sans_dns),
                    organization=TLSScope.CLIENT.value,
                ),
            ],
            refresh_events=[self.refresh_tls_certificates],
            private_key=client_private_key,
        )

        self.peer_certificate = TLSCertificatesRequiresV4(
            self.charm,
            PEER_TLS_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.common_name,
                    sans_ip=frozenset(self.sans.sans_ip),
                    sans_dns=frozenset(self.sans.sans_dns),
                    organization=TLSScope.PEER.value,
                ),
            ],
            private_key=peer_private_key,
        )

        self._init_credentials()

        for rel in (PEER_TLS_RELATION, CLIENT_TLS_RELATION):
            self.framework.observe(self.charm.on[rel].relation_created, self._tls_relation_created)
            self.framework.observe(self.charm.on[rel].relation_broken, self._tls_relation_broken)

        self.framework.observe(
            self.client_certificate.on.certificate_available, self._on_client_certificate_available
        )

        self.framework.observe(
            self.peer_certificate.on.certificate_available, self._on_peer_certificate_available
        )

    def _tls_relation_created(self, event: RelationCreatedEvent) -> None:
        """Handle `certificates_relation_created` event."""
        if not self.charm.unit.is_leader() or not self.state.peer_relation:
            return

        if event.relation.name == CLIENT_TLS_RELATION:
            self.state.cluster.tls_state = TLSState.ACTIVE

    def _on_peer_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handle `certificate_available` event.

        After provider updates signed certs for peer TLS relation.
        """
        self._handle_certificate_available_event(event, self.requirer_state(self.peer_certificate))

    def _on_client_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handle `certificate_available` event.

        After provider updates signed certs for client TLS relation.
        """
        self._handle_certificate_available_event(
            event, self.requirer_state(self.client_certificate)
        )

    def _handle_certificate_available_event(
        self,
        event: CertificateAvailableEvent,
        tls_state: TLSContext,
    ) -> None:
        if not self.state.peer_relation:
            logger.warning("No peer relation on certificate available")
            event.defer()
            return

        tls_changed = (
            tls_state.certificate and event.certificate.raw != tls_state.certificate.raw
        ) or (tls_state.ca and event.ca.raw != tls_state.ca.raw)

        tls_state.certificate = event.certificate
        tls_state.ca = event.ca
        tls_state.chain = event.chain

        self.tls_manager.remove_stores(scope=tls_state.scope)
        self.tls_manager.configure(
            tls_state.resolved,
            keystore_password=self.state.unit.keystore_password,
            trust_password=self.state.unit.truststore_password,
        )
        self.config_manager.render_cassandra_config(
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
        )

        if tls_changed:
            tls_state.rotation = True

        if self.state.unit.workload_state == UnitWorkloadState.ACTIVE:
            self.charm.on[str(self.bootstrap_manager.name)].acquire_lock.emit()

    def _tls_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle `certificates_relation_broken` event."""
        state = (
            self.state.unit.peer_tls
            if event.relation.name == PEER_TLS_RELATION
            else self.state.unit.client_tls
        )

        # clear TLS state
        state.csr = None
        state.certificate = None
        state.chain = []
        state.ca = None

        self.tls_manager.remove_stores(scope=state.scope)

        if state.scope == TLSScope.CLIENT and self.charm.unit.is_leader():
            self.state.cluster.tls_state = TLSState.UNKNOWN
        else:
            # switch back to internal TLS
            if not self.setup_internal_certificates(self.sans):
                event.defer()
                return
            state.rotation = True

        self.config_manager.render_cassandra_config(
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
        )

        if self.state.unit.workload_state == UnitWorkloadState.ACTIVE:
            self.charm.on[str(self.bootstrap_manager.name)].acquire_lock.emit()

    def requirer_state(self, requirer: TLSCertificatesRequiresV4) -> TLSContext:
        """Return the appropriate TLSState based on the scope."""
        if requirer.relationship_name == CLIENT_TLS_RELATION:
            return self.state.unit.client_tls
        elif requirer.relationship_name == PEER_TLS_RELATION:
            return self.state.unit.peer_tls

        raise NotImplementedError(f"{requirer.relationship_name} not supported!")

    def _init_credentials(self) -> None:
        """Set private key, keystore password and truststore passwords if not already set."""
        for requirer in (self.peer_certificate, self.client_certificate):
            _, private_key = requirer.get_assigned_certificate(requirer.certificate_requests[0])

            if not private_key:
                continue

            current_key = self.requirer_state(requirer).private_key
            if current_key is None or current_key.raw != private_key.raw:
                self.requirer_state(requirer).private_key = private_key

        # generate unit private key if not already created by action
        if not self.state.unit.keystore_password:
            self.state.unit.keystore_password = self.workload.generate_password()
        if not self.state.unit.truststore_password:
            self.state.unit.truststore_password = self.workload.generate_password()
