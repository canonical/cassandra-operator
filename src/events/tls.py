"""TLS event handling and related logic for the charm."""
#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

from charms.data_platform_libs.v1.data_models import TypedCharmBase
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
)
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.tls import TLSManager

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
        tls_manager: TLSManager,
    ):
        super().__init__(charm, key="tls_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.tls_manager = tls_manager

        host_mapping = cluster_manager.network_address()
        self.sans = self.tls_manager.build_sans(
            sans_ip=[host_mapping[0]],
            sans_dns=[host_mapping[1], self.charm.unit.name],
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
                    sans_ip=frozenset(self.sans["sans_ip"]),
                    sans_dns=frozenset(self.sans["sans_dns"]),
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
                    sans_ip=frozenset(self.sans["sans_ip"]),
                    sans_dns=frozenset(self.sans["sans_dns"]),
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
        if not self.state.peer_relation:
            logger.warning("No peer relation on certificate available")
            event.defer()
            return

        if not self.workload.installed:
            logger.warning("Workload is not yet installed")
            event.defer()
            return

        self._handle_certificate_available_event(event, self.peer_certificate)
        self.charm.on.config_changed.emit()

    def _on_client_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handle `certificate_available` event.

        After provider updates signed certs for client TLS relation.
        """
        if not self.state.peer_relation:
            logger.warning("No peer relation on certificate available")
            event.defer()
            return

        if not self.workload.installed:
            logger.warning("Workload is not yet installed")
            event.defer()
            return

        self._handle_certificate_available_event(event, self.client_certificate)
        self.charm.on.config_changed.emit()

    def _handle_certificate_available_event(
        self,
        event: CertificateAvailableEvent,
        requirer: TLSCertificatesRequiresV4,
    ) -> None:
        tls_changed = False

        tls_state = self.requirer_state(requirer)

        if tls_state.certificate and event.certificate.raw != tls_state.certificate.raw:
            tls_changed = True

        if tls_state.ca and event.ca.raw != tls_state.ca.raw:
            tls_changed = True

        tls_state.certificate = event.certificate
        tls_state.ca = event.ca
        tls_state.chain = event.chain

        self.tls_manager.remove_stores(scope=tls_state.scope)
        self.tls_manager.configure(
            tls_state.resolved(),
            keystore_password=self.state.unit.keystore_password,
            trust_password=self.state.unit.truststore_password,
        )

        if tls_changed:
            tls_state.rotation = True

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

        is_leader = self.charm.unit.is_leader()

        if state.scope == TLSScope.CLIENT and is_leader:
            self.state.cluster.tls_state = TLSState.UNKNOWN
            return

        # switch back to internal TLS
        if is_leader and not self.state.cluster.internal_ca:
            ca, pk = self.tls_manager.generate_internal_ca(
                common_name=self.state.unit.unit.app.name,
            )

            self.state.cluster.internal_ca = ca
            self.state.cluster.internal_ca_key = pk

        if not self.state.cluster.internal_ca or not self.state.cluster.internal_ca_key:
            logger.debug(
                "Deferring _tls_relation_broken for unit \
               due to cluster internal CA's isn't initialized yet"
            )
            event.defer()
            return

        if not self.state.unit.peer_tls.ready:
            provider_crt, pk = self.tls_manager.generate_internal_credentials(
                ca=self.state.cluster.internal_ca,
                ca_key=self.state.cluster.internal_ca_key,
                unit_key=self.state.unit.peer_tls.private_key,
                common_name=self.state.unit.unit.name,
                sans_ip=frozenset(self.sans["sans_ip"]),
                sans_dns=frozenset(self.sans["sans_dns"]),
            )

            self.state.unit.peer_tls.setup_provider_certificates(provider_crt)
            self.state.unit.peer_tls.private_key = pk
            self.state.unit.peer_tls.ca = self.state.cluster.internal_ca

        self.tls_manager.configure(
            self.state.unit.peer_tls.resolved(),
            keystore_password=self.state.unit.keystore_password,
            trust_password=self.state.unit.truststore_password,
        )

        state.rotation = True
        self.charm.on.config_changed.emit()

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
