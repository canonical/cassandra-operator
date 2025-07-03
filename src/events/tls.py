#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

from ops.charm import RelationBrokenEvent, RelationCreatedEvent
from ops.framework import EventBase, EventSource, Object

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.tls_certificates_interface.v4.tls_certificates import CertificateRequestAttributes, TLSCertificatesRequiresV4
from core.config import CharmConfig
from core.state import CLIENT_TLS_RELATION, PEER_RELATION, PEER_TLS_RELATION,ApplicationState, TLSScope, TLSContext, TLSState
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from managers.tls import TLSManager

from managers.tls import (
    setup_internal_ca,
    setup_internal_credentials,
)


logger = logging.getLogger(__name__)

class RefreshTLSCertificatesEvent(EventBase):
    """Event for refreshing TLS certificates."""

class TLSEvents(Object):

    refresh_tls_certificates = EventSource(RefreshTLSCertificatesEvent)    

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
        self.tls_manager = tls_manager
    
        self.sans = cluster_manager.get_host_mapping()
        self.common_name = f"{self.charm.unit.name}-{self.charm.model.uuid}"

        peer_private_key = self.state.unit.peer_tls.private_key
        client_private_key = self.state.unit.client_tls.private_key

        self.client_certificate = TLSCertificatesRequiresV4(
            self.charm,
            CLIENT_TLS_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.common_name,
                    sans_ip=frozenset(self.sans["ip"]),
                    sans_dns=frozenset(self.sans["hostname"]),
                    organization=TLSScope.CLIENT.value,
                ),
            ],
            refresh_events=[self.refresh_tls_certificates],
            private_key=client_private_key,
        )

        self.peer_certificate = TLSCertificatesRequiresV4(
            self.charm,
            PEER_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.common_name,
                    sans_ip=frozenset(self.sans["ip"]),
                    sans_dns=frozenset(self.sans["hostname"]),
                    organization=TLSScope.PEER.value,
                ),
            ],
            refresh_events=[self.refresh_tls_certificates],
            private_key=peer_private_key,
        )

        self._init_credentials()

        for rel in (PEER_TLS_RELATION, CLIENT_TLS_RELATION):
            self.framework.observe(self.charm.on[rel].relation_created, self._tls_relation_created)
            self.framework.observe(self.charm.on[rel].relation_broken, self._tls_relation_broken)

        # for relation in [self.client_certificate, self.peer_certificate]:
        #     self.framework.observe(
        #         relation.on.certificate_available, self._on_certificate_available
        #     )

    def _tls_relation_created(self, event: RelationCreatedEvent) -> None:
        """Handler for `certificates_relation_created` event."""
        if not self.charm.unit.is_leader() or not self.state.peer_relation:
            return

        if event.relation.name == PEER_TLS_RELATION:
            self.state.cluster.tls_state = TLSState.ACTIVE
        
    def _tls_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `certificates_relation_broken` event."""
        state = (
            self.state.unit.client_tls
            if event.relation.name == PEER_TLS_RELATION
            else self.state.unit.peer_tls
        )

        # clear TLS state
        state.csr = None
        state.certificate = None
        state.chain = []
        state.ca = None

        # remove all existing keystores from the unit so we don't preserve certs
        self.tls_manager.remove_stores(scope=state.scope)

        if state.scope == TLSScope.PEER:
            # switch back to internal TLS
            is_leader = self.charm.unit.is_leader()
            if is_leader:
                setup_internal_ca(self.tls_manager, self.state)
                
            setup_internal_credentials(
                self.tls_manager,
                self.state,
                sans_ip=frozenset({self.sans["ip"]}),
                sans_dns=frozenset({self.charm.unit.name, self.sans["hostname"]}),
                is_leader=self.charm.unit.is_leader(),
            )

            state.rotation = True
            self.charm.on.config_changed.emit()

        if not self.charm.unit.is_leader():
            return

        if state.scope == TLSScope.CLIENT:
            self.state.cluster.tls_state = TLSState.UNKNOWN

            
    def requirer_state(self, requirer: TLSCertificatesRequiresV4) -> TLSContext:
        """Returns the appropriate TLSState based on the scope of the TLS Certificates Requirer instance."""
        if requirer.relationship_name == CLIENT_TLS_RELATION:
            return self.state.unit.client_tls
        elif requirer.relationship_name == PEER_TLS_RELATION:
            return self.state.unit.peer_tls

        raise NotImplementedError(f"{requirer.relationship_name} not supported!")
        
        
    def _init_credentials(self) -> None:
        """Sets private key, keystore password and truststore passwords if not already set."""
        for requirer in (self.peer_certificate, self.client_certificate):
            _, private_key = requirer.get_assigned_certificate(requirer.certificate_requests[0])

            if private_key and self.requirer_state(requirer).private_key != private_key:
                self.requirer_state(requirer).private_key = private_key

        # generate unit private key if not already created by action
        if not self.state.unit.keystore_password:
            self.state.unit.keystore_password = self.workload.generate_password()
        if not self.state.unit.truststore_password:
            self.state.unit.truststore_password = self.workload.generate_password()

        
