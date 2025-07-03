#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from dataclasses import dataclass
import logging
import json
from enum import StrEnum
from typing import List, Optional

from charms.data_platform_libs.v0.data_interfaces import (
    Data,
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
)

from charms.tls_certificates_interface.v4.tls_certificates import (
    Certificate,
    CertificateSigningRequest,
    PrivateKey,
)
from ops import Application, CharmBase, Object, Relation, Unit

PEER_RELATION = "cassandra-peers"
PEER_PORT = 7000
CLIENT_PORT = 9042

logger = logging.getLogger(__name__)

class TLSScope(StrEnum):
    """Enum for TLS scopes."""

    PEER = "peer"  # for internal communications
    CLIENT = "client"  # for external/client communications


class ClusterState(StrEnum):
    """TODO."""

    UNKNOWN = ""
    ACTIVE = "active"


class UnitWorkloadState(StrEnum):
    """TODO."""

    INSTALLING = ""
    WAITING_FOR_START = "waiting_for_start"
    STARTING = "starting"
    ACTIVE = "active"

    
class RelationState:
    """Relation state object."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Unit | Application | None,
    ):
        self.relation = relation
        self.data_interface = data_interface
        self.component = component
        self.relation_data = self.data_interface.as_dict(self.relation.id) if self.relation else {}

    def _field_setter_wrapper(self, field: str, value: str) -> None:
        if not self.relation:
            logger.warning(
                f"Field `{field}` were attempted to\
                be written on the relation before it exists."
            )
            return

        if value == "":
            try:
                del self.relation_data[field]
            except KeyError:
                pass
        else:
            self.relation_data.update({field: value})            

@dataclass
class ResolvedTLSState:
    private_key: PrivateKey
    ca: Certificate
    certificate: Certificate
    chain: list[Certificate]
    bundle: list[Certificate]
            
class TLSState(RelationState):
    """State collection metadata for TLS credentials."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Unit,
        scope: TLSScope):
        self.scope = scope
        super().__init__(relation, data_interface, component)

    @property
    def private_key(self) -> Optional[PrivateKey]:
        """The unit private-key set during `certificates_joined`.

        Returns:
            String of key contents
            Empty if key not yet generated
        """
        raw = self.relation_data.get(f"{self.scope.value}-private-key", "")
        if not raw:
            return None
        return PrivateKey.from_string(raw)

    @private_key.setter
    def private_key(self, value: PrivateKey) -> None:
        self._field_setter_wrapper(f"{self.scope.value}-private-key", value.raw)

    @property
    def csr(self) -> Optional[CertificateSigningRequest]:
        """The unit cert signing request.

        Returns:
            String of csr contents
            Empty if csr not yet generated
        """
        raw = self.relation_data.get(f"{self.scope.value}-csr", "")
        if not raw:
            return None
        return CertificateSigningRequest.from_string(raw)

    @csr.setter
    def csr(self, value: CertificateSigningRequest) -> None:
        self._field_setter_wrapper(f"{self.scope.value}-csr", value.raw)

    @property
    def certificate(self) -> Optional[Certificate]:
        """The signed unit certificate from the provider relation."""
        raw = self.relation_data.get(f"{self.scope.value}-certificate", "")
        if not raw:
            return None
        return Certificate.from_string(raw)

    @certificate.setter
    def certificate(self, value: Certificate) -> None:
        self._field_setter_wrapper(f"{self.scope.value}-certificate", value.raw)

    @property
    def ca(self) -> Optional[Certificate]:
        """The ca used to sign unit cert.

        Returns:
            String of ca contents in PEM format
            Empty if cert not yet generated/signed
        """
        # defaults to ca for backwards compatibility after field change introduced with secrets
        raw = self.relation_data.get(f"{self.scope.value}-ca-cert", "")
        if not raw:
            return None

        return Certificate.from_string(raw)

    @ca.setter
    def ca(self, value: Certificate) -> None:
        self._field_setter_wrapper(f"{self.scope.value}-ca-cert", value.raw)

    @property
    def chain(self) -> List[Certificate]:
        """The chain used to sign the unit cert."""
        raw = self.relation_data.get(f"{self.scope.value}-chain")
        if not raw:
            return []
        return [Certificate.from_string(c) for c in json.loads(raw)]

    @chain.setter
    def chain(self, value: List[Certificate]) -> None:
        """Sets the chain used to sign the unit cert."""
        self._field_setter_wrapper(f"{self.scope.value}-chain", json.dumps([str(c) for c in value]))

    @property
    def bundle(self) -> List[Certificate]:
        """The cert bundle used for TLS identity."""
        if not all([self.certificate, self.ca]):
            return []

        cert = self.certificate
        ca = self.ca
        if not cert or not ca:
            return []
        return list(dict.fromkeys([cert, ca] + self.chain))
    
    @property
    def rotation(self) -> bool:
        """Whether or not CA/chain rotation is in progress."""
        return bool(self.relation_data.get(f"{self.scope.value}-rotation", ""))

    @rotation.setter
    def rotation(self, value: bool) -> None:
        _value = "" if not value else "true"
        self._field_setter_wrapper(f"{self.scope.value}-rotation", _value)

    @property
    def ready(self) -> bool:
        """Returns True if all the necessary TLS relation data has been set, False otherwise."""
        return all([self.certificate, self.ca, self.private_key])

    def resolved(self) -> ResolvedTLSState:
        if not self.certificate or not self.private_key or not self.ca:
            raise RuntimeError("TLS state is incomplete")
        return ResolvedTLSState(
            private_key=self.private_key,
            ca=self.ca,
            certificate=self.certificate,
            chain=self.chain,
            bundle=self.bundle,
        )
    

            
class UnitContext(RelationState):
    """State/Relation data collection for a unit."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerUnitData,
        component: Unit,
    ):
        super().__init__(relation, data_interface, component)
        self.unit = component

    @property
    def unit_id(self) -> int:
        """The id of the unit from the unit name."""
        return int(self.unit.name.split("/")[1])

    @property
    def unit_name(self) -> str:
        """The id of the unit from the unit name."""
        return self.unit.name

    @property
    def node_name(self) -> str:
        """The Human-readable name for this cassandra cluster node."""
        return f"{self.unit.app.name}{self.unit_id}"

    @property
    def hostname(self) -> str:
        """The hostname for the unit."""
        return self.relation_data.get("hostname", "")

    @hostname.setter
    def hostname(self, value: str) -> None:
        self._field_setter_wrapper("hostname", value)

    @property
    def ip(self) -> str:
        """The IP address for the unit."""
        return self.relation_data.get("ip", "")

    @ip.setter
    def ip(self, value: str) -> None:
        self._field_setter_wrapper("ip", value)

    @property
    def peer_url(self) -> str:
        """The peer connection endpoint for the cassandra server."""
        return f"{self.ip}:{PEER_PORT}"

    @property
    def client_url(self) -> str:
        """The client connection endpoint for the cassandra server."""
        return f"{self.ip}:{CLIENT_PORT}"

    @property
    def workload_state(self) -> UnitWorkloadState:
        """TODO."""
        return self.relation_data.get("workload_state", UnitWorkloadState.INSTALLING)

    @workload_state.setter
    def workload_state(self, value: UnitWorkloadState) -> None:
        self._field_setter_wrapper("workload_state", value.value)

    # --- TLS ---
    @property
    def peer_tls(self) -> TLSState:
        """TLS state for internal (peer) communications."""
        return TLSState(self.relation, self.data_interface, self.unit, TLSScope.PEER)

    @property
    def client_tls(self) -> TLSState:
        """TLS state for external (client) communications."""
        return TLSState(self.relation, self.data_interface, self.unit, TLSScope.CLIENT)

    @property
    def keystore_password(self) -> str:
        """The unit keystore password set during `certificates_joined`.

        Returns:
            String of password
            None if password not yet generated
        """
        return self.relation_data.get("keystore-password", "")

    @property
    def truststore_password(self) -> str:
        """The unit truststore password set during `certificates_joined`.

        Returns:
            String of password
            None if password not yet generated
        """
        return self.relation_data.get("truststore-password", "")
        


class ClusterContext(RelationState):
    """State/Relation data collection for the cassandra application."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerData,
        component: Application,
    ):
        super().__init__(relation, data_interface, component)
        self.app = component


    @property
    def seeds(self) -> list[str]:
        """TODO."""
        seeds = self.relation_data.get("seeds", "")
        return seeds.split(",") if seeds else []

    @seeds.setter
    def seeds(self, value: list[str]) -> None:
        self._field_setter_wrapper("seeds", ",".join(value))    
        
    @property
    def seeds(self) -> list[str]:
        """TODO."""
        seeds = self.relation_data.get("seeds", "")
        return seeds.split(",") if seeds else []

    @seeds.setter
    def seeds(self, value: list[str]) -> None:
        self._field_setter_wrapper("seeds", ",".join(value))

    @property
    def state(self) -> ClusterState:
        """The cluster state ('new' or 'existing') of the cassandra cluster."""
        return self.relation_data.get("cluster_state", ClusterState.UNKNOWN)

    @state.setter
    def state(self, value: ClusterState) -> None:
        """TODO."""
        self._field_setter_wrapper("cluster_state", value.value)

    @property
    def is_active(self) -> bool:
        """TODO."""
        return self.state == ClusterState.ACTIVE

    # --- TLS ---
    @property
    def internal_ca(self) -> Certificate | None:
        """The internal CA certificate used for the peer relations."""
        ca = self.relation_data.get("internal-ca", "")
        ca_key = self.internal_ca_key

        if ca_key is None or not ca:
            return None

        return Certificate.from_string(ca)

    @internal_ca.setter
    def internal_ca(self, value: Certificate) -> None:
        self._field_setter_wrapper("internal-ca", str(value))

    @property
    def internal_ca_key(self) -> PrivateKey | None:
        """The private key of internal CA certificate used for the peer relations."""
        if not (ca_key := self.relation_data.get("internal-ca-key", "")):
            return None

        return PrivateKey.from_string(ca_key)

    @internal_ca_key.setter
    def internal_ca_key(self, value: PrivateKey) -> None:
        self._field_setter_wrapper("internal-ca-key", str(value))

    @property
    def peer_cluster_ca(self) -> List[Certificate]:
        raw = self.relation_data.get("bundle", "")
        if not raw:
            return []
        return [Certificate.from_string(c) for c in json.loads(raw)]

    @peer_cluster_ca.setter
    def peer_cluster_ca(self, value: List[Certificate]) -> None:
        self._field_setter_wrapper("bundle", json.dumps([str(c) for c in value]))
    

class ApplicationState(Object):
    """Global state object for the cassandra cluster."""

    def __init__(self, charm: CharmBase):
        super().__init__(parent=charm, key="charm_state")
        self.peer_app_interface = DataPeerData(
            self.model,
            relation_name=PEER_RELATION,
        )
        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=PEER_RELATION)

    @property
    def peer_relation(self) -> Relation | None:
        """Get the cluster peer relation."""
        return self.model.get_relation(PEER_RELATION)

    @property
    def peer_relation_units(self) -> dict[Unit, DataPeerOtherUnitData]:
        """Get unit data interface of all peer units from the cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        return {
            unit: DataPeerOtherUnitData(model=self.model, unit=unit, relation_name=PEER_RELATION)
            for unit in self.peer_relation.units
        }

    @property
    def cluster(self) -> ClusterContext:
        """Get the cluster context of the entire cassandra application."""
        return ClusterContext(
            relation=self.peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
        )

    @property
    def unit(self) -> UnitContext:
        """Get the server state of this unit."""
        return UnitContext(
            relation=self.peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
        )

    @property
    def units(self) -> set[UnitContext]:
        """Get all nodes/units in the current peer relation, including this unit itself.

        Note: This is not to be confused with the list of cluster members.

        Returns:
            Set of CassandraUnitContexts with their unit data.
        """
        if not self.peer_relation:
            return set()

        return {
            self.unit,
            *(
                UnitContext(
                    relation=self.peer_relation,
                    data_interface=data_interface,
                    component=unit,
                )
                for unit, data_interface in self.peer_relation_units.items()
            ),
        }
