#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application state definition."""

import json
import logging
import os
from dataclasses import dataclass
from enum import StrEnum

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

DATA_STORAGE = "data"
CLIENT_TLS_RELATION = "client-certificates"
PEER_TLS_RELATION = "peer-certificates"
PEER_RELATION = "cassandra-peers"
CASSANDRA_PEER_PORT = 7000
CASSANDRA_CLIENT_PORT = 9042
JMX_EXPORTER_PORT = 7071

METRICS_RULES_DIR = "./src/alert_rules/prometheus"

SECRETS_UNIT = [
    "truststore-password-secret",
    "keystore-password-secret",
    "client-ca-cert-secret",
    "client-certificate-secret",
    "client-chain-secret",
    "client-csr-secret",
    "client-private-key-secret",
    "peer-ca-cert-secret",
    "peer-certificate-secret",
    "peer-chain-secret",
    "peer-csr-secret",
    "peer-private-key-secret",
]

SECRETS_APP = ["internal-ca-secret", "internal-ca-key-secret", "operator-password"]

logger = logging.getLogger(__name__)


class TLSScope(StrEnum):
    """Enum for TLS scopes."""

    PEER = "peer"  # for internal communications
    CLIENT = "client"  # for external/client communications


class TLSState(StrEnum):
    """Current state of the Cassandra cluster."""

    UNKNOWN = ""
    ACTIVE = "active"
    """Cassandra cluster is initialized by the leader unit and active."""


class ClusterState(StrEnum):
    """Current state of the Cassandra cluster."""

    UNKNOWN = ""
    """Cassandra cluster isn't yet initialized by the leader unit."""
    ACTIVE = "active"
    """Cassandra cluster is initialized by the leader unit and active."""


class UnitWorkloadState(StrEnum):
    """Current state of the Cassandra workload."""

    INSTALLING = ""
    """Cassandra is installing."""
    WAITING_FOR_START = "waiting_for_start"
    """Unit is waiting prior startup sequence."""
    STARTING = "starting"
    """Cassandra is starting."""
    CANT_START = "cant_start"
    """Cassandra service can't start currently. Another attempt will be taken soon."""
    ACTIVE = "active"
    """Cassandra is active and ready."""


class RelationState:
    """Basic class for relation bag mapping classes."""

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
            logger.error(
                f"Field `{field}` were attempted to be written on the relation before it exists."
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
class ResolvedTLSContext:
    """..."""

    private_key: PrivateKey
    ca: Certificate
    certificate: Certificate
    chain: list[Certificate]
    bundle: list[Certificate]
    scope: TLSScope


class TLSContext(RelationState):
    """State collection metadata for TLS credentials."""

    def __init__(
        self, relation: Relation | None, data_interface: Data, component: Unit, scope: TLSScope
    ):
        self.scope = scope
        super().__init__(relation, data_interface, component)

    @property
    def private_key(self) -> PrivateKey | None:
        """The unit private-key set during `certificates_joined`.

        Returns:
            String of key contents
            Empty if key not yet generated
        """
        raw = self.relation_data.get(f"{self.scope.value}-private-key-secret", "")
        if not raw:
            return None
        return PrivateKey.from_string(raw)

    @private_key.setter
    def private_key(self, value: PrivateKey | None) -> None:
        if not value:
            return self._field_setter_wrapper(f"{self.scope.value}-private-key-secret", "")
        self._field_setter_wrapper(f"{self.scope.value}-private-key-secret", value.raw)

    @property
    def csr(self) -> CertificateSigningRequest | None:
        """The unit cert signing request.

        Returns:
            String of csr contents
            Empty if csr not yet generated
        """
        raw = self.relation_data.get(f"{self.scope.value}-csr-secret", "")
        if not raw:
            return None
        return CertificateSigningRequest.from_string(raw)

    @csr.setter
    def csr(self, value: CertificateSigningRequest | None) -> None:
        if not value:
            return self._field_setter_wrapper(f"{self.scope.value}-csr-secret", "")
        self._field_setter_wrapper(f"{self.scope.value}-csr-secret", value.raw)

    @property
    def certificate(self) -> Certificate | None:
        """The signed unit certificate from the provider relation."""
        raw = self.relation_data.get(f"{self.scope.value}-certificate-secret", "")
        if not raw:
            return None
        return Certificate.from_string(raw)

    @certificate.setter
    def certificate(self, value: Certificate | None) -> None:
        if not value:
            return self._field_setter_wrapper(f"{self.scope.value}-certificate-secret", "")
        self._field_setter_wrapper(f"{self.scope.value}-certificate-secret", value.raw)

    @property
    def ca(self) -> Certificate | None:
        """The ca used to sign unit cert.

        Returns:
            String of ca contents in PEM format
            Empty if cert not yet generated/signed
        """
        # defaults to ca for backwards compatibility after field change introduced with secrets
        raw = self.relation_data.get(f"{self.scope.value}-ca-cert-secret", "")
        if not raw:
            return None

        return Certificate.from_string(raw)

    @ca.setter
    def ca(self, value: Certificate | None) -> None:
        if not value:
            return self._field_setter_wrapper(f"{self.scope.value}-ca-cert-secret", "")
        self._field_setter_wrapper(f"{self.scope.value}-ca-cert-secret", value.raw)

    @property
    def chain(self) -> list[Certificate]:
        """The chain used to sign the unit cert."""
        raw = self.relation_data.get(f"{self.scope.value}-chain-secret")
        if not raw:
            return []
        return [Certificate.from_string(c) for c in json.loads(raw)]

    @chain.setter
    def chain(self, value: list[Certificate]) -> None:
        """Set the chain used to sign the unit cert."""
        if len(value) == 0:
            return self._field_setter_wrapper(f"{self.scope.value}-chain-secret", "")
        self._field_setter_wrapper(
            f"{self.scope.value}-chain-secret", json.dumps([str(c) for c in value])
        )

    @property
    def bundle(self) -> list[Certificate]:
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

    @property
    def resolved(self) -> ResolvedTLSContext:
        """Return a ResolvedTLSContext if all required TLS fields are present.

        Else raise RuntimeError.
        """
        if not self.certificate or not self.private_key or not self.ca:
            raise RuntimeError("TLS state is incomplete")
        return ResolvedTLSContext(
            private_key=self.private_key,
            ca=self.ca,
            certificate=self.certificate,
            chain=self.chain,
            bundle=self.bundle,
            scope=self.scope,
        )


class UnitContext(RelationState):
    """Unit context of the application state.

    Provides mappings for the unit data bag of peer relation.
    """

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerUnitData,
        component: Unit,
        seeds: set[str],
    ):
        super().__init__(relation, data_interface, component)
        self.unit = component
        self.seeds = seeds

    @property
    def unit_id(self) -> int:
        """The id of the unit from the unit name."""
        return int(self.unit.name.split("/")[1])

    @property
    def unit_name(self) -> str:
        """Unit name."""
        return self.unit.name

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
        """The internode connection endpoint for the cassandra server from unit IP."""
        return f"{self.ip}:{CASSANDRA_PEER_PORT}"

    @property
    def client_url(self) -> str:
        """The client connection endpoint for the cassandra server from unit IP."""
        return f"{self.ip}:{CASSANDRA_CLIENT_PORT}"

    @property
    def workload_state(self) -> UnitWorkloadState:
        """Current state of the Cassandra workload."""
        return self.relation_data.get("workload_state", UnitWorkloadState.INSTALLING)

    @workload_state.setter
    def workload_state(self, value: UnitWorkloadState) -> None:
        self._field_setter_wrapper("workload_state", value.value)

    # --- TLS ---
    @property
    def peer_tls(self) -> TLSContext:
        """TLS state for internal (peer) communications."""
        return TLSContext(self.relation, self.data_interface, self.unit, TLSScope.PEER)

    @property
    def client_tls(self) -> TLSContext:
        """TLS state for external (client) communications."""
        return TLSContext(self.relation, self.data_interface, self.unit, TLSScope.CLIENT)

    @property
    def keystore_password(self) -> str:
        """Get keystore password.

        Returns:
            String of password
            None if password not yet generated
        """
        return self.relation_data.get("keystore-password-secret", "")

    @property
    def truststore_password(self) -> str:
        """Get truststore password.

        Returns:
            String of password
            None if password not yet generated
        """
        return self.relation_data.get("truststore-password-secret", "")

    @keystore_password.setter
    def keystore_password(self, value: str) -> None:
        """Set keystore password.

        Returns:
            String of password
            None if password not yet generated
        """
        self._field_setter_wrapper("keystore-password-secret", value)

    @truststore_password.setter
    def truststore_password(self, value: str) -> None:
        """Set truststore password.

        Returns:
            String of password
            None if password not yet generated
        """
        self._field_setter_wrapper("truststore-password-secret", value)

    @property
    def is_seed(self) -> bool:
        """Whether this unit's `peer_url` present in `ClusterContext.seeds`."""
        return self.peer_url in self.seeds

    @property
    def is_ready(self) -> bool:
        """Whether this unit already proceeded with startup phase.

        This can help to determine whether the event can be skipped
        if it will be reconciled during startup anyway.
        """
        return self.workload_state != UnitWorkloadState.INSTALLING

    @property
    def is_config_change_eligible(self) -> bool:
        """Whether it's safe to modify Cassandra service configuration.

        WAITING_FOR_START, CANT_START and ACTIVE workload states are considered safe.
        See UnitWorkloadState documentation for more information.
        """
        return self.workload_state in [
            UnitWorkloadState.WAITING_FOR_START,
            UnitWorkloadState.CANT_START,
            UnitWorkloadState.ACTIVE,
        ]

    @property
    def is_operational(self) -> bool:
        """Whether this unit's workload is active."""
        return self.workload_state == UnitWorkloadState.ACTIVE


class ClusterContext(RelationState):
    """Cluster context of the application state.

    Provides mappings for the application data bag of peer relation.
    """

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerData,
        component: Application,
    ):
        super().__init__(relation, data_interface, component)
        self.app = component

    @property
    def seeds(self) -> set[str]:
        """Set of peer urls of Cassandra seed nodes.

        When achievable, it's recommended to use `ApplicationState.seed_units` over this raw value.
        """
        seeds = self.relation_data.get("seeds", "")
        return set(seeds.split(",")) if seeds else set()

    @seeds.setter
    def seeds(self, value: set[str]) -> None:
        self._field_setter_wrapper("seeds", ",".join(value))

    @property
    def state(self) -> ClusterState:
        """Current state of the Cassandra cluster."""
        return self.relation_data.get("cluster_state", ClusterState.UNKNOWN)

    @state.setter
    def state(self, value: ClusterState) -> None:
        self._field_setter_wrapper("cluster_state", value.value)

    @property
    def is_active(self) -> bool:
        """Whether Cassandra cluster state is `ACTIVE`."""
        return self.state == ClusterState.ACTIVE

    @property
    def tls_state(self) -> TLSState:
        """Current state of the Cassandra cluster."""
        return TLSState(self.relation_data.get("tls_state", TLSState.UNKNOWN))

    @tls_state.setter
    def tls_state(self, value: TLSState) -> None:
        self._field_setter_wrapper("tls_state", value.value)

    # --- TLS ---
    @property
    def internal_ca(self) -> Certificate | None:
        """The internal CA certificate used for the peer relations."""
        ca = self.relation_data.get("internal-ca-secret", "")
        ca_key = self.internal_ca_key

        if ca_key is None or not ca:
            return None

        return Certificate.from_string(ca)

    @internal_ca.setter
    def internal_ca(self, value: Certificate) -> None:
        self._field_setter_wrapper("internal-ca-secret", str(value))

    @property
    def internal_ca_key(self) -> PrivateKey | None:
        """The private key of internal CA certificate used for the peer relations."""
        if not (ca_key := self.relation_data.get("internal-ca-key-secret", "")):
            return None

        return PrivateKey.from_string(ca_key)

    @internal_ca_key.setter
    def internal_ca_key(self, value: PrivateKey) -> None:
        self._field_setter_wrapper("internal-ca-key-secret", str(value))

    @property
    def operator_password_secret(self) -> str:
        """Password of `cassandra` system user."""
        return self.relation_data.get("operator-password", "")

    @operator_password_secret.setter
    def operator_password_secret(self, value: str) -> None:
        self._field_setter_wrapper("operator-password", value)


class ApplicationState(Object):
    """Mappings for the charm relations that forms global application state."""

    def __init__(self, charm: CharmBase):
        super().__init__(parent=charm, key="charm_state")
        self.peer_app_interface = DataPeerData(
            self.model,
            relation_name=PEER_RELATION,
            additional_secret_fields=SECRETS_APP,
        )

        self.peer_unit_interface = DataPeerUnitData(
            self.model,
            relation_name=PEER_RELATION,
            additional_secret_fields=SECRETS_UNIT,
        )

        # TODO: remove when data platform bug is fixed:
        # https://github.com/canonical/data-platform-libs/issues/243#issue-3527889114
        if os.getenv("JUJU_HOOK_NAME", "") == f"{DATA_STORAGE}-storage-detaching":
            self.peer_unit_interface = DataPeerUnitData(
                self.model,
                relation_name=PEER_RELATION,
            )

    @property
    def peer_relation(self) -> Relation | None:
        """Cluster peer relation."""
        return self.model.get_relation(PEER_RELATION)

    @property
    def peer_relation_units(self) -> dict[Unit, DataPeerOtherUnitData]:
        """Unit data interface of all units in the cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        return {
            unit: DataPeerOtherUnitData(model=self.model, unit=unit, relation_name=PEER_RELATION)
            for unit in self.peer_relation.units
        }

    @property
    def cluster(self) -> ClusterContext:
        """Cluster context."""
        return ClusterContext(
            relation=self.peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
        )

    @property
    def unit(self) -> UnitContext:
        """This unit context."""
        return UnitContext(
            relation=self.peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
            seeds=self.cluster.seeds,
        )

    @property
    def units(self) -> set[UnitContext]:
        """Contexts of all the units in the cluster peer relation, including this unit itself."""
        return {self.unit, *self.other_units}

    @property
    def other_units(self) -> set[UnitContext]:
        """Contexts of other units in the cluster peer relation."""
        return {
            UnitContext(
                relation=self.peer_relation,
                data_interface=data_interface,
                component=unit,
                seeds=self.cluster.seeds,
            )
            for unit, data_interface in self.peer_relation_units.items()
        }

    @property
    def seed_units(self) -> set[UnitContext]:
        """Contexts of all the units, that are configured as seed nodes.

        See `ApplicationState.units` for more info.
        """
        return {unit for unit in self.units if unit.is_seed}

    @seed_units.setter
    def seed_units(self, value: set[UnitContext] | UnitContext):
        self.cluster.seeds = (
            {value.peer_url}
            if isinstance(value, UnitContext)
            else {unit.peer_url for unit in value}
        )

    @property
    def other_seed_units(self) -> set[UnitContext]:
        """Contexts of other units, that are configured as seed nodes.

        See `ApplicationState.other_units` for more info.
        """
        return {unit for unit in self.other_units if unit.is_seed}
