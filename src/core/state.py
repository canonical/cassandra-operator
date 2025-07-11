#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application state definition."""

import logging
from enum import StrEnum

from charms.data_platform_libs.v0.data_interfaces import (
    Data,
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
)
from ops import Application, CharmBase, Object, Relation, Unit

PEER_RELATION = "cassandra-peers"
CASSANDRA_PEER_PORT = 7000
CASSANDRA_CLIENT_PORT = 9042

logger = logging.getLogger(__name__)


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
    """Subordinate unit is waiting for leader to initialize cluster before it starts workload."""
    STARTING = "starting"
    """Cassandra is starting."""
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


class UnitContext(RelationState):
    """Unit context of the application state.

    Provides mappings for the unit data bag of peer relation.
    """

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
    def cluster_name(self) -> str:
        """Established Cassandra cluster name."""
        return self.relation_data.get("cluster_name", "")

    @cluster_name.setter
    def cluster_name(self, value: str) -> None:
        self._field_setter_wrapper("cluster_name", value)

    @property
    def seeds(self) -> list[str]:
        """List of peer urls of Cassandra seed nodes."""
        seeds = self.relation_data.get("seeds", "")
        return seeds.split(",") if seeds else []

    @seeds.setter
    def seeds(self, value: list[str]) -> None:
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


class ApplicationState(Object):
    """Mappings for the charm relations that forms global application state."""

    def __init__(self, charm: CharmBase):
        super().__init__(parent=charm, key="charm_state")
        self.peer_app_interface = DataPeerData(
            self.model,
            relation_name=PEER_RELATION,
        )
        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=PEER_RELATION)

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
            )
            for unit, data_interface in self.peer_relation_units.items()
        }
