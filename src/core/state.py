#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

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
PEER_PORT = 7000
CLIENT_PORT = 9042

logger = logging.getLogger(__name__)


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
