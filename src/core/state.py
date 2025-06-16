#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging

from charms.data_platform_libs.v0.data_interfaces import (
    Data,
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
)
from ops import Application, CharmBase, Object, Relation, Unit

from common.literals import (
    CLIENT_MGMT_PORT,
    CLIENT_PORT,
    PEER_PORT,
    PEER_RELATION,
)

logger = logging.getLogger(__name__)


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

    @property
    def ip(self) -> str:
        """The IP address for the unit."""
        return self.relation_data.get("ip", "")

    @property
    def peer_url(self) -> str:
        """The peer connection endpoint for the cassandra server."""
        return f"{self.ip}:{PEER_PORT}"

    @property
    def client_url(self) -> str:
        """The client connection endpoint for the cassandra server."""
        return f"{self.ip}:{CLIENT_PORT}"

    @property
    def client_mgmt_url(self) -> str:
        """The client mgmt connection endpoint for the cassandra server."""
        return f"{self.ip}:{CLIENT_MGMT_PORT}"

    @property
    def node_endpoint(self) -> str:
        """Concatenate node_name and peer_url."""
        return f"{self.node_name}={self.peer_url}"

    @property
    def is_started(self) -> bool:
        """Check if the unit has started."""
        return self.relation_data.get("state", "") == "started"

    # TODO: should we rename it to unit_state?
    @property
    def state(self) -> str:
        if not self.relation:
            return ""
        return self.relation_data.get("state", "")

    @state.setter
    def state(self, value: str) -> None:
        self._field_setter_wrapper("state", value)

    @ip.setter
    def ip(self, value: str) -> None:
        self._field_setter_wrapper("ip", value)


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
    def cluster_state(self) -> str:
        """The cluster state ('new' or 'existing') of the cassandra cluster."""
        return self.relation_data.get("cluster_state", "")

    @property
    def auth_enabled(self) -> bool:
        """Flag to check if authentication is already enabled in the Cluster."""
        return self.relation_data.get("authentication", "") == "enabled"

    @property
    def cluster_nodes(self) -> str:
        """Get the list of current nodes added to the cassandra cluster.

        This data is added to the peer cluster relation app databag when the first unit initializes
        the cluster on startup after deployment.
        """
        return self.relation_data.get("cluster_nodes", "")

    @cluster_state.setter
    def cluster_state(self, value: str) -> None:
        self._field_setter_wrapper("cluster_state", value)

    @cluster_nodes.setter
    def cluster_nodes(self, value: str) -> None:
        self._field_setter_wrapper("cluster_nodes", value)


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
    def unit_context(self) -> UnitContext:
        """Get the server state of this unit."""
        return UnitContext(
            relation=self.peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
        )

    @property
    def peer_units_data_interfaces(self) -> dict[Unit, DataPeerOtherUnitData]:
        """Get unit data interface of all peer units from the cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        return {
            unit: DataPeerOtherUnitData(model=self.model, unit=unit, relation_name=PEER_RELATION)
            for unit in self.peer_relation.units
        }

    @property
    def cluster_context(self) -> ClusterContext:
        """Get the cluster context of the entire cassandra application."""
        return ClusterContext(
            relation=self.peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
        )

    @property
    def nodes(self) -> set[UnitContext]:
        """Get all nodes/units in the current peer relation, including this unit itself.

        Note: This is not to be confused with the list of cluster members.

        Returns:
            Set of CassandraUnitContexts with their unit data.
        """
        if not self.peer_relation:
            return set()

        return {
            self.unit_context,
            *(
                UnitContext(
                    relation=self.peer_relation,
                    data_interface=data_interface,
                    component=unit,
                )
                for unit, data_interface in self.peer_units_data_interfaces.items()
            ),
        }
