#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
from typing import Dict, Set

from charms.data_platform_libs.v0.data_interfaces import (
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
)
from ops import CharmBase, Object, Relation, Unit

from common.literals import (
    PEER_RELATION,
)
from core.context import ClusterContext, UnitContext

logger = logging.getLogger(__name__)


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
    def peer_units_data_interfaces(self) -> Dict[Unit, DataPeerOtherUnitData]:
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
    def nodes(self) -> Set[UnitContext]:
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
