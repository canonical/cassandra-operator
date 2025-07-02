#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

from ops import Object

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from core.config import CharmConfig
from core.state import ApplicationState, ClusterState, UnitWorkloadState
from core.statuses import Status
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager

logger = logging.getLogger(__name__)

class TLSEvents(Object):

    def __init__(
        self,
        charm: TypedCharmBase[CharmConfig],
        state: ApplicationState,
        workload: WorkloadBase,
        cluster_manager: ClusterManager,
        config_manager: ConfigManager,
    ):
        super().__init__(charm, key="cassandra_events")
        self.charm = charm
        self.state = state
        self.workload = workload
    
        self.sans = cluster_manager.get_host_mapping()
        self.common_name = f"{self.charm.unit.name}-{self.charm.model.uuid}"
        
