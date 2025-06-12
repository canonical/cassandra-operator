#!/usr/bin/env python3
"""TODO."""

import logging

from ops import (
    ConfigChangedEvent,
    InstallEvent,
    Object,
    StartEvent,
)

from common.exceptions import HealthCheckFailedError
from common.literals import Status
from core.charm import CassandraCharmBase

logger = logging.getLogger(__name__)


class CassandraEvents(Object):
    """Handle all base and cassandra related events."""

    def __init__(self, charm: CassandraCharmBase):
        super().__init__(charm, key="etcd_events")
        self.charm = charm

        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.install, self._on_install)

    def _on_install(self, _: InstallEvent) -> None:
        if not self.charm.workload.install():
            self.charm.set_status(Status.SERVICE_NOT_INSTALLED)
        return

    def _on_start(self, event: StartEvent) -> None:
        # TODO: maby add functionality like in etcd (MEMBER UNITS).
        # Member units are added to cluster, but does not operate
        # within this cluster until they are bootstraped and helthy
        self.charm.config_manager.set_config_properties()

        if not self.charm.state.cluster_context.cluster_state and self.charm.unit.is_leader():
            # this unit is creating cluster
            self.charm.cluster_manager.start_node()
            try:
                self.charm.cluster_manager.broadcast_peer_url(
                    self.charm.state.unit_context.peer_url
                )
            except ValueError:
                logger.error("Failed to update member configuration")
                event.defer()
                return
        else:
            # this unit is added to cluster
            self.charm.set_status(Status.CLUSTER_NOT_JOINED)
            event.defer()
            return

        if not self.charm.workload.alive():
            self.charm.set_status(Status.SERVICE_NOT_RUNNING)
        return

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        ip_address = self.charm.cluster_manager.get_host_mapping().get("ip")
        if ip_address is not None and ip_address != self.charm.state.unit_context.ip:
            logger.info(f"New ip address: {ip_address}")
            self.charm.state.unit_context.update({"ip": ip_address})

            # update cluster configuration
            self.charm.cluster_manager.broadcast_peer_url(self.charm.state.unit_context.peer_url)
            self.charm.config_manager.set_config_properties()

            if not self.charm.cluster_manager.restart_node():
                raise HealthCheckFailedError("Failed to check health of the node")
