#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

from ops import CollectStatusEvent, main

from common.literals import CharmConfig, Status
from core.charm import CassandraCharmBase
from events.cassandra import CassandraEvents
from workload import CassandraWorkload

logger = logging.getLogger(__name__)


class CassandraOperatorCharm(CassandraCharmBase):
    """Charm the application."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(CassandraWorkload(), *args)

        self.cassandra_events = CassandraEvents(self)

        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_status)

    def _on_collect_status(self, event: CollectStatusEvent) -> None:
        if self.app.planned_units() == 0:
            event.add_status(Status.REMOVED.value.status)
            return

        # compute cluster status
        for status in self.cluster_manager.compute_component_status():
            event.add_status(status.value.status)

        # add all other statuses collected during the current hook
        for status in self.pending_inactive_statuses + [Status.ACTIVE]:
            event.add_status(status.value.status)


if __name__ == "__main__":  # pragma: nocover
    main(CassandraOperatorCharm)
