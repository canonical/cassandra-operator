# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Apache Cassandra refresh."""

import abc
import dataclasses
import logging

import charm_refresh
from charms.operator_libs_linux.v2 import snap

from core.state import ApplicationState
from core.workload import WorkloadBase
from managers.node import NodeManager

logger = logging.getLogger(__name__)


@dataclasses.dataclass(eq=False)
class Refresh(charm_refresh.CharmSpecificCommon, abc.ABC):
    """Base class for Apache Cassandra refresh operations."""

    _state: ApplicationState
    _workload: WorkloadBase
    _node_manager: NodeManager

    @classmethod
    def is_compatible(
        cls,
        *,
        old_charm_version: charm_refresh.CharmVersion,
        new_charm_version: charm_refresh.CharmVersion,
        old_workload_version: str,
        new_workload_version: str,
    ) -> bool:
        """Check charm and workload version compatibility."""
        return super().is_compatible(
            old_charm_version=old_charm_version,
            new_charm_version=new_charm_version,
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        )

    def run_pre_refresh_checks_after_1_unit_refreshed(self) -> None:  # type: ignore
        """Run pre-refresh checks after the first unit has been refreshed."""
        logger.debug("Running pre-refresh checks")

        if any(
            [
                self._state.unit.peer_tls.rotation,
                self._state.unit.client_tls.rotation,
            ]
        ):
            raise charm_refresh.PrecheckFailed("TLS CA rotation is in progress")

        for host in [u.ip for u in self._state.units]:
            if not self._node_manager.is_healthy(host, retry=True):
                raise charm_refresh.PrecheckFailed("Cluster is not healthy")


@dataclasses.dataclass(eq=False)
class MachinesRefresh(Refresh, charm_refresh.CharmSpecificMachines):  # type: ignore
    """Refresh handler for Cassandra charm on machines substrate."""

    def refresh_snap(
        self,
        *,
        snap_name: str,
        snap_revision: str,
        refresh: charm_refresh.Machines,
    ) -> None:
        """Refresh the snap package, restart Cassandra, and handle failures."""
        self._node_manager.prepare_shutdown()
        self._workload.stop()

        revision_before_refresh = snap.SnapCache()[snap_name].revision
        assert snap_revision != revision_before_refresh
        try:
            self._workload.install()
        except snap.SnapError as e:
            logger.exception(f"Snap refresh failed: {e}")

            revision_after_refresh = snap.SnapCache()[snap_name].revision

            if revision_after_refresh == revision_before_refresh:
                self._workload.start()
            else:
                refresh.update_snap_revision()
            raise
        else:
            refresh.update_snap_revision()

        logger.info(f"Upgrading {snap_name} service...")
        self._workload.restart()

        self.post_snap_refresh(refresh)

    def post_snap_refresh(self, refresh: charm_refresh.Machines) -> None:
        """Perform health checks after a snap refresh."""
        logger.debug("Running post-snap-refresh check...")
        if not self._node_manager.is_healthy(self._state.unit.ip, retry=True):
            logger.warning(
                "Post-snap-refresh check timed out."
                "Node is unhealthy. Next unit is not allowed to refresh."
            )
            return

        refresh.next_unit_allowed_to_refresh = True
