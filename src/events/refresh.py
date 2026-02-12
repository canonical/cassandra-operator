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

        if not self._workload.installed:
            raise charm_refresh.PrecheckFailed("Current unit workload is not installed")

        for unit in self._state.units:
            if not unit.is_ready:
                continue

            if not unit.is_operational:
                raise charm_refresh.PrecheckFailed(f"Unit {unit.hostname} is not operational")
            if unit.peer_tls.rotation or unit.client_tls.rotation:
                raise charm_refresh.PrecheckFailed(
                    f"TLS CA rotation is in progress for unit {unit.hostname}"
                )
            if not self._node_manager.is_healthy(unit.ip, retry=True, timeout=300):
                raise charm_refresh.PrecheckFailed(f"Unit {unit.hostname} is not healthy")


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
        revision_before_refresh = snap.SnapCache()[snap_name].revision
        if snap_revision == revision_before_refresh:
            refresh.next_unit_allowed_to_refresh = True
            return

        if self._workload.is_alive:
            self._node_manager.prepare_shutdown()
            self._workload.stop()

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
        if self._state.unit.is_ready:
            self._workload.restart()
        self.post_snap_refresh(refresh)

    def post_snap_refresh(self, refresh: charm_refresh.Machines) -> None:
        """Perform health checks after a snap refresh."""
        logger.debug("Running post-snap-refresh check...")
        if not self._state.unit.is_ready:
            # Worklaod
            refresh.next_unit_allowed_to_refresh = True
            return
        if not self._node_manager.is_healthy(self._state.unit.ip, retry=True):
            logger.warning(
                "Post-snap-refresh check timed out."
                "Node is unhealthy. Next unit is not allowed to refresh."
            )
            return

        refresh.next_unit_allowed_to_refresh = True
