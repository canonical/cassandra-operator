# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Apache Cassandra refresh."""

import abc
import dataclasses
import logging
from typing import Callable
import charm_refresh

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from common.exceptions import CassandraRefreshError
from core.config import CharmConfig
from core.state import ApplicationState, UnitWorkloadState
from core.workload import WorkloadBase
from managers.node import NodeManager
from charms.operator_libs_linux.v2 import snap

from tenacity import (
    RetryError,
    Retrying,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)


logger = logging.getLogger(__name__)

class RefreshManager(charm_refresh.Machines):
    """Extended charm_refresh.Machines that allows None initialization and custom methods."""

    def __init__(self, refresh: charm_refresh.Machines | None = None):
        if refresh is not None:
            self.__dict__.update(refresh.__dict__)
            self._refresh_initialized = True
        else:
            self._refresh_initialized = False            
            
    @property
    def is_initialized(self) -> bool:
        return self._refresh_initialized

    @property
    def ready(self) -> bool:
        return self._refresh_initialized and not self.in_progress

@dataclasses.dataclass(eq=False)
class Refresh(charm_refresh.CharmSpecificCommon, abc.ABC):
    """Base class for Apache Cassandra refresh operations."""

    _hosts: list[str]
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
        """Checks charm version compatibility."""
        if not super().is_compatible(
            old_charm_version=old_charm_version,
            new_charm_version=new_charm_version,
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        ):
            return False
        
        return is_workload_compatible(
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        )

    def run_pre_refresh_checks_after_1_unit_refreshed(self) -> None: # type: ignore
        """Implement pre-refresh checks after 1 unit refreshed."""
        logger.debug("Running pre-refresh checks")
        for attempt in Retrying(
                wait=wait_exponential(), stop=stop_after_delay(600), reraise=True
        ):
            with attempt:
                for host in self._hosts:
                    if not self._node_manager.is_healthy(host):
                        raise charm_refresh.PrecheckFailed("Cluster is not healthy")
            
@dataclasses.dataclass(eq=False)
class MachinesRefresh(Refresh, charm_refresh.CharmSpecificMachines): # type: ignore
    """Refresh handler for Cassandra charm on machines substrate."""
    def refresh_snap(
        self,
        *,
        snap_name: str,
        snap_revision: str,
        refresh: charm_refresh.Machines,
    ) -> None:
        self._node_manager.prepare_shutdown()
        self._workload.stop()

        revision_before_refresh = snap.SnapCache()[snap_name].revision
        assert snap_revision != revision_before_refresh
        if not self._workload.install():
            logger.exception("Snap refresh failed")

            revision_after_refresh = snap.SnapCache()[snap_name].revision

            if revision_after_refresh == revision_before_refresh:
                self._workload.start()
            else:
                refresh.update_snap_revision()
            raise CassandraRefreshError

        refresh.update_snap_revision()

        logger.info(f"Upgrading {snap_name} service...")
        self._workload.restart()

        self.post_snap_refresh(refresh)

    def post_snap_refresh(self, refresh: charm_refresh.Machines) -> None:
        logger.debug("Running post-snap-refresh check...")
        try:
            for attempt in Retrying(
                    wait=wait_exponential(), stop=stop_after_delay(600), reraise=False
            ):
                with attempt:
                    for host in self._hosts:
                        if not self._node_manager.is_healthy(host):
                            raise
        except RetryError:
            logger.warning(
            "Post-snap-refresh check timed out."
            "Some nodes may still be unhealthy. Next unit is not allowed to refresh."
            )
            return

        refresh.next_unit_allowed_to_refresh = True        
        

def is_workload_compatible(
    old_workload_version: str,
    new_workload_version: str,
) -> bool:
    """Check if the workload versions are compatible."""
    try:
        old_major, old_minor, *_ = (
            int(component) for component in old_workload_version.split(".")
        )
        new_major, new_minor, *_ = (
            int(component) for component in new_workload_version.split(".")
        )
    except ValueError:
        # Not enough values to unpack or cannot convert
        logger.info(
            "Unable to parse workload versions."
            f"Got {old_workload_version} to {new_workload_version}"
        )
        return False

    if old_major != new_major:
        logger.info(
            "Refreshing to a different major workload is not supported. "
            f"Got {old_major} to {new_major}"
        )
        return False

    if not new_minor >= old_minor:
        logger.info(
            "Downgrading to a previous minor workload is not supported. "
            f"Got {old_major}.{old_minor} to {new_major}.{new_minor}"
        )
        return False


    return True

