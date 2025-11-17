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
