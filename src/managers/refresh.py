# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Apache Cassandra refresh."""

import logging

import charm_refresh

logger = logging.getLogger(__name__)


class RefreshManager(charm_refresh.Machines):
    """Extended charm_refresh.Machines that allows None initialization and custom methods."""

    def __init__(self, refresh_specific: charm_refresh.CharmSpecificMachines):
        """Initialize the manager optionally using an existing refresh object."""
        try:
            refresh = charm_refresh.Machines(refresh_specific)
            self.__dict__.update(refresh.__dict__)
            self._refresh_initialized = True
        except (charm_refresh.PeerRelationNotReady, charm_refresh.UnitTearingDown):
            self._refresh_initialized = False

    @property
    def is_initialized(self) -> bool:
        """Return whether the refresh object has been initialized."""
        return self._refresh_initialized

    @property
    def ready(self) -> bool:
        """Return True if initialized and no refresh is currently in progress."""
        return self._refresh_initialized and not self.in_progress
