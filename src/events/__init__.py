#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handler definitions."""

import abc


class Reconciliable(abc.ABC):
    """Mixin ABC for reconciliable event handlers."""

    def reconcile(self) -> None:
        """Reconcile this event handler."""
        pass
