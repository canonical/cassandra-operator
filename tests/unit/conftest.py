#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

from managers.ssh import SSHManager

from .helpers import make_refresh_like

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def mock_refresh():
    """Fixture for refresh logic and events."""
    refresh_mock = make_refresh_like()
    refresh_manager_mock = make_refresh_like()
    refresh_manager_mock.is_initialized = True
    refresh_manager_mock.ready = True

    fake_versions = MagicMock(charm="1.0.0", workload="5.0.0")

    logger.info("Refresh mocked")

    with (
        patch("charm_refresh.Machines", MagicMock(return_value=refresh_mock)),
        patch("charm.RefreshManager", MagicMock(return_value=refresh_manager_mock)),
        patch("charm_refresh._main._RefreshVersions", MagicMock(return_value=fake_versions)),
    ):
        yield


@pytest.fixture(autouse=True)
def mock_ssh_manager(monkeypatch):
    mock = MagicMock(spec=SSHManager)
    mock.return_value.public_key = "mykey"
    monkeypatch.setattr("charm.SSHManager", mock)


@pytest.fixture(autouse=True)
def patched_snap(monkeypatch):
    cache = MagicMock()
    snap_mock = MagicMock()
    snap_mock.services = defaultdict(default_factory=lambda _: {"active": True})
    cache.return_value = {"charmed-cassandra": snap_mock}
    with monkeypatch.context() as m:
        m.setattr("charms.operator_libs_linux.v2.snap.SnapCache", cache)
        yield
