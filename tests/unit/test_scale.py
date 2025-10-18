# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import scenario
from ops import testing

from charm import CassandraCharm
from common.exceptions import ExecError
from core.state import DATA_STORAGE, PEER_RELATION


def make_state(storage: testing.Storage, leader: bool = True):
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    return testing.State(leader=leader, relations={relation}, storages=frozenset([storage]))


def test_storage_detaching_cluster_unhealthy(caplog):
    """Charm should fail decommission if cluster is unhealthy."""
    ctx = testing.Context(CassandraCharm)
    storage = testing.Storage(name=DATA_STORAGE, index=0)
    state = make_state(storage)

    with (
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.is_healthy", return_value=False),
        patch(
            "managers.node.NodeManager.is_bootstrap_decommissioning",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.decommission") as decommission,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.generate_password.return_value = "password"

        with pytest.raises(Exception, match="Cluster is not healthy"):
            ctx.run(ctx.on.storage_detaching(storage), state)
        decommission.assert_not_called()


def test_storage_detaching_multiple_units_removal_logs_warning(caplog):
    """Charm should log a warning if more than one unit planned for removal."""
    ctx = testing.Context(CassandraCharm)
    storage = testing.Storage(name=DATA_STORAGE, index=0)
    state = make_state(storage)

    with (
        patch("charm.CassandraWorkload") as workload,
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.is_healthy", return_value=True),
        patch(
            "managers.node.NodeManager.is_bootstrap_decommissioning",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.decommission", return_value=None) as decommission,
        patch("ops.model.Application.planned_units", return_value=1),
        patch(
            "core.state.ApplicationState.units",
            new_callable=PropertyMock(return_value=[MagicMock(), MagicMock(), MagicMock()]),
        ),
    ):
        workload.return_value.generate_password.return_value = "password"

        with caplog.at_level(logging.WARNING):
            ctx.run(ctx.on.storage_detaching(storage), state)

        assert "More than one unit removing" in caplog.text
        decommission.assert_called_once()


def test_storage_detaching_success(caplog):
    """Charm should call decommission and log success message."""
    ctx = testing.Context(CassandraCharm)
    storage = testing.Storage(name=DATA_STORAGE, index=0)
    state = make_state(storage)

    with (
        patch("charm.CassandraWorkload") as workload,
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.is_healthy", return_value=True),
        patch(
            "managers.node.NodeManager.is_bootstrap_decommissioning",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.decommission", return_value=None) as decommission,
        patch("ops.model.Application.planned_units", return_value=2),
        patch("core.state.ApplicationState.units", new_callable=PropertyMock) as units,
    ):
        workload.return_value.generate_password.return_value = "password"

        units.return_value = [MagicMock(), MagicMock(), MagicMock()]

        with caplog.at_level(logging.INFO):
            ctx.run(ctx.on.storage_detaching(storage), state)

        decommission.assert_called_once()
        assert "node decommissioning" in caplog.text
        assert "storage-detaching event completed" in caplog.text


def test_storage_detaching_decommission_fails(caplog):
    """Charm should log failure if decommission fails with ExecError."""
    ctx = testing.Context(CassandraCharm)
    storage = testing.Storage(name=DATA_STORAGE, index=0)
    state = make_state(storage)

    with (
        patch("charm.CassandraWorkload") as workload,
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch(
            "managers.node.NodeManager.is_bootstrap_decommissioning",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.node.NodeManager.is_healthy", return_value=True),
        patch("ops.model.Application.planned_units", return_value=1),
        patch("core.state.ApplicationState.units", new_callable=PropertyMock) as units,
        patch(
            "managers.node.NodeManager.decommission",
            side_effect=ExecError(stdout="", stderr="error"),
        ),
    ):
        workload.return_value.generate_password.return_value = "password"

        units.return_value = [MagicMock()]
        with pytest.raises(scenario.errors.UncaughtCharmError) as e:
            ctx.run(ctx.on.storage_detaching(storage), state)

        assert isinstance(e.value.__cause__, ExecError)
        assert "Failed to decommission unit" in caplog.text
