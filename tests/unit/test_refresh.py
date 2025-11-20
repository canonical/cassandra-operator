#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import MagicMock, patch

import pytest
from charm_refresh import PrecheckFailed
from ops import BlockedStatus, testing

from charm import CassandraCharm
from core.state import (
    PEER_RELATION,
)
from src.events.refresh import MachinesRefresh

from .helpers import make_refresh_like


@pytest.mark.parametrize(
    "helthy, unit_data, pre_check_result",
    [
        (True, {"client-rotation": "true"}, "TLS CA rotation is in progress"),
        (True, {"peer-rotation": "true"}, "TLS CA rotation is in progress"),
        (False, {"ip": "1.1.1.1"}, "Cluster is not healthy"),
    ],
)
def test_pre_refresh_checks(helthy, unit_data, pre_check_result) -> None:
    ctx = testing.Context(CassandraCharm)

    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={},
        local_unit_data=unit_data,
    )

    state_in = testing.State(relations={peer_relation})

    with ctx(ctx.on.relation_changed(relation=peer_relation), state_in) as manager:
        charm: CassandraCharm = manager.charm

        with (
            patch("events.refresh.MachinesRefresh.__init__", return_value=None),
            patch("managers.node.NodeManager") as node_manager,
        ):
            node_manager.is_healthy.return_value = helthy
            refresh = MachinesRefresh.__new__(MachinesRefresh)
            refresh._state = charm.state
            refresh._node_manager = node_manager

            with pytest.raises(PrecheckFailed) as e:
                refresh.run_pre_refresh_checks_after_1_unit_refreshed()

            assert str(e.value) == pre_check_result


@pytest.mark.parametrize("helthy", [True, False])
def test_snap_refresh(helthy) -> None:
    ctx = testing.Context(CassandraCharm)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)

    state_in = testing.State(relations={peer_relation})

    with patch("workload.snap.SnapCache"):
        with ctx(ctx.on.relation_changed(relation=peer_relation), state_in) as manager:
            mock_refresh = MagicMock()
            mock_refresh.next_unit_allowed_to_refresh = False
            charm: CassandraCharm = manager.charm

            # Mock the refresh constructor to avoid version checks
            with (
                patch("events.refresh.MachinesRefresh.__init__", return_value=None),
                patch("managers.node.NodeManager") as node_manager,
                patch("charm.CassandraWorkload") as workload,
            ):
                refresh = MachinesRefresh.__new__(MachinesRefresh)
                node_manager.is_healthy.return_value = helthy
                refresh._state = charm.state
                refresh._node_manager = node_manager
                refresh._workload = workload

                refresh.refresh_snap(
                    snap_name="charmed-cassandra", snap_revision="124", refresh=mock_refresh
                )

            assert (
                mock_refresh.next_unit_allowed_to_refresh
                if helthy
                else not mock_refresh.next_unit_allowed_to_refresh
            )


def test_statuses() -> None:
    ctx = testing.Context(CassandraCharm)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)

    state_in = testing.State(relations={peer_relation}, leader=True)

    # higher app status
    refresh_mock = make_refresh_like()
    refresh_mock.app_status_higher_priority = BlockedStatus("123")

    refresh_manager_mock = make_refresh_like()
    refresh_manager_mock.app_status_higher_priority = BlockedStatus("123")
    with (
        patch("charm_refresh.Machines", MagicMock(return_value=refresh_mock)),
        patch("charm.RefreshManager", MagicMock(return_value=refresh_manager_mock)),
        patch("workload.snap.SnapCache"),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

        assert state_out.app_status == BlockedStatus("123")
        assert state_out.unit_status != BlockedStatus("123")

    # higher unit status
    refresh_mock = MagicMock()
    refresh_mock.unit_status_higher_priority = BlockedStatus("456")

    refresh_manager_mock = make_refresh_like()
    refresh_manager_mock.unit_status_higher_priority = BlockedStatus("456")
    with (
        patch("charm_refresh.Machines", MagicMock(return_value=refresh_mock)),
        patch("charm.RefreshManager", MagicMock(return_value=refresh_manager_mock)),
        patch("workload.snap.SnapCache"),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

        assert state_out.unit_status == BlockedStatus("456")

    # lower unit status
    refresh_mock = MagicMock()
    refresh_mock.unit_status_lower_priority.return_value = BlockedStatus("789")

    refresh_manager_mock = make_refresh_like()
    refresh_manager_mock.unit_status_lower_priority.return_value = BlockedStatus("789")
    with (
        patch("charm_refresh.Machines", MagicMock(return_value=refresh_mock)),
        patch("charm.RefreshManager", MagicMock(return_value=refresh_manager_mock)),
        patch("workload.snap.SnapCache"),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

        assert state_out.unit_status == BlockedStatus("789")
