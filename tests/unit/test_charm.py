# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from unittest.mock import PropertyMock, patch

import ops
from ops import testing

from charm import CassandraCharm
from core.state import PEER_RELATION


def test_start_maintenance_status():
    """Charm enters MaintenanceStatus when Cassandra is not yet healthy."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(leader=True, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraWorkload"),
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=False),
        ),
    ):
        state = ctx.run(ctx.on.start(), state)
        assert state.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        assert state.get_relation(1).local_unit_data.get("workload_state") == "starting"
        assert not state.get_relation(1).local_app_data.get("cluster_state")

        state = ctx.run(ctx.on.update_status(), state)
        assert state.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        assert state.get_relation(1).local_unit_data.get("workload_state") == "starting"
        assert not state.get_relation(1).local_app_data.get("cluster_state")


def test_start_active_status_when_healthy():
    """Charm enters ActiveStatus when Cassandra is healthy after start."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(leader=True, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraWorkload"),
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=True),
        ),
    ):
        state = ctx.run(ctx.on.start(), state)
        assert state.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        assert state.get_relation(1).local_unit_data.get("workload_state") == "starting"
        assert not state.get_relation(1).local_app_data.get("cluster_state")

        state = ctx.run(ctx.on.update_status(), state)
        assert state.unit_status == ops.ActiveStatus()
        assert state.get_relation(1).local_unit_data.get("workload_state") == "active"
        assert state.get_relation(1).local_app_data.get("cluster_state") == "active"


def test_start_only_after_leader_active():
    """Ensure Cassandra node starts only after leader is active and cluster_state is 'active'."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(leader=False, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraWorkload") as workload,
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=False),
        ),
    ):
        state = ctx.run(ctx.on.start(), state)
        assert state.unit_status == ops.WaitingStatus("waiting for cluster to start")
        assert state.get_relation(1).local_unit_data.get("workload_state") == "waiting_for_start"
        workload.return_value.start.assert_not_called()

    relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_app_data={"cluster_state": "active"}
    )
    bootstrap_relation = testing.PeerRelation(id=2, endpoint="bootstrap")
    state = testing.State(leader=False, relations={relation, bootstrap_relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraWorkload"),
        patch(
            "charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock", autospec=True
        ) as bootstrap,
    ):
        state = ctx.run(ctx.on.start(), state)
        assert state.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        assert state.get_relation(1).local_unit_data.get("workload_state") == "starting"
        bootstrap.assert_called_once()


def test_config_changed_invalid_config():
    """Ensure charm enters BlockedStatus if config is invalid during config_changed event."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(leader=True, relations={relation}, config={"profile": "invalid"})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraWorkload"),
    ):
        state = ctx.run(ctx.on.config_changed(), state)
        assert state.unit_status == ops.BlockedStatus("invalid config")


def test_config_changed_no_restart():
    """Ensure node is not restarted if workload_state is 'starting' during config_changed event."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"workload_state": "starting"}
    )
    state = testing.State(leader=True, relations={relation})
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraWorkload") as workload,
    ):
        state = ctx.run(ctx.on.config_changed(), state)
        assert state.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        workload.return_value.restart.assert_not_called()
