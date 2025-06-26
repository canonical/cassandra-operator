# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import ops
from ops import testing
from unittest.mock import patch, PropertyMock
from charm import CassandraCharm
from core.state import PEER_RELATION


def test_start_maintenance_status_when_starting():
    """Charm enters MaintenanceStatus when Cassandra is not yet healthy."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state_in = testing.State(leader=True, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart"),
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert state_out.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        assert state_out.get_relation(1).local_unit_data.get("workload_state") == 'active'
        assert state_out.get_relation(1).local_app_data.get("cluster_state") == 'active'

def test_start_sets_active_status_when_healthy():
    """Charm enters ActiveStatus when Cassandra is healthy after start."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state_in = testing.State(leader=True, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart"),
        patch("managers.cluster.ClusterManager.is_healthy", new_callable=PropertyMock) as is_healthy,
    ):
        is_healthy.return_value = True
        state_out = ctx.run(ctx.on.start(), state_in)
        assert state_out.unit_status == ops.ActiveStatus()
        assert state_out.get_relation(1).local_unit_data.get("workload_state") == 'active'
        assert state_out.get_relation(1).local_app_data.get("cluster_state") == 'active'

def test_start_only_after_leader_active():
    """Ensure Cassandra node starts only after leader is active and cluster_state is 'active'."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state_in = testing.State(leader=False, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart") as restart,
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert state_out.unit_status == ops.MaintenanceStatus("installing Cassandra")
        restart.assert_not_called()

    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION, local_app_data={"cluster_state": "active"})
    state_in = testing.State(leader=False, relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart") as restart,
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert state_out.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        assert state_out.get_relation(1).local_unit_data.get("workload_state") == 'active'
        restart.assert_called()

def test_config_changed_invalid_config():
    """Ensure charm enters BlockedStatus if config is invalid during config_changed event."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state_in = testing.State(leader=True, relations={relation}, config={'profile': 'invalid'})
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart"),
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        assert state_out.unit_status == ops.BlockedStatus("invalid config")

def test_config_changed_no_restart():
    """Ensure node is not restarted if workload_state is 'starting' during config_changed event."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION, local_unit_data={"workload_state": "starting"})
    state_in = testing.State(leader=True, relations={relation})
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart") as restart,
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        assert state_out.unit_status == ops.MaintenanceStatus("waiting for Cassandra to start")
        restart.assert_not_called()

def test_collect_unit_status_active_but_not_healthy():
    """Ensure unit status is MaintenanceStatus if workload_state is 'active' but node is not healthy (is_healthy=False)."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(
        id=1, 
        endpoint=PEER_RELATION,
        local_unit_data={"workload_state": "active"},
)
    state_in = testing.State(leader=True, relations={relation})
    with (
        patch("managers.cluster.ClusterManager.is_healthy", new_callable=PropertyMock) as is_healthy,
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart"),
    ):
        is_healthy.return_value = False
        state_out = ctx.run(ctx.on.collect_unit_status(), state_in)
        assert state_out.unit_status == ops.MaintenanceStatus('waiting for Cassandra to start')


def test_start_not_leader_and_cluster_state_not_active():
    """Ensure charm does not start Cassandra if not leader and cluster_state is not 'active'."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION, local_app_data={"cluster_state": "pending"})
    state_in = testing.State(leader=False, relations={relation})
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("workload.CassandraWorkload.restart") as restart,
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        assert state_out.unit_status == ops.MaintenanceStatus("installing Cassandra")
        restart.assert_not_called()


