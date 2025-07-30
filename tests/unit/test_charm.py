# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from unittest.mock import PropertyMock, patch

import ops
from ops import testing

from charm import CassandraCharm
from core.state import PEER_RELATION

BOOTSTRAP_RELATION = "bootstrap"
PEER_SECRET = "cassandra-peers.cassandra.app"


def test_start_change_password():
    """Leader should generate & configure cassandra password."""
    ctx = testing.Context(CassandraCharm)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    state = testing.State(leader=True, relations={peer_relation, bootstrap_relation})

    with (
        patch("managers.config.ConfigManager.render_env") as render_env,
        patch("managers.config.ConfigManager.render_cassandra_config") as render_cassandra_config,
        patch(
            "managers.database.DatabaseManager.update_system_user_password"
        ) as update_system_user_password,
        patch("charm.CassandraWorkload") as workload,
        patch("managers.tls.TLSManager.configure"),
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=True),
        ),
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.start(), state)
        render_env.assert_called()
        render_cassandra_config.assert_called_once()
        assert render_cassandra_config.call_args.kwargs["authentication"] is False
        workload.return_value.start.assert_called_once()
        assert len(state.deferred) == 1 and state.deferred[0].observer == "_on_start"

        state = testing.State(
            leader=True,
            relations=state.relations,
            secrets=state.secrets,
        )

        render_cassandra_config.reset_mock()
        state = ctx.run(ctx.on.start(), state)
        update_system_user_password.assert_called_once_with("cassandra", "password")
        assert render_cassandra_config.call_args.kwargs["authentication"] is True
        workload.return_value.restart.assert_called()
        assert len(state.deferred) == 0


def test_start_subordinate_only_after_leader_active():
    """Subordinate should start only after leader initialized cluster."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraCharm.setup_internal_certificates", return_value=True),
        patch("charm.CassandraWorkload") as workload,
        patch(
            "charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock", autospec=True
        ) as bootstrap,
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.start(), state)
        bootstrap.assert_not_called()

        relation = testing.PeerRelation(
            id=1, endpoint=PEER_RELATION, local_app_data={"cluster_state": "active"}
        )
        state = testing.State(relations={relation})

        state = ctx.run(ctx.on.start(), state)
        bootstrap.assert_called_once()


def test_start_invalid_config():
    """Both leader and subordinate should wait for config to be fixed prior starting workload."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(leader=True, relations={relation}, config={"profile": "invalid"})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraCharm.setup_internal_certificates", return_value=True),
        patch("charm.CassandraWorkload") as workload,
        patch(
            "charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock", autospec=True
        ) as bootstrap,
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.start(), state)
        workload.return_value.restart.assert_not_called()

        state = testing.State(leader=True, relations={relation})

        state = ctx.run(ctx.on.start(), state)
        bootstrap.assert_not_called()


def test_config_changed_invalid_config():
    """Charm should enter BlockedStatus if config is invalid during config_changed event."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(leader=True, relations={relation}, config={"profile": "invalid"})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("charm.CassandraCharm.setup_internal_certificates", return_value=True),
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.config_changed(), state)
        assert state.unit_status == ops.BlockedStatus("invalid config")


def test_config_changed():
    """Charm should restart workload only if it's active when config is changed."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    state = testing.State(leader=True, relations={relation, bootstrap_relation})
    with (
        patch("managers.config.ConfigManager.render_env") as render_env,
        patch("managers.config.ConfigManager.render_cassandra_config") as render_cassandra_config,
        patch("managers.database.DatabaseManager.update_system_user_password"),
        patch("charm.CassandraWorkload") as workload,
        patch("charm.CassandraCharm.setup_internal_certificates", return_value=True),
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=True),
        ),
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.config_changed(), state)
        render_env.assert_not_called()
        render_cassandra_config.assert_not_called()
        workload.return_value.restart.assert_not_called()

        relation = testing.PeerRelation(
            id=1, endpoint=PEER_RELATION, local_unit_data={"workload_state": "active"}
        )
        state = testing.State(leader=True, relations={relation, bootstrap_relation})

        render_env.reset_mock()
        state = ctx.run(ctx.on.config_changed(), state)
        render_env.assert_called()
        render_cassandra_config.assert_called()
        workload.return_value.restart.assert_called_once()
