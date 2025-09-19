# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from unittest.mock import PropertyMock, patch

import ops
import pytest
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
        patch("managers.database.DatabaseManager.init_admin") as init_admin,
        patch("charm.CassandraWorkload") as workload,
        patch("managers.tls.TLSManager.configure"),
        patch(
            "managers.node.NodeManager.is_healthy",
            new_callable=PropertyMock(return_value=True),
        ),
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.start(), state)
        render_env.assert_called()
        render_cassandra_config.assert_called()
        workload.return_value.start.assert_called()
        init_admin.assert_called_once_with("password")
        workload.return_value.restart.assert_called()


def test_start_subordinate_only_after_leader_active():
    """Subordinate should start only after leader initialized cluster."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    state = testing.State(relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch(
            "managers.node.NodeManager.network_address", return_value=("1.1.1.1", "hostname")
        ),
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
            id=1,
            endpoint=PEER_RELATION,
            local_app_data={"cluster_state": "active", "seeds": "1.1.1.1:7000"},
            local_unit_data={"ip": "1.1.1.1"},
        )
        state = testing.State(relations={relation})

        state = ctx.run(ctx.on.start(), state)
        bootstrap.assert_called_once()


@pytest.mark.parametrize("workload_active", [True, False])
@pytest.mark.parametrize("seed_active", [True, False])
def test_start_subordinate_only_after_seed_active(workload_active: bool, seed_active: bool):
    """Subordinate should start only after seed is actually running."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_app_data={"cluster_state": "active", "seeds": "2.2.2.2:7000"},
        local_unit_data={"ip": "1.1.1.1"},
        peers_data={
            2: {
                "ip": "2.2.2.2",
                "workload_state": "active" if workload_active else "",
            }
        },
    )
    state = testing.State(relations={relation})

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch(
            "managers.node.NodeManager.network_address", return_value=("1.1.1.1", "hostname")
        ),
        patch("managers.database.DatabaseManager.check", return_value=seed_active),
        patch("charm.CassandraCharm.setup_internal_certificates", return_value=True),
        patch("charm.CassandraWorkload") as workload,
        patch(
            "charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock", autospec=True
        ) as bootstrap,
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.start(), state)
        if workload_active and seed_active:
            bootstrap.assert_called()
        else:
            bootstrap.assert_not_called()


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

        state = testing.State(leader=True, relations={relation}, config={"profile": "invalid"})

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


@pytest.mark.parametrize("env_changed", [True, False])
@pytest.mark.parametrize("cassandra_config_changed", [True, False])
def test_config_changed(env_changed: bool, cassandra_config_changed: bool):
    """Charm should restart workload only if it's active when config is changed."""
    ctx = testing.Context(CassandraCharm)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    state = testing.State(leader=True, relations={relation, bootstrap_relation})
    with (
        patch("managers.config.ConfigManager.render_env", return_value=env_changed) as render_env,
        patch(
            "managers.config.ConfigManager.render_cassandra_config",
            return_value=cassandra_config_changed,
        ) as render_cassandra_config,
        patch("managers.database.DatabaseManager.update_role_password"),
        patch("charm.CassandraWorkload") as workload,
        patch("charm.CassandraCharm.setup_internal_certificates", return_value=True),
        patch(
            "managers.node.NodeManager.is_healthy",
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

        if env_changed or cassandra_config_changed:
            workload.return_value.restart.assert_called_once()
        else:
            workload.return_value.restart.assert_not_called()
