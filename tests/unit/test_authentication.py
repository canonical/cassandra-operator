# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from unittest.mock import PropertyMock, patch

import pytest
from ops import testing

from charm import CassandraCharm
from core.state import PEER_RELATION

BOOTSTRAP_RELATION = "bootstrap"
PEER_SECRET = "cassandra-peers.cassandra.app"


@pytest.mark.parametrize("bad_secret", [True, False])
def test_start_custom_secret(bad_secret: bool):
    ctx = testing.Context(CassandraCharm)
    peer_relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    password_secret = testing.Secret(
        tracked_content={"foo": "bar"} if bad_secret else {"cassandra": "custom_password"}
    )
    state = testing.State(
        leader=True,
        relations={peer_relation, bootstrap_relation},
        config={"system-users": password_secret.id},
        secrets={password_secret},
    )

    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
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

        if bad_secret:
            assert state.app_status.name == "blocked"
        else:
            peer_secret = state.get_secret(label="cassandra-peers.cassandra.app")
            assert (
                peer_secret.latest_content
                and peer_secret.latest_content.get("cassandra-password") == "custom_password"
            )
            update_system_user_password.assert_called_once_with("custom_password")


def test_update_custom_secret():
    ctx = testing.Context(CassandraCharm)
    peer_relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"workload_state": "active"}
    )
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    password_secret = testing.Secret(
        tracked_content={"cassandra": "custom_password"},
        latest_content={"cassandra": "updated_password"},
    )
    state = testing.State(
        leader=True,
        relations={peer_relation, bootstrap_relation},
        config={"system-users": password_secret.id},
        secrets={password_secret},
    )

    with (
        patch(
            "managers.database.DatabaseManager.update_system_user_password"
        ) as update_system_user_password,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.generate_password.return_value = "password"

        state = ctx.run(ctx.on.secret_changed(password_secret), state)

        peer_secret = state.get_secret(label="cassandra-peers.cassandra.app")
        assert (
            peer_secret.latest_content
            and peer_secret.latest_content.get("cassandra-password") == "updated_password"
        )
        update_system_user_password.assert_called_once_with("updated_password")
