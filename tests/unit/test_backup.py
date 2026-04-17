# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

from unittest.mock import PropertyMock, patch

import pytest
from ops import testing

from charm import CassandraCharm
from core.state import PEER_RELATION, S3_RELATION
from core.statuses import Status
from events.backup import BackupMessages

from .helpers import generate_tls_artifacts

BOOTSTRAP_RELATION = "bootstrap"
PEER_SECRET = "cassandra-peers.cassandra.app"
TEST_BACKUP_ID = "managed-2026-01-01T01:01:01"


@pytest.mark.parametrize(
    "action_params",
    [("list-backups", {}), ("create-backup", {}), ("restore", {"backup-id": TEST_BACKUP_ID})],
    ids=lambda ap: f"{ap[0]} {ap[1]}",
)
def test_run_actions_before_s3_integration(action_params):
    ctx = testing.Context(CassandraCharm)
    peer_relation = testing.PeerRelation(endpoint=PEER_RELATION)
    state = testing.State(leader=True, relations={peer_relation})

    with (
        patch("charm.CassandraWorkload") as workload,
        patch("managers.node.NodeManager.is_healthy", return_value=True),
        patch("charm.CassandraCharm.restart"),
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
    ):
        workload.return_value.exec.return_value = ("ok", None)
        workload.return_value.generate_string.return_value = "password"
        action, params = action_params

        with pytest.raises(testing.ActionFailed) as exc:
            state = ctx.run(ctx.on.action(action, params=params), state)

        assert exc.value.message == BackupMessages.NOT_READY.value


@pytest.mark.parametrize(
    "action_params",
    [("list-backups", {}), ("create-backup", {}), ("restore", {"backup-id": TEST_BACKUP_ID})],
    ids=lambda ap: f"running {ap[0]} with params={ap[1]}",
)
@pytest.mark.parametrize(
    "workload_active", [True, False], ids=lambda p: f"Workload:{'active' if p else 'not active'} "
)
def test_run_actions_with_s3_integration(action_params, workload_active, caplog):
    ctx = testing.Context(CassandraCharm)
    tls_artifacts = generate_tls_artifacts()
    peer_relation = testing.PeerRelation(
        endpoint=PEER_RELATION,
        local_unit_data={
            "ip": "10.10.10.10",
            "workload_state": "active" if workload_active else "",
        },
        local_app_data={
            "cluster_state": "active",
            "seeds": "10.10.10.10:7000",
            "nodetool-password": "password",
            "operator-password": "drowssap",
            "internal-ca-secret": tls_artifacts.ca.raw,
            "internal-ca-key-secret": tls_artifacts.private_key.raw,
        },
    )
    secret = testing.Secret({"access-key": "aws-access-key", "secret-key": "aws-secret-key"})
    s3_relation = testing.Relation(
        endpoint=S3_RELATION,
        remote_app_data={
            "bucket": "test",
            "region": "us-east-1",
            "endpoint": "https://s3.amazonaws.com",
            "secret-extra": secret.id,
        },
    )
    state = testing.State(leader=True, relations={peer_relation, s3_relation}, secrets=[secret])

    with (
        patch("charm.CassandraWorkload") as workload,
        patch("managers.node.NodeManager.is_healthy", return_value=True),
        patch("charm.CassandraCharm.restart"),
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.backup.BackupManager.list_backups") as list_backups,
        patch("managers.backup.BackupManager.create_backup") as create_backup,
    ):
        workload.return_value.exec.return_value = ("ok", None)
        workload.return_value.generate_string.return_value = "password"
        action, params = action_params

        if not workload_active:
            with pytest.raises(testing.ActionFailed) as exc:
                state = ctx.run(ctx.on.action(action, params=params), state)

            assert exc.value.message == BackupMessages.WORKLOAD_NOT_READY.value
            return

        state = ctx.run(ctx.on.action(action, params=params), state)
        match action:
            case "list-backups":
                list_backups.assert_called_once()
            case "create-backup":
                create_backup.assert_called_once()
                assert caplog.messages[-1].startswith("Backup succeeded: with backup-id")
            case "restore":
                assert state.unit_status == Status.RESTORING.value
                peer_relation_after = state.get_relation(peer_relation.id)
                assert peer_relation_after.local_unit_data["restoring"] == "true"
                assert peer_relation_after.local_unit_data["backup-id"] == TEST_BACKUP_ID
