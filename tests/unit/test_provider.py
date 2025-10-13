# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import dataclasses
import logging
from unittest.mock import MagicMock, patch

import pytest
from charms.data_platform_libs.v1.data_interfaces import (
    EntityPermissionModel,
    RequirerCommonModel,
    ResourceEntityPermissionsChangedEvent,
    ResourceEntityRequestedEvent,
    ResourceRequestedEvent,
    SecretBool,
    SecretStr,
    gen_hash,
    gen_salt,
)
from ops import testing
from ops.testing import Context

from charm import CassandraCharm
from core.state import (
    CLIENT_RELATION,
    CLIENT_TLS_RELATION,
    PEER_RELATION,
    DbRole,
)

logger = logging.getLogger(__name__)

BOOTSTRAP_RELATION = "bootstrap"


@pytest.fixture
def ctx() -> Context[CassandraCharm]:
    """Create a test context for CassandraCharm."""
    return Context(CassandraCharm, unit_id=0)


@dataclasses.dataclass
class ClientRealtionContext:
    context: Context[CassandraCharm]
    peer_relation: testing.PeerRelation
    client_relation: testing.Relation
    client_tls_relation: testing.Relation
    bootstrap_relation: testing.PeerRelation


#    client_csr: CertificateSigningRequest
#    client_crt: Certificate
#    client_provider_crt: ProviderCertificate


def client_relations_context(
    ctx: Context[CassandraCharm],
    workload_active: bool,
) -> ClientRealtionContext:
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "ip": "1.1.1.1",
            "workload_state": "active" if workload_active else "",
        },
        local_app_data={"cluster_state": "active", "seeds": "2.2.2.2:7000"},
    )
    client_relation = testing.Relation(id=2, endpoint=CLIENT_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION)
    bootstrap_relation = testing.PeerRelation(id=4, endpoint=BOOTSTRAP_RELATION)

    return ClientRealtionContext(
        context=ctx,
        peer_relation=peer_relation,
        client_relation=client_relation,
        client_tls_relation=client_tls_relation,
        bootstrap_relation=bootstrap_relation,
    )


# ===================== TESTS =====================


def generate_resource_request() -> RequirerCommonModel:
    resource = "test_ks"
    resource_name = "ks"
    salt = gen_salt()
    return RequirerCommonModel(
        resource=resource,
        salt=salt,
        request_id=gen_hash(resource_name, salt),
    )


def generate_resource_entity_request(permissions: EntityPermissionModel) -> RequirerCommonModel:
    resource = "test_ks"
    resource_name = "ks"
    salt = gen_salt()
    return RequirerCommonModel(
        resource=resource,
        salt=salt,
        request_id=gen_hash(resource_name, salt),
        entity_type="USER",
        entity_permissions=[permissions],
    )


@pytest.mark.parametrize("is_leader", [True, False])
@pytest.mark.parametrize("workload_active", [True, False])
def test_resource_requested_non_leader_does_nothing(ctx, is_leader, workload_active):
    """Non-leader units should not process resource requests."""
    new_ctx = client_relations_context(ctx, workload_active)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=is_leader,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.create_keyspace") as create_keyspace,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        resource_requested_event = MagicMock(spec=ResourceRequestedEvent)
        resource_requested_event.request = generate_resource_request()
        resource_requested_event.relation = new_ctx.client_relation
        resource_requested_event.defer = MagicMock()

        # For some reason ops.testing.Relation do not have app
        object.__setattr__(resource_requested_event.relation, "app", MagicMock())

        charm.provider_events._on_resource_requested(resource_requested_event)

        manager.run()

        if is_leader and workload_active:
            init_user.assert_called_once()
            create_keyspace.assert_called_once()
            set_ks_permissions.assert_called_once()
        else:
            init_user.assert_not_called()


@pytest.mark.parametrize("is_leader", [True, False])
@pytest.mark.parametrize("workload_active", [True, False])
def test_resource_entity_requested_behaviour(ctx, is_leader, workload_active):
    new_ctx = client_relations_context(ctx, workload_active)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=is_leader,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        permissions = EntityPermissionModel(
            resource_name="test_ks",
            resource_type="keyspace",
            privileges=["ALL"],
        )
        event = MagicMock(spec=ResourceEntityRequestedEvent)
        event.request = generate_resource_entity_request(permissions)
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        charm.provider_events._on_resource_entity_requested(event)

        manager.run()

        if is_leader and workload_active:
            init_user.assert_called_once()
            set_ks_permissions.assert_called_once()
        else:
            init_user.assert_not_called()


def test_resource_entity_requested_empty_resource_no_calls(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        permissions = EntityPermissionModel(
            resource_name="test_ks",
            resource_type="keyspace",
            privileges=["ALL"],
        )
        req = generate_resource_entity_request(permissions)
        # Override resource to empty
        req.resource = ""
        event = MagicMock(spec=ResourceEntityRequestedEvent)
        event.request = req
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        charm.provider_events._on_resource_entity_requested(event)

        manager.run()

        init_user.assert_not_called()
        set_ks_permissions.assert_not_called()


def test_resource_requested_mtls_defers_when_alias_needs_update(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.create_keyspace") as create_keyspace,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        # Force alias_needs_update to True so we defer
        charm.provider_events.tls_manager.alias_needs_update = MagicMock(return_value=True)

        req = generate_resource_request()
        # Provide some mtls_cert so the code path checks alias
        req.mtls_cert = SecretStr("cert-bytes")
        event = MagicMock(spec=ResourceRequestedEvent)
        event.request = req
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        charm.provider_events._on_resource_requested(event)

        manager.run()

        event.defer.assert_called_once()
        init_user.assert_not_called()
        create_keyspace.assert_not_called()
        set_ks_permissions.assert_not_called()


def test_resource_requested_existing_role_noop(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.create_keyspace") as create_keyspace,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        event = MagicMock(spec=ResourceRequestedEvent)
        event.request = generate_resource_request()
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        # Pre-add role to cluster state to trigger early return
        rolename = charm.provider_events._rolename_from_relation(
            event.relation.id, event.request.salt
        )
        charm.provider_events.state.cluster.roles = {DbRole(rolename, event.relation.id)}

        charm.provider_events._on_resource_requested(event)

        manager.run()

        init_user.assert_not_called()
        create_keyspace.assert_not_called()
        set_ks_permissions.assert_not_called()


@pytest.mark.parametrize("role_exists", [True, False])
def test_permissions_changed_respects_existing_role(ctx, role_exists):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        charm: CassandraCharm = manager.charm

        event = MagicMock(spec=ResourceEntityPermissionsChangedEvent)
        event.relation = MagicMock()
        event.relation.id = new_ctx.client_relation.id
        event.relation.app = MagicMock()
        event.defer = MagicMock()
        event.request = generate_resource_entity_request(
            EntityPermissionModel(
                resource_name="test_keyspace",
                resource_type="keyspace",
                privileges=["SELECT", "MODIFY"],
            )
        )

        # Adjust cluster state based on role_exists
        rolename = charm.provider_events._rolename_from_relation(
            event.relation.id, event.request.salt
        )
        charm.provider_events.state.cluster.roles = (
            {DbRole(rolename, event.relation.id)} if role_exists else set()
        )

        charm.provider_events._on_resource_entity_permissions_changed(event)

        manager.run()

        if role_exists:
            set_ks_permissions.assert_called()
        else:
            set_ks_permissions.assert_not_called()


def test_relation_broken_removes_users_for_remote_app(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.remove_user") as remove_user,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        charm: CassandraCharm = manager.charm

        # Build a RelationBrokenEvent-like object
        event = MagicMock()
        event.relation = MagicMock()
        event.relation.id = new_ctx.client_relation.id
        event.relation.app = MagicMock()  # remote app
        event.defer = MagicMock()

        # Prepare state with roles
        charm.provider_events.state.cluster.roles = {
            DbRole("user_a", event.relation.id),
            DbRole("user_b", event.relation.id),
        }

        # Ensure planned_units != 0
        charm.app.planned_units = MagicMock(return_value=1)

        charm.provider_events._on_relation_broken(event)

        manager.run()

        assert remove_user.call_count == 2


@pytest.mark.parametrize(
    "invalid_privileges",
    [
        ["INVALID"],
        ["SELECT", "WRONG"],
        ["ALLY"],
        ["ALL", "NOPE"],
    ],
)
def test_resource_entity_requested_with_invalid_permissions_raises_and_no_db_calls(
    ctx, invalid_privileges
):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        permissions = EntityPermissionModel(
            resource_name="test_ks",
            resource_type="keyspace",
            privileges=invalid_privileges,
        )
        event = MagicMock(spec=ResourceEntityRequestedEvent)
        event.request = generate_resource_entity_request(permissions)
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        with pytest.raises(ValueError):
            charm.provider_events._on_resource_entity_requested(event)

        # Run any pending framework tasks (should be none due to exception)
        manager.run()

        init_user.assert_not_called()
        set_ks_permissions.assert_not_called()


def test_resource_requested_defers_when_relation_app_missing(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.create_keyspace") as create_keyspace,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        event = MagicMock(spec=ResourceRequestedEvent)
        event.request = generate_resource_request()
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", None)

        charm.provider_events._on_resource_requested(event)

        manager.run()

        event.defer.assert_called_once()
        init_user.assert_not_called()
        create_keyspace.assert_not_called()
        set_ks_permissions.assert_not_called()


def test_relation_broken_no_removal_when_planned_units_zero(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.remove_user") as remove_user,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        charm: CassandraCharm = manager.charm

        event = MagicMock()
        event.relation = MagicMock()
        event.relation.id = new_ctx.client_relation.id
        event.relation.app = MagicMock()  # remote app
        event.defer = MagicMock()

        charm.provider_events.state.cluster.roles = {DbRole("user_a", event.relation.id)}

        charm.app.planned_units = MagicMock(return_value=0)

        charm.provider_events._on_relation_broken(event)

        manager.run()

        remove_user.assert_not_called()


def test_relation_broken_same_app_no_removal(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.remove_user") as remove_user,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        charm: CassandraCharm = manager.charm

        event = MagicMock()
        event.relation = MagicMock()
        event.relation.id = new_ctx.client_relation.id
        event.relation.app = charm.app  # same app
        event.defer = MagicMock()

        charm.provider_events.state.cluster.roles = {DbRole("user_a", event.relation.id)}

        charm.app.planned_units = MagicMock(return_value=1)

        charm.provider_events._on_relation_broken(event)

        manager.run()

        remove_user.assert_not_called()


def test_relation_broken_no_removal_other_relations(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.remove_user") as remove_user,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        charm: CassandraCharm = manager.charm

        event = MagicMock()
        event.relation = MagicMock()
        event.relation.id = new_ctx.client_relation.id
        event.relation.app = MagicMock()
        event.defer = MagicMock()

        same_relation_user = DbRole("user_a", event.relation.id)
        other_relation_user = DbRole("user_b", event.relation.id + 10)

        charm.provider_events.state.cluster.roles = {same_relation_user, other_relation_user}

        charm.app.planned_units = MagicMock(return_value=1)

        charm.provider_events._on_relation_broken(event)

        manager.run()

        remove_user.assert_called()

        assert other_relation_user in charm.provider_events.state.cluster.roles
        assert same_relation_user not in charm.provider_events.state.cluster.roles


def test_entity_requested_empty_permissions_still_creates_user_and_response(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.init_user") as init_user,
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        workload.return_value.generate_string.return_value = "password"
        charm: CassandraCharm = manager.charm

        req = generate_resource_entity_request(
            EntityPermissionModel(resource_name="test_ks", resource_type="keyspace", privileges=[])
        )
        req.entity_permissions = []
        event = MagicMock(spec=ResourceEntityRequestedEvent)
        event.request = req
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        charm.provider_events._on_resource_entity_requested(event)

        manager.run()

        init_user.assert_called_once()
        set_ks_permissions.assert_not_called()


def test_permissions_changed_empty_permissions_only_sets_response(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("managers.database.DatabaseManager.set_ks_permissions") as set_ks_permissions,
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.is_alive.return_value = True
        charm: CassandraCharm = manager.charm

        event = MagicMock(spec=ResourceEntityPermissionsChangedEvent)
        event.relation = MagicMock()
        event.relation.id = new_ctx.client_relation.id
        event.relation.app = MagicMock()
        event.defer = MagicMock()
        event.request = generate_resource_entity_request(
            EntityPermissionModel(
                resource_name="test_keyspace",
                resource_type="keyspace",
                privileges=["SELECT", "MODIFY"],
            )
        )

        # Ensure role exists to pass that guard
        rolename = charm.provider_events._rolename_from_relation(
            event.relation.id, event.request.salt
        )
        charm.provider_events.state.cluster.roles = {DbRole(rolename, event.relation.id)}

        # Make permissions empty
        event.request.entity_permissions = []

        charm.provider_events._on_resource_entity_permissions_changed(event)

        manager.run()

        set_ks_permissions.assert_not_called()


def test_resource_requested_response_payload_fields(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("workload.CassandraWorkload.generate_string", return_value="password"),
        patch("workload.CassandraWorkload.is_alive", return_value=True),
        patch("events.provider.ResourceProviderEventHandler.set_response") as set_response,
        patch("managers.database.DatabaseManager.init_user"),
        patch("managers.database.DatabaseManager.create_keyspace"),
        patch("managers.database.DatabaseManager.set_ks_permissions"),
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
    ):
        charm: CassandraCharm = manager.charm

        event = MagicMock(spec=ResourceRequestedEvent)
        req = generate_resource_request()
        event.request = req
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        charm.provider_events._on_resource_requested(event)

        manager.run()

        assert set_response.called
        args, kwargs = set_response.call_args
        relation_id, response = args
        assert relation_id == new_ctx.client_relation.id
        # Validate selected response fields
        assert response.username.get_secret_value().startswith("user_")
        assert response.password.get_secret_value() == "password"
        assert response.salt == req.salt
        assert response.resource == req.resource
        assert response.request_id == req.request_id
        assert response.endpoints
        assert isinstance(response.tls, SecretBool)


def test_entity_requested_response_payload_fields(ctx):
    new_ctx = client_relations_context(ctx, True)
    context = new_ctx.context
    state_in = testing.State(
        relations=[
            new_ctx.peer_relation,
            new_ctx.client_relation,
            new_ctx.bootstrap_relation,
        ],
        leader=True,
    )

    with (
        patch("workload.snap.SnapCache"),
        patch("workload.CassandraWorkload.generate_string", return_value="password"),
        patch("workload.CassandraWorkload.is_alive", return_value=True),
        patch("events.provider.ResourceProviderEventHandler.set_response") as set_response,
        patch("managers.database.DatabaseManager.init_user"),
        patch("managers.database.DatabaseManager.set_ks_permissions"),
        context(context.on.relation_created(new_ctx.client_relation), state=state_in) as manager,
    ):
        charm: CassandraCharm = manager.charm

        permissions = EntityPermissionModel(
            resource_name="test_ks",
            resource_type="keyspace",
            privileges=["SELECT"],
        )
        event = MagicMock(spec=ResourceEntityRequestedEvent)
        req = generate_resource_entity_request(permissions)
        event.request = req
        event.relation = new_ctx.client_relation
        event.defer = MagicMock()
        object.__setattr__(event.relation, "app", MagicMock())

        charm.provider_events._on_resource_entity_requested(event)

        manager.run()

        assert set_response.called
        args, kwargs = set_response.call_args
        relation_id, response = args
        assert relation_id == new_ctx.client_relation.id
        # Validate selected response fields
        assert response.entity_name.get_secret_value().startswith("user_")
        assert response.entity_password.get_secret_value() == "password"
        assert response.salt == req.salt
        assert response.resource == req.resource
        assert response.request_id == req.request_id
        assert response.endpoints
        assert isinstance(response.tls, SecretBool)
