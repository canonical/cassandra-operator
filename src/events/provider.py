#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for main Cassandra charm events."""

import logging

from charms.data_platform_libs.v1.data_interfaces import (
    EntityPermissionModel,
    RequirerCommonModel,
    ResourceEntityPermissionsChangedEvent,
    ResourceEntityRequestedEvent,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    ResourceRequestedEvent,
    SecretBool,
)
from charms.data_platform_libs.v1.data_models import TypedCharmBase
from ops import (
    Object,
    RelationBrokenEvent,
)
from pydantic import SecretStr

from core.config import CharmConfig
from core.state import CLIENT_RELATION, ApplicationState, DbRole
from core.workload import WorkloadBase
from managers.database import _CASSANDRA_DEFAULT_CREDENTIALS, DatabaseManager, Permissions
from managers.node import NodeManager
from managers.tls import TLSManager

logger = logging.getLogger(__name__)


class ProviderEvents(Object):
    """Handle all base and cassandra related events."""

    def __init__(
        self,
        charm: TypedCharmBase[CharmConfig],
        state: ApplicationState,
        workload: WorkloadBase,
        node_manager: NodeManager,
        tls_manager: TLSManager,
        database_manager: DatabaseManager,
    ):
        super().__init__(charm, key="provider_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.database_manager = database_manager
        self.node_manager = node_manager
        self.tls_manager = tls_manager

        self.cassandra_client = ResourceProviderEventHandler(
            self.charm,
            relation_name=CLIENT_RELATION,
            request_model=RequirerCommonModel,
            mtls_enabled=False,  # mTLS feature is not yet ready.
        )

        self.framework.observe(
            self.cassandra_client.on.resource_requested, self._on_resource_requested
        )
        self.framework.observe(
            self.cassandra_client.on.resource_entity_requested, self._on_resource_entity_requested
        )
        self.framework.observe(
            self.cassandra_client.on.resource_entity_permissions_changed,
            self._on_resource_entity_permissions_changed,
        )

        self.framework.observe(
            self.charm.on[CLIENT_RELATION].relation_broken, self._on_relation_broken
        )

    def _on_resource_requested(self, event: ResourceRequestedEvent) -> None:
        """Event triggered when a new keyspace is requested."""
        logger.debug("External client requested database resource")

        request: RequirerCommonModel = event.request
        resource = request.resource
        relation_id = event.relation.id

        if not self.charm.unit.is_leader():
            return

        if not self.state.unit.is_operational:
            logger.debug("Deferring _on_resource_requested unit workload is not ready")
            event.defer()
            return

        if not event.relation or not event.relation.app:
            logger.debug("Deferring _on_resource_requested event relation is not ready")
            event.defer()
            return

        rolename = self._rolename_from_relation(relation_id, request.salt)
        password = self.workload.generate_string()

        if rolename in {r.name for r in self.state.cluster.roles}:
            logger.error(f"Rolename: {rolename} already exists for this relation")
            return

        self.database_manager.init_user(rolename, password)

        self.database_manager.create_keyspace(
            resource,
            len(self.state.units),
        )

        self.database_manager.set_ks_permissions(
            rolename,
            resource,
            Permissions("ALL"),
        )

        response = ResourceProviderModel(
            salt=request.salt,
            username=SecretStr(rolename),
            password=SecretStr(password),
            resource=resource,
            request_id=request.request_id,
            endpoints=",".join([f"{unit.ip}" for unit in self.state.units]),
            tls=SecretBool(self.state.unit.client_tls.ready),
            tls_ca=SecretStr(
                self.state.unit.client_tls.ca.raw if self.state.unit.client_tls.ca else ""
            ),
            version="v1",
        )

        self.cassandra_client.set_response(relation_id, response)

        self.state.cluster.roles = {*self.state.cluster.roles, DbRole(rolename, event.relation.id)}

    def _on_resource_entity_requested(self, event: ResourceEntityRequestedEvent) -> None:
        """Event triggered when a new user in a keyspace is requested."""
        if not self.charm.unit.is_leader():
            return

        if not self.state.unit.is_operational:
            logger.debug("Deferring _on_resource_entity_requested unit workload is not ready")
            event.defer()
            return

        logger.debug("External client requested entity in database")

        request: RequirerCommonModel = event.request
        resource = request.resource

        if not resource:
            logger.error("_on_resource_entity_requested resource is empty")
            return

        relation_id = event.relation.id
        rolename = self._rolename_from_relation(relation_id, request.salt)
        password = self.workload.generate_string()

        permissions_req: list[EntityPermissionModel] = (
            request.entity_permissions if request.entity_permissions else []
        )
        self._validate_entity_permissions(permissions_req)

        if rolename in {r.name for r in self.state.cluster.roles}:
            logger.error(f"Rolename: {rolename} already exists for this relation")
            return

        # User will remain if code below throws an exception
        self.database_manager.init_user(rolename, password)

        for perm_req in permissions_req:
            # Ignore resource_type. This hook is only for keyspaces.
            self.database_manager.set_ks_permissions(
                rolename,
                perm_req.resource_name,
                Permissions(*perm_req.privileges),
            )

        response = ResourceProviderModel(
            resource=resource,
            request_id=request.request_id,
            salt=request.salt,
            entity_name=SecretStr(rolename),
            entity_password=SecretStr(password),
            endpoints=",".join([f"{unit.ip}" for unit in self.state.units]),
            tls=SecretBool(self.state.unit.client_tls.ready),
            tls_ca=SecretStr(
                self.state.unit.client_tls.ca.raw if self.state.unit.client_tls.ca else ""
            ),
            version="v1",
        )

        self.cassandra_client.set_response(relation_id, response)

        self.state.cluster.roles = {*self.state.cluster.roles, DbRole(rolename, event.relation.id)}

    def _on_resource_entity_permissions_changed(
        self, event: ResourceEntityPermissionsChangedEvent
    ) -> None:
        """Event triggered when a client changed user permissions for a keyspace."""
        if not self.charm.unit.is_leader():
            return

        if not self.state.unit.is_operational:
            logger.debug(
                "Deferring _on_resource_entity_permissions_changed unit workload is not ready"
            )
            event.defer()
            return

        logger.info("External client changed user permissions")

        request: RequirerCommonModel = event.request
        resource = request.resource

        relation_id = event.relation.id
        rolename = self._rolename_from_relation(relation_id, request.salt)

        if rolename not in {r.name for r in self.state.cluster.roles}:
            logger.error(f"Requested user not exists: {rolename} for this relation")
            return

        permissions_req: list[EntityPermissionModel] = (
            request.entity_permissions if request.entity_permissions else []
        )
        self._validate_entity_permissions(permissions_req)

        for perm_req in permissions_req:
            # Ignore resource_type. This hook is only for keyspaces.
            self.database_manager.set_ks_permissions(
                rolename,
                perm_req.resource_name,
                Permissions(*perm_req.privileges),
            )

        response = ResourceProviderModel(
            request_id=request.request_id,
            resource=resource,
            username=SecretStr(rolename),
            endpoints=",".join([f"{unit.ip}" for unit in self.state.units]),
            tls=SecretBool(self.state.unit.client_tls.ready),
            tls_ca=SecretStr(
                self.state.unit.client_tls.ca.raw if self.state.unit.client_tls.ca else ""
            ),
        )

        self.cassandra_client.set_response(relation_id, response)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Event triggered when client relation is broken.

        Removes relation users from cluster.

        Args:
            event: the event from a related client application needing a user
        """
        if not self.charm.unit.is_leader():
            return

        if not self.state.unit.is_operational:
            logger.debug("Deferring _on_relation_broken inactive unit workload state")
            event.defer()
            return

        # don't remove anything if app is going down
        if self.charm.app.planned_units() == 0:
            return

        if event.relation.app != self.charm.app:
            for role in self.state.cluster.roles:
                if role.relation_id != event.relation.id:
                    continue

                self.database_manager.remove_user(role.name)
                self.state.cluster.roles = self.state.cluster.roles - {role}

    @staticmethod
    def _validate_entity_permissions(perms: list[EntityPermissionModel]) -> None:
        for perm in perms:
            Permissions(*perm.privileges)

    @staticmethod
    def _client_alias_from_relation(app_name: str, relation_id: int) -> str:
        return f"{app_name}-{relation_id}"

    @staticmethod
    def _rolename_from_relation(relation_id: int, salt: str) -> str:
        return f"user_{salt}_relation_{relation_id}"
