#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for main Cassandra charm events."""

import logging
import hashlib
from random import randrange
from time import sleep
from typing import Callable

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.data_platform_libs.v1.data_interfaces import (
    EntityPermissionModel,
    ResourceEntityPermissionsChangedEvent,
    ResourceProviderEventHandler,
    RequirerCommonModel,
    ResourceProviderModel,
    ResourceRequestedEvent,
    ResourceEntityRequestedEvent,
    SecretBool,
    RelationCreatedEvent,
    )
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops import (
    Object,
    RelationBrokenEvent,
)

from pydantic import ValidationError, SecretStr
from tenacity import Retrying, stop_after_delay, wait_exponential, wait_fixed

from common.exceptions import BadSecretError
from core.config import CharmConfig
from core.literals import CASSANDRA_ADMIN_USERNAME
from core.state import CASSANDRA_CLIENT_PORT, PEER_RELATION, ApplicationState, UnitWorkloadState, CLIENT_RELATION
from core.statuses import Status
from core.workload import WorkloadBase
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from managers.database import DatabaseManager, Permissions
from managers.tls import Sans, TLSManager

logger = logging.getLogger(__name__)

class ExternalClientsEvents(Object):
    """Handle all base and cassandra related events."""

    def __init__(
        self,
        charm: TypedCharmBase[CharmConfig],
        state: ApplicationState,
        workload: WorkloadBase,
        cluster_manager: ClusterManager,
        database_manager: DatabaseManager,
        acquire_operator_password: Callable[[], str],
    ):
        super().__init__(charm, key="provider_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.database_manager = database_manager
        self.cluster_manager = cluster_manager

        self.acquire_operator_password = acquire_operator_password

        self.cassandra_client = ResourceProviderEventHandler(
            self.charm,
            relation_name=CLIENT_RELATION,
            request_model=RequirerCommonModel,
            mtls_enabled=False,
        )

        self.framework.observe(self.cassandra_client.on.resource_requested, self._on_resource_requested)
        self.framework.observe(self.cassandra_client.on.resource_entity_requested, self._on_resource_entity_requested)        
        self.framework.observe(self.cassandra_client.on.resource_entity_permissions_changed, self._on_resource_entity_permissions_changed)

        self.framework.observe(
            self.charm.on.cassandra_client_relation_created, self._cassandra_client_relation_created
        )        

    def _cassandra_client_relation_created(self, _: RelationCreatedEvent) -> None:
        logger.debug(f"---- _cassandra_client_relation_created ----")
        
    def _on_resource_requested(self, event: ResourceRequestedEvent) -> None:
        """Event triggered when a new keyspace is requested."""

        if not self.charm.unit.is_leader():
            return

        if any([self.state.unit.workload_state != UnitWorkloadState.ACTIVE, not self.workload.is_alive()]):
            logger.debug(f"Defering _on_resource_requested unit workload is not ready")
            event.defer()
            return
        
        logger.debug(f"External client requested database resource")

        request: RequirerCommonModel = event.request

        relation_id = event.relation.id

        resource = request.resource

        self.database_manager.create_keyspace(resource, len(self.state.units), self.acquire_operator_password())

        sleep(randrange(5))

        response = ResourceProviderModel(
            resource=resource,
            salt=request.salt,
            request_id=request.request_id,
            username=SecretStr("a"),
            password=SecretStr("a"),
            endpoints=",".join([f"{unit.ip}" for unit in self.state.units]),
            tls=SecretBool(self.state.unit.client_tls.ready),
            tls_ca=SecretStr(self.state.unit.client_tls.ca.raw if self.state.unit.client_tls.ca else ""),
            version="v1"
        )

        logger.info(f"Sending response on resource requested: {response}")

        self.cassandra_client.set_response(relation_id, response)

    def _on_resource_entity_requested(self, event: ResourceEntityRequestedEvent) -> None:
        """Event triggered when a new user in a keyspace is requested."""        
        if not self.charm.unit.is_leader():
            return

        if any([self.state.unit.workload_state != UnitWorkloadState.ACTIVE, not self.workload.is_alive()]):
            logger.debug(f"Defering _on_resource_entity_requested unit workload is not ready")
            event.defer()
            return
        
        logger.debug(f"External client requested entity in database")

        request: RequirerCommonModel = event.request        
        resource = request.resource

        relation_id = event.relation.id
        rolename = self._rolename_from_relation(relation_id)
        password = self.workload.generate_password()
        permissions_req: list[EntityPermissionModel] = request.entity_permissions if request.entity_permissions else []
        self._validate_entity_permissions(permissions_req)

        # User will remain if code below throws an exception
        self.database_manager.init_user(rolename, password, self.acquire_operator_password())

        for perm_req in permissions_req:
            # Ignore resource_type. This hook is only for keyspaces.
            self.database_manager.set_ks_permissions(
                rolename,
                perm_req.resource_name,
                Permissions(*perm_req.privileges),
                self.acquire_operator_password(),
            )

        response = ResourceProviderModel(
            request_id=request.request_id,
            resource=resource,
            password=SecretStr(password),
            username=SecretStr(rolename),
            endpoints=",".join([f"{unit.ip}" for unit in self.state.units]),
            tls=SecretBool(self.state.unit.client_tls.ready),
            tls_ca=SecretStr(self.state.unit.client_tls.ca.raw if self.state.unit.client_tls.ca else ""),
        )

        self.cassandra_client.set_response(relation_id, response)

        
    def _on_resource_entity_permissions_changed(self, event: ResourceEntityPermissionsChangedEvent) -> None:
        """Event triggered when a client chaged user permissions for a keyspace."""        
        if not self.charm.unit.is_leader():
            return

        if any([self.state.unit.workload_state != UnitWorkloadState.ACTIVE, not self.workload.is_alive()]):
            logger.debug(f"Defering _on_resource_entity_permissions_changed unit workload is not ready")
            event.defer()
            return
        
        logger.debug(f"External client changed user permissions")

        request: RequirerCommonModel = event.request
        resource = request.resource

        relation_id = event.relation.id
        rolename = self._rolename_from_relation(relation_id)

        permissions_req: list[EntityPermissionModel]  = request.entity_permissions if request.entity_permissions else []
        self._validate_entity_permissions(permissions_req)        

        for perm_req in permissions_req:
            # Ignore resource_type. This hook is only for keyspaces.
            self.database_manager.set_ks_permissions(
                rolename,
                perm_req.resource_name,
                Permissions(*perm_req.privileges),
                self.acquire_operator_password(),
            )

        response = ResourceProviderModel(
            request_id=request.request_id,
            resource=resource,
            username=SecretStr(rolename),
            endpoints=",".join([f"{unit.ip}" for unit in self.state.units]),
            tls=SecretBool(self.state.unit.client_tls.ready),
            tls_ca=SecretStr(self.state.unit.client_tls.ca.raw if self.state.unit.client_tls.ca else "")
        )

        self.cassandra_client.set_response(relation_id, response)

        
    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `client-relation-broken` event.

        Removes relation users from cluster.

        Args:
            event: the event from a related client application needing a user
        """
        if not self.charm.unit.is_leader:
            return

        if not self.workload.is_alive():
            logger.debug(f"Defering _on_relation_broken inactive unit workload state")
            event.defer()
            return

        # don't remove anything if app is going down
        if self.charm.app.planned_units() == 0:
            return

        if event.relation.app != self.charm.app:
            rolename = f"relation_{event.relation.id}"
            self.database_manager.remove_user(rolename, self.acquire_operator_password())

    @staticmethod
    def _validate_entity_permissions(perms: list[EntityPermissionModel]) -> None:
        for perm in perms:
            Permissions(*perm.privileges)
            
    @staticmethod
    def _rolename_from_relation(relation_id: int) -> str:
        """
        Generate a deterministic role name from a relation id.
        The result is safe for Cassandra identifiers (letters, digits, underscore).
        """
        # Преобразуем relation_id в строку и хешируем
        unique_str = f"relation_{relation_id}"
        hashed = hashlib.sha256(unique_str.encode()).hexdigest()[:8]
        
        # Заменяем тире на нижнее подчёркивание, хотя в hex их нет
        safe_hashed = hashed.replace("-", "_")
        
        return f"user_{safe_hashed}"
