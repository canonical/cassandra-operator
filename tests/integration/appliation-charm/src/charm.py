#!/usr/bin/env python3
# Copyright 2025 bon
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Application Charm definition."""

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    Session,
)
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from charms.data_platform_libs.v1.data_interfaces import (
    EntityPermissionModel,
    RequirerCommonModel,
    RequirerDataContractV1,
    ResourceCreatedEvent,
    ResourceEndpointsChangedEvent,
    ResourceEntityCreatedEvent,
    ResourceProviderModel,
    ResourceRequirerEventHandler,
)
from charms.operator_libs_linux.v2 import snap
from ops import ActionEvent, InstallEvent, RelationCreatedEvent
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, WaitingStatus

CQLSH_SNAP_NAME = "cqlsh"
PEER_REL = "local"

logger = logging.getLogger(__name__)


@dataclass
class UnitData:
    """Holds unit-specific data for database access."""

    keyspaces: list[EntityPermissionModel] = field(default_factory=list)


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        self.keyspace_name = f"{self.app.name.replace('-', '_')}_test"

        self._storage_data = UnitData(
            keyspaces=[
                EntityPermissionModel(
                    resource_name=self.keyspace_name,
                    resource_type="ks",
                    privileges=["ALL"],
                )
            ]
        )

        self._cqlsh_snap = snap.SnapCache()[CQLSH_SNAP_NAME]

        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.install, self._on_install)

        self.cassandra_client = ResourceRequirerEventHandler(
            self,
            "cassandra-client",
            requests=[
                RequirerCommonModel(resource=self.keyspace_name),
                RequirerCommonModel(
                    resource=self.keyspace_name,
                    entity_type="USER",
                    entity_permissions=self._storage_data.keyspaces,
                ),
            ],
            response_model=ResourceProviderModel,
        )

        self.framework.observe(
            self.cassandra_client.on.resource_created,
            self._on_keyspace_created,
        )

        self.framework.observe(
            self.cassandra_client.on.resource_entity_created,
            self._on_keyspace_user_created,
        )

        self.framework.observe(
            self.cassandra_client.on.endpoints_changed,
            self._on_endpoints_changed,
        )

        self.framework.observe(
            self.cassandra_client.on.cassandra_client_relation_created,
            self._cassandra_client_relation_created,
        )

        self.framework.observe(self.on.create_table_action, self._create_table_action)

        self.framework.observe(
            self.on.change_user_permission_action, self._change_user_permission_action
        )

        self.execution_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
        )

    def _on_install(self, _: InstallEvent) -> None:
        self._cqlsh_snap.ensure(snap.SnapState.Present)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _cassandra_client_relation_created(self, _: RelationCreatedEvent) -> None:
        logger.debug("---- _cassandra_client_relation_created ----")

    def _on_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Handle etcd client relation data changed event."""
        if not (peer_relation := self.model.get_relation(PEER_REL)):
            event.defer()
            return

        logger.info("Endpoints changed: %s", event.response.endpoints)
        if not event.response.endpoints:
            logger.error("No endpoints available")
            return

        peer_relation.data[self.app]["hosts"] = str(event.response.endpoints)

    def _on_keyspace_created(self, event: ResourceCreatedEvent[ResourceProviderModel]) -> None:
        self.unit.status = WaitingStatus("handling keyspace created")

        response: ResourceProviderModel = event.response

        rolename = response.username.get_secret_value() if response.username else None
        password = response.password.get_secret_value() if response.password else None

        if not rolename or not password:
            raise Exception("No rolename or password provided in _on_keyspace_created")

        ks = response.resource

        if not (peer_relation := self.model.get_relation(PEER_REL)):
            event.defer()
            return

        peer_relation.data[self.app]["ks_owner_rolename"] = rolename
        peer_relation.data[self.app]["ks_owner_password"] = password

        if response.endpoints:
            peer_relation.data[self.app]["hosts"] = str(event.response.endpoints)

        logger.info(f"Keyspace created: {ks}")
        logger.info(f"Initial user created: {rolename}")

        self.unit.status = ActiveStatus()

    def _on_keyspace_user_created(self, event: ResourceEntityCreatedEvent) -> None:
        self.unit.status = WaitingStatus("handling keyspace user created")

        response: ResourceProviderModel = event.response

        rolename = response.entity_name.get_secret_value() if response.entity_name else None
        password = (
            response.entity_password.get_secret_value() if response.entity_password else None
        )

        if not rolename or not password:
            raise Exception("No rolename or password provided in _on_keyspace_created")

        if not (peer_relation := self.model.get_relation(PEER_REL)):
            event.defer()
            return

        peer_relation.data[self.app]["ks_user_rolename"] = rolename
        peer_relation.data[self.app]["ks_user_password"] = password

        logger.info(f"User {rolename} created with permissions: {self._storage_data.keyspaces}")

        if response.endpoints:
            peer_relation.data[self.app]["hosts"] = str(event.response.endpoints)

        self.unit.status = ActiveStatus()

    def _create_table_action(self, event: ActionEvent) -> None:
        if not (peer_relation := self.model.get_relation(PEER_REL)):
            event.fail("No peer relation")
            return

        self.unit.status = WaitingStatus("creating table")

        rolename = peer_relation.data[self.app]["ks_user_rolename"]
        password = peer_relation.data[self.app]["ks_user_password"]

        if not rolename or not password:
            raise Exception("No rolename or password available in _create_table_action")

        tbl_name = event.params["table-name"]

        logger.info(f"Creating Table: {tbl_name}")
        self._create_table(
            rolename=rolename,
            password=password,
            ks=self.keyspace_name,
            tbl=tbl_name,
            hosts=peer_relation.data[self.app]["hosts"].split(","),
        )
        logger.info(f"Table created: {tbl_name}")

        self.unit.status = ActiveStatus()

    def _change_user_permission_action(self, event: ActionEvent) -> None:
        self.unit.status = WaitingStatus("changing permissions")

        new_permissions = EntityPermissionModel(
            resource_name=self.keyspace_name,
            resource_type="ks",
            privileges=["SELECT", "MODIFY"],
        )

        self._set_keyspace_permissions(new_permissions)

        model = self.cassandra_client.interface.build_model(
            self.cassandra_client.relations[0].id,
            model=RequirerDataContractV1[RequirerCommonModel],
            component=self.app,
        )

        for req in model.requests:
            req.resource = self.keyspace_name
            req.entity_type = "USER"
            req.entity_permissions = self._storage_data.keyspaces

        logger.info(f"Updating entity_permissions: {self._storage_data.keyspaces}")
        self.cassandra_client.interface.write_model(self.cassandra_client.relations[0].id, model)

        self.unit.status = ActiveStatus()

    def _create_table(
        self, rolename: str, password: str, ks: str, tbl: str, hosts: list[str]
    ) -> None:
        cql = f"""
        CREATE TABLE IF NOT EXISTS {ks}.{tbl} (
            id UUID PRIMARY KEY
        )
        """

        with self._cqlsh_session(
            hosts=hosts,
            auth_provider=PlainTextAuthProvider(username=rolename, password=password),
        ) as session:
            logger.debug(f"Query: {cql}")
            session.execute(cql)

    def _set_keyspace_permissions(self, perm: EntityPermissionModel) -> None:
        for i, ks in enumerate(self._storage_data.keyspaces):
            if ks.resource_name == perm.resource_name:
                self._storage_data.keyspaces[i] = perm

    @contextmanager
    def _cqlsh_session(
        self,
        auth_provider: PlainTextAuthProvider,
        hosts: list[str],
        keyspace: str | None = None,
    ) -> Generator[Session, None, None]:
        cluster = Cluster(
            auth_provider=auth_provider,
            contact_points=hosts,
            protocol_version=5,
            execution_profiles={EXEC_PROFILE_DEFAULT: self.execution_profile},
        )
        session = cluster.connect()
        if keyspace:
            session.set_keyspace(keyspace)
        try:
            yield session
        finally:
            session.shutdown()
            cluster.shutdown()


if __name__ == "__main__":
    main(ApplicationCharm)
