#!/usr/bin/env python3
# Copyright 2025 bon
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Application Charm definition."""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
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
    AuthenticationUpdatedEvent,
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
from ops.charm import CharmBase, ConfigChangedEvent
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, WaitingStatus
from ssl import CERT_NONE, CERT_REQUIRED, PROTOCOL_TLS, PROTOCOL_TLS_CLIENT, SSLContext

CQLSH_SNAP_NAME = "cqlsh"
PEER_REL = "local"

logger = logging.getLogger(__name__)


@dataclass
class UnitData:
    """Holds unit-specific data for database access."""

    keyspace: EntityPermissionModel


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    _stored = StoredState()
    def __init__(self, *args):
        super().__init__(*args)

        self._storage_data = UnitData(
            EntityPermissionModel(
                resource_name=self.keyspace_name,
                resource_type="ks",
                privileges=self.user_permissions,
            )
        )

        self._stored.set_default(prev_permissions=self.user_permissions)
        self._cqlsh_snap = snap.SnapCache()[CQLSH_SNAP_NAME]

        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

        self.cassandra_client = ResourceRequirerEventHandler(
            self,
            "cassandra-client",
            requests=[
                RequirerCommonModel(resource=self.keyspace_name),
                RequirerCommonModel(
                    resource=self.keyspace_name,
                    entity_type="USER",
                    entity_permissions=[self._storage_data.keyspace],
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
            self.cassandra_client.on.authentication_updated,
            self._cassandra_client_authentication_updated
        )

        self.framework.observe(self.on.create_table_action, self._create_table_action)

        self.execution_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
        )

    def _on_install(self, _: InstallEvent) -> None:
        self._cqlsh_snap.ensure(snap.SnapState.Present)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _cassandra_client_authentication_updated(self, event: AuthenticationUpdatedEvent[ResourceProviderModel]) -> None:
        tls_ca = event.response.tls_ca
        tls = event.response.tls        

        if tls is None:
            return

        self.set_tls(tls_ca.get_secret_value() if tls_ca else "")

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

        if response.tls_ca:
            self.set_tls(response.tls_ca.get_secret_value())            

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

        logger.info(f"User {rolename} created with permissions: {self._storage_data.keyspace}")

        if response.endpoints:
            peer_relation.data[self.app]["hosts"] = str(event.response.endpoints)

        if response.tls_ca:
            self.set_tls(response.tls_ca.get_secret_value())            

        self.unit.status = ActiveStatus()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        self.unit.status = WaitingStatus("handling config changed event")

        old_perms = set(self._stored.prev_permissions)
        new_perms = set(self.user_permissions)

        if old_perms != new_perms:
            logger.info(f"Changing permissions from: {old_perms} to: {new_perms}")
            self._change_user_permissions(self.user_permissions)
            self._stored.prev_permissions = list(new_perms)
            logger.info("Permissions updated")
        else:
            logger.info("Permissions unchanged")

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

    def _change_user_permissions(self, permissions: list[str]) -> None:
        new_permissions = EntityPermissionModel(
            resource_name=self.keyspace_name,
            resource_type="ks",
            privileges=permissions,
        )

        model = self.cassandra_client.interface.build_model(
            self.cassandra_client.relations[0].id,
            model=RequirerDataContractV1[RequirerCommonModel],
            component=self.app,
        )

        for req in model.requests:
            req.resource = self.keyspace_name
            req.entity_type = "USER"
            req.entity_permissions = [new_permissions]

        logger.info(f"Updating entity_permissions: {new_permissions}")
        self.cassandra_client.interface.write_model(self.cassandra_client.relations[0].id, model)

        self._storage_data.keyspace = new_permissions

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

    def set_tls(self, tls_ca: str) -> None:
        if not (peer_relation := self.model.get_relation(PEER_REL)):
            return

        peer_relation.data[self.app].update({"tls_ca":tls_ca})
            
    @property
    def keyspace_name(self) -> str:
        """Keyspace name."""
        return str(self.config.get("keyspace-name", default=""))

    @property
    def user_permissions(self) -> list[str]:
        """List of permissions for requested user."""
        return str(self.config.get("user-permissions", default="ALL")).split(",")

    @contextmanager
    def _cqlsh_session(
        self,
        auth_provider: PlainTextAuthProvider,
        hosts: list[str],
        keyspace: str | None = None,
    ) -> Generator[Session, None, None]:
        if not (peer_relation := self.model.get_relation(PEER_REL)):
            return

        tls_ca = peer_relation.data[self.app].get("tls_ca")
        ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)

        if tls_ca:
            logger.info(f"Loading SSL context with cert: {tls_ca}")

            # TODO: change for mTLS            
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE

            ssl_context.load_verify_locations(
                cadata=tls_ca
            )
        else:
            logger.info(f"SSL context is disabled")
            ssl_context = None
        
        cluster = Cluster(
            auth_provider=auth_provider,
            contact_points=hosts,
            protocol_version=5,
            execution_profiles={EXEC_PROFILE_DEFAULT: self.execution_profile},
            ssl_context=ssl_context,
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
