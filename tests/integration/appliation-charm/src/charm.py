#!/usr/bin/env python3
# Copyright 2025 bon
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk
from dataclasses import dataclass
import logging
import json
from contextlib import contextmanager

from ops import RelationChangedEvent, InstallEvent, RelationCreatedEvent, StoredState
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus
from pydantic import SecretStr
from charms.operator_libs_linux.v2 import snap
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
    ResourceProviderModel,
    ResourceRequirerEventHandler,
    ResourceEndpointsChangedEvent,
    UserSecretStr,
)

CQLSH_SNAP_NAME = "cqlsh"

logger = logging.getLogger(__name__)

@dataclass
class UnitData:
    keyspaces: list[EntityPermissionModel] = []
    rolename: str = ""
    password: str = ""

class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""
    _stored = StoredState()        

    def __init__(self, *args):
        super().__init__(*args)

        keyspace_name = f"{self.app.name.replace('-', '_')}_test"        

        self._data = UnitData(keyspaces=[
            EntityPermissionModel(
                resource_name=keyspace_name,
                resource_type="ks",
                privileges=["ALL"],
            )
        ])

        self._cqlsh_snap = snap.SnapCache()[CQLSH_SNAP_NAME]

        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.install, self._on_install)        

        self.cassandra_client = ResourceRequirerEventHandler(
            self,
            "cassandra-client",
            requests=[RequirerCommonModel(resource=keyspace_name, entity_permissions=self._data.keyspaces)],
            response_model=ResourceProviderModel,
        )

        self.framework.observe(
            self.cassandra_client.on.resource_created, self._on_keyspace_created,
        )

        self.framework.observe(
            self.cassandra_client.on.resource_entity_created, self._on_keyspace_user_created,
        )

        self.framework.observe(
            self.cassandra_client.on.endpoints_changed, self._on_endpoints_changed,
        )

        self.execution_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
        )

    def _on_install(self, _: InstallEvent) -> None:
        self._cqlsh_snap.ensure(snap.SnapState.Present)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _cassandra_client_relation_created(self, event: RelationCreatedEvent) -> None:
        logger.debug(f"---- _cassandra_client_relation_created ----")

        self.cassandra_client._request_model()
        
    def _on_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Handle etcd client relation data changed event."""
        logger.info("Endpoints changed: %s", event.response.endpoints)
        if not event.response.endpoints:
            logger.error("No endpoints available")
            return        

    def _on_keyspace_created(self, event: ResourceCreatedEvent) -> None:
        ks = event.response.resource
        self._data.keyspaces.append(EntityPermissionModel(
            resource_name=ks,
            resource_type="ks",
            privileges=[],
        ))
        logger.info(f"Cassandra keyspace created: {ks}")

    def _on_keyspace_user_created(self, event: ResourceCreatedEvent) -> None:
        response: ResourceProviderModel = event.response

        rolename: UserSecretStr = response.username
        password: UserSecretStr = response.password

        if not rolename or not password:
            logger.warning(f"No rolename or password provided in _on_keyspace_user_created")
            return

        logger.info(f"""
        Cassandra user created:
        rolename: {rolename.get_secret_value()},
        password: {password.get_secret_value()},
        keyspace: {event.response.resource}
        """)

        tbl_name = f"{self.app.name.replace('-', '_')}_table"
        logger.debug(f"Creating Table: {tbl_name}")
        self._create_test_table(
            rolename=rolename.get_secret_value(),
            password=password.get_secret_value(),
            ks=event.response.resource,
            tbl=tbl_name,
            hosts=str(event.response.endpoints).split(","),
        )
        logger.debug(f"Table created: {tbl_name}")

        logger.debug("Changing entity permissions")

        new_permissions = EntityPermissionModel(
            resource_name=event.response.resource,
            resource_type="ks",
            privileges=["SELECT", "MODIFY"],
        )

        self._set_keyspace_permissions(new_permissions)

        model = self.cassandra_client.interface.build_model(
            event.relation.id, model=RequirerDataContractV1[RequirerCommonModel]
        )

        for request in model.requests:
            request.entity_permissions = self._data.keyspaces

        logger.debug(f"Updating entity_permissions: {json.dumps(self._data.keyspaces)}")
        self.cassandra_client.interface.write_model(event.relation.id, model)

    def _create_test_table(self, rolename: str, password: str, ks: str, tbl: str, hosts: list[str]) -> None:

        cql = f"""
        CREATE TABLE IF NOT EXISTS {ks}.{tbl} (
            id UUID PRIMARY KEY
        )
        """        
        
        with self._cqlsh_session(
            hosts=hosts,
            auth_provider=PlainTextAuthProvider(
                username=rolename, password=password
            ),
        ) as session:
            logger.debug(f"Query: {cql}")            
            session.execute(cql)

    def _set_keyspace_permissions(self, perm: EntityPermissionModel) -> None:
        for i, ks in enumerate(self._data.keyspaces):
            if ks.resource_name == perm.resource_name:
                self._data.keyspaces[i] = perm

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
