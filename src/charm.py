#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm definition."""

import logging

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from ops import EventBase, ModelError, SecretNotFoundError, main

from common.exceptions import BadSecretError, ExecError
from common.lock_manager import LockManager
from core.config import CharmConfig
from core.literals import CASSANDRA_ADMIN_USERNAME
from core.state import (
    JMX_EXPORTER_PORT,
    METRICS_RULES_DIR,
    ApplicationState,
    ClusterState,
    UnitWorkloadState,
)
from events.cassandra import CassandraEvents
from events.tls import TLSEvents
from managers.config import ConfigManager
from managers.database import DatabaseManager
from managers.node import NodeManager
from managers.tls import Sans, TLSManager
from workload import SNAP_NAME, CassandraWorkload

logger = logging.getLogger(__name__)


class CassandraCharm(TypedCharmBase[CharmConfig]):
    """Application charm."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.on.define_event("bootstrap", EventBase)
        self.framework.observe(self.on.bootstrap, self._on_bootstrap)

        self.state = ApplicationState(self)
        self.workload = CassandraWorkload()
        self.node_manager = NodeManager(workload=self.workload)
        self.tls_manager = TLSManager(workload=self.workload)

        config_manager = ConfigManager(
            workload=self.workload,
            cluster_name=self.app.name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
            authentication=True,
        )
        database_manager = DatabaseManager(
            workload=self.workload,
            tls_manager=self.tls_manager,
            hosts=[self.state.unit.ip],
            user=CASSANDRA_ADMIN_USERNAME,
            password=self.state.cluster.operator_password_secret,
        )
        self.bootstrap_manager = LockManager(self, "bootstrap")

        self.cassandra_events = CassandraEvents(
            self,
            state=self.state,
            workload=self.workload,
            node_manager=self.node_manager,
            config_manager=config_manager,
            database_manager=database_manager,
            tls_manager=self.tls_manager,
            setup_internal_certificates=self.setup_internal_certificates,
            read_auth_secret=self.read_auth_secret,
            restart=self.restart,
        )

        self.tls_events = TLSEvents(
            self,
            state=self.state,
            workload=self.workload,
            node_manager=self.node_manager,
            config_manager=config_manager,
            tls_manager=self.tls_manager,
            setup_internal_certificates=self.setup_internal_certificates,
            restart=self.restart,
        )

        self._grafana_agent = COSAgentProvider(
            self,
            metrics_endpoints=[
                {"path": "/metrics", "port": JMX_EXPORTER_PORT},
            ],
            metrics_rules_dir=METRICS_RULES_DIR,
            log_slots=[f"{SNAP_NAME}:logs"],
        )

    def _on_bootstrap(self, event: EventBase) -> None:
        if self.state.unit.workload_state != UnitWorkloadState.STARTING:
            if self.bootstrap_manager.try_lock():
                logger.debug("Bootstrap lock is acquired")
                if self.workload.is_alive():
                    logger.debug("Gracefully shutting down an active workload")
                    try:
                        self.node_manager.prepare_shutdown()
                    except ExecError as e:
                        logger.error(f"Failed to prepare workload shutdown during restart: {e}")
                self.workload.restart()
                self.state.unit.workload_state = UnitWorkloadState.STARTING
            event.defer()
            return

        if not self._on_bootstrap_pending_check():
            # TODO: determine whether we need removing var/lib/cassandra/* and in which cases.
            logger.error(
                "Releasing the bootstrap exclusive lock and migrating to CANT_START workload state"
            )
            self.state.unit.workload_state = UnitWorkloadState.CANT_START
            self.bootstrap_manager.release()
            return

        if not self.node_manager.is_healthy(ip=self.state.unit.ip):
            logger.debug("Deferring on_bootstrap due to workload not being healthy yet")
            event.defer()
            return

        logger.debug("Releasing the exclusive lock after successful bootstrap")
        self.bootstrap_manager.release()

        if self.state.unit.peer_tls.rotation:
            self.state.unit.peer_tls.rotation = False
        if self.state.unit.client_tls.rotation:
            self.state.unit.client_tls.rotation = False
        self.state.unit.workload_state = UnitWorkloadState.ACTIVE
        if self.unit.is_leader():
            self.state.cluster.state = ClusterState.ACTIVE

    def _on_bootstrap_pending_check(self) -> bool:
        if not self.workload.is_alive():
            logger.error("Cassandra service abruptly stopped during bootstrap")
            return False

        if self.node_manager.is_bootstrap_pending:
            logger.warning("Pending Cassandra bootstrap is detected, trying to resume")
            if self.node_manager.resume_bootstrap():
                logger.info("Cassandra bootstrap resuming successful")
                return True
            else:
                logger.error("Cassandra bootstrap resuming failed")
                return False

        if self.node_manager.is_bootstrap_in_unknown_state:
            logger.error("Cassandra bootstrap is in unknown state, failed bootstrap assumed")
            return False

        return True

    def restart(self) -> None:
        """Restart Cassandra service."""
        if not self.bootstrap_manager.is_active:
            self.on.bootstrap.emit()
        else:
            logger.debug("Restart request was skipped as unit already bootstrapping")

    def setup_internal_certificates(self, sans: Sans) -> bool:
        """Configure internal TLS certificates for the current unit using an internally managed CA.

        Args:
            sans (Sans): Subject Alternative Names to include in the generated certificate.

        Returns:
            bool: True if the internal certificates were successfully configured, False otherwise.
        """
        if not self.state.unit.peer_tls.ready and not self.state.cluster.internal_ca:
            if not self.unit.is_leader():
                return False

            ca, pk = self.tls_manager.generate_internal_ca(
                common_name=self.state.unit.unit.app.name
            )

            self.state.cluster.internal_ca = ca
            self.state.cluster.internal_ca_key = pk

        if not self.state.cluster.internal_ca or not self.state.cluster.internal_ca_key:
            logger.warning("Internal CA is not ready yet")
            return False

        if not self.state.unit.peer_tls.ready:
            provider_crt, pk = self.tls_manager.generate_internal_credentials(
                ca=self.state.cluster.internal_ca,
                ca_key=self.state.cluster.internal_ca_key,
                unit_key=self.state.unit.peer_tls.private_key,
                common_name=self.state.unit.unit.name,
                sans_ip=frozenset(sans.sans_ip),
                sans_dns=frozenset(sans.sans_dns),
            )

            self.state.unit.peer_tls.certificate = provider_crt.certificate
            self.state.unit.peer_tls.csr = provider_crt.certificate_signing_request
            self.state.unit.peer_tls.chain = provider_crt.chain
            self.state.unit.peer_tls.private_key = pk
            self.state.unit.peer_tls.ca = self.state.cluster.internal_ca

        self.tls_manager.configure(
            self.state.unit.peer_tls.resolved,
            keystore_password=self.state.unit.keystore_password,
            trust_password=self.state.unit.truststore_password,
        )

        return True

    def read_auth_secret(self, secret_id: str) -> str:
        """Read and validate user-defined authentication secret.

        Returns:
            operator password.
        """
        try:
            if (
                password := self.model.get_secret(id=secret_id)
                .get_content(refresh=True)
                .get(CASSANDRA_ADMIN_USERNAME)
            ):
                return password
            else:
                logger.error(
                    "User-defined system users secret doesn't contain"
                    f" `{CASSANDRA_ADMIN_USERNAME}` field"
                )
                raise BadSecretError()
        except SecretNotFoundError:
            logger.error("Cannot find user-defined system users secret")
            raise BadSecretError()
        except ModelError as e:
            logger.error(f"Error accessing user-defined system users secret: {e}")
            raise BadSecretError()


if __name__ == "__main__":  # pragma: nocover
    main(CassandraCharm)
