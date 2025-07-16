#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm definition."""

import logging

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.rolling_ops.v0.rollingops import RollingOpsManager, RunWithLock
from ops import main
from tenacity import Retrying, stop_after_delay, wait_exponential

from core.config import CharmConfig
from core.state import (
    JMX_EXPORTER_PORT,
    METRICS_RULES_DIR,
    ApplicationState,
    ClusterState,
    UnitWorkloadState,
)
from events.cassandra import CassandraEvents
from events.tls import TLSEvents
from managers.cluster import ClusterManager
from managers.config import ConfigManager
from managers.database import DatabaseManager
from managers.tls import Sans, TLSManager
from workload import SNAP_NAME, CassandraWorkload

logger = logging.getLogger(__name__)


class CassandraCharm(TypedCharmBase[CharmConfig]):
    """Application charm."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.state = ApplicationState(self)
        self.workload = CassandraWorkload()
        self.cluster_manager = ClusterManager(workload=self.workload)
        self.tls_manager = TLSManager(workload=self.workload)

        config_manager = ConfigManager(
            workload=self.workload,
            cluster_name=self.state.cluster.cluster_name,
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            enable_peer_tls=False,
            enable_client_tls=False,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
            authentication=bool(self.state.cluster.cassandra_password_secret),
        )
        database_manager = DatabaseManager(
            hosts=[
                "127.0.0.1"
                if self.state.unit.workload_state == UnitWorkloadState.CHANGING_PASSWORD
                else self.state.unit.ip
            ],
            user="cassandra",
            password=self.state.cluster.cassandra_password_secret,
        )
        bootstrap_manager = RollingOpsManager(
            charm=self, relation="bootstrap", callback=self.bootstrap
        )

        self.cassandra_events = CassandraEvents(
            self,
            state=self.state,
            workload=self.workload,
            cluster_manager=self.cluster_manager,
            config_manager=config_manager,
            database_manager=database_manager,
            bootstrap_manager=bootstrap_manager,
            tls_manager=self.tls_manager,
            configure_certificates=self.configure_internal_certificates,
        )

        self.tls_events = TLSEvents(
            self,
            state=self.state,
            workload=self.workload,
            cluster_manager=self.cluster_manager,
            tls_manager=self.tls_manager,
            configure_certificates=self.configure_internal_certificates,
        )

        self._grafana_agent = COSAgentProvider(
            self,
            metrics_endpoints=[
                {"path": "/metrics", "port": JMX_EXPORTER_PORT},
            ],
            metrics_rules_dir=METRICS_RULES_DIR,
            log_slots=[f"{SNAP_NAME}:logs"],
        )

    def bootstrap(self, event: RunWithLock) -> None:
        """Start workload and join this unit to the cluster."""
        # TODO: add leader elected hook
        self.state.unit.workload_state = UnitWorkloadState.STARTING

        self.workload.restart()

        for _ in Retrying(wait=wait_exponential(), stop=stop_after_delay(1800)):
            if self.cluster_manager.is_healthy:
                self.state.unit.workload_state = UnitWorkloadState.ACTIVE
                if self.unit.is_leader():
                    self.state.cluster.state = ClusterState.ACTIVE
                return

        raise Exception("bootstrap timeout exceeded")

    def configure_internal_certificates(self, sans: Sans) -> bool:
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


if __name__ == "__main__":  # pragma: nocover
    main(CassandraCharm)
