#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for main Cassandra charm events."""

import logging
from typing import Callable

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from ops import (
    CollectStatusEvent,
    ConfigChangedEvent,
    EventBase,
    InstallEvent,
    LeaderElectedEvent,
    Object,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
    SecretChangedEvent,
    StartEvent,
    StorageDetachingEvent,
    UpdateStatusEvent,
)
from pydantic import ValidationError
from tenacity import (
    Retrying,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

from common.exceptions import BadSecretError, ExecError
from core.config import CharmConfig
from core.literals import CASSANDRA_ADMIN_USERNAME
from core.state import (
    DATA_STORAGE,
    PEER_RELATION,
    ApplicationState,
    AuthRepairState,
    UnitWorkloadState,
)
from core.statuses import Status
from core.workload import WorkloadBase
from managers.config import ConfigManager
from managers.database import DatabaseManager
from managers.node import NodeManager
from managers.refresh import RefreshManager
from managers.tls import Sans, TLSManager

logger = logging.getLogger(__name__)


class CassandraEvents(Object):
    """Handler for main Cassandra charm events."""

    def __init__(
        self,
        charm: TypedCharmBase[CharmConfig],
        state: ApplicationState,
        workload: WorkloadBase,
        node_manager: NodeManager,
        config_manager: ConfigManager,
        tls_manager: TLSManager,
        refresh_manager: RefreshManager,
        setup_internal_certificates: Callable[[Sans], bool],
        database_manager: DatabaseManager,
        read_auth_secret: Callable[[str], str],
        restart: Callable[[], None],
    ):
        super().__init__(charm, key="cassandra_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.node_manager = node_manager
        self.config_manager = config_manager
        self.tls_manager = tls_manager
        self.setup_internal_certificates = setup_internal_certificates
        self.database_manager = database_manager
        self.read_auth_secret = read_auth_secret
        self.restart = restart
        self.refresh_manager = refresh_manager

        self.charm.on.define_event("update_auth_rf", EventBase)
        self.framework.observe(self.charm.on.update_auth_rf, self._on_update_auth_rf)

        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_joined, self._on_peer_relation_joined
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed, self._on_peer_relation_changed
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_departed, self._on_peer_relation_departed
        )
        self.framework.observe(self.charm.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        self.framework.observe(self.charm.on.collect_unit_status, self._on_collect_unit_status)
        self.framework.observe(self.charm.on.collect_app_status, self._on_collect_app_status)
        self.framework.observe(
            self.charm.on[DATA_STORAGE].storage_detaching, self._on_storage_detaching
        )

    def _acquire_operator_password(self) -> str:
        if self.charm.config.system_users:
            return self.read_auth_secret(self.charm.config.system_users)
        if self.state.cluster.operator_password_secret:
            return self.state.cluster.operator_password_secret
        return self.workload.generate_string()

    def _on_install(self, _: InstallEvent) -> None:
        self.workload.install()

    def _on_start(self, event: StartEvent) -> None:
        self._update_network_address()

        if not self.refresh_manager.is_initialized:
            logger.debug("Deferring on_start due to charm_refresh is not ready")
            event.defer()
            return

        # don't want to run default start/pebble-ready events during upgrades
        if self.refresh_manager.in_progress:
            return

        if self.state.unit.workload_state == UnitWorkloadState.ACTIVE:
            logger.debug("Unintended restart detected, resetting unit's workload state")
            self.state.unit.workload_state = UnitWorkloadState.WAITING_FOR_START

        if not self.setup_internal_certificates(
            self.tls_manager.build_sans(
                sans_ip=[self.state.unit.ip],
                sans_dns=[self.state.unit.hostname, self.charm.unit.name],
            )
        ):
            logger.debug("Deferring on_start due to internal certificates setup")
            event.defer()
            return

        try:
            self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None,
            )
        except ValidationError as e:
            logger.debug(f"Deferring on_start due to config not passing validation yet: {e}")
            event.defer()
            return

        if self.charm.unit.is_leader():
            self._start_leader(event)
        else:
            self._start_subordinate(event)

    def _start_leader(self, event: StartEvent) -> None:
        if not self.state.seed_units:
            self.state.seed_units = self.state.unit

        if not self._are_seeds_reachable:
            logger.debug("Deferring leader on_start due to seeds not being ready")
            event.defer()
            return

        if not self.state.cluster.operator_password_secret:
            try:
                self._start_leader_setup_auth()
            except BadSecretError as e:
                logger.debug(
                    f"Deferring leader on_start due to auth secret not passing validation: {e}"
                )
                event.defer()
                return

        self.config_manager.render_cassandra_config(
            listen_address=self.state.unit.ip,
            seeds=self.state.cluster.seeds,
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
        )
        self.restart()

    def _start_leader_setup_auth(self) -> None:
        password = self._acquire_operator_password()

        self.config_manager.render_cassandra_config(
            listen_address="127.0.0.1",
            seeds={"127.0.0.1:7000"},
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
        )
        self.workload.start()
        logger.info("Trying to check heath in _start_leader_setup_auth")
        for attempt in Retrying(
            wait=wait_exponential(), stop=stop_after_delay(1800), reraise=True
        ):
            with attempt:
                if not self.node_manager.is_healthy(ip="127.0.0.1"):
                    raise Exception("bootstrap timeout exceeded")

        logger.info("Heath in _start_leader_setup_auth is good")
        for attempt in Retrying(wait=wait_fixed(10), stop=stop_after_delay(120), reraise=True):
            with attempt:
                self.database_manager.init_admin(password)
        self.state.cluster.operator_password_secret = password

        self.node_manager.prepare_shutdown()

    def _start_subordinate(self, event: StartEvent) -> None:
        if not self.state.cluster.is_active and not self.state.unit.is_seed:
            self.state.unit.workload_state = UnitWorkloadState.WAITING_FOR_START
            logger.debug("Deferring subordinate on_start due to cluster not being active yet")
            event.defer()
            return

        if not self._are_seeds_reachable:
            self.state.unit.workload_state = UnitWorkloadState.WAITING_FOR_START
            logger.debug("Deferring subordinate on_start due to seeds not being ready")
            event.defer()
            return

        self.config_manager.render_cassandra_config(
            listen_address=self.state.unit.ip,
            enable_peer_tls=self.state.unit.peer_tls.ready,
            enable_client_tls=self.state.unit.client_tls.ready,
            keystore_password=self.state.unit.keystore_password,
            truststore_password=self.state.unit.truststore_password,
        )
        self.restart()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        if not self.refresh_manager.ready:
            return

        if not self.state.unit.is_ready:
            logger.debug("Exiting on_config_changed due to unit not being ready")
            return
        if not self.state.unit.is_config_change_eligible:
            logger.debug("Deferring on_config_changed due to unit not being ready change config")
            event.defer()
            return
        try:
            if self.charm.unit.is_leader() and self.state.cluster.operator_password_secret != (
                password := self._acquire_operator_password()
            ):
                if self.state.unit.is_operational:
                    self.database_manager.update_role_password(CASSANDRA_ADMIN_USERNAME, password)
                    self.state.cluster.operator_password_secret = password
                else:
                    logger.debug("Deferring on_config_changed due to unit not being operational")
                    event.defer()
            env_changed = self.config_manager.render_env(
                cassandra_limit_memory_mb=1024 if self.charm.config.profile == "testing" else None
            )
            cassandra_config_changed = self.config_manager.render_cassandra_config()
            if env_changed or cassandra_config_changed:
                self.restart()
        except ValidationError as e:
            logger.error(f"Config haven't passed validation: {e}")
            return
        except BadSecretError as e:
            logger.error(
                f"Deferring on_config_changed due to auth secret not passing validation: {e}"
            )
            event.defer()
            return

    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            return

        if not self.state.unit.is_ready:
            logger.debug("Exiting on_secret_changed due to unit not being ready")
            return

        if not self.refresh_manager.ready:
            logger.debug("Deferring on_secret_changed due to charm_refresh is not ready")
            event.defer()
            return

        try:
            if event.secret.id != self.charm.config.system_users:
                return

            if not self.state.unit.is_operational:
                logger.debug("Deferring on_secret_changed due to unit not being operational")
                event.defer()
                return

            if self.state.cluster.operator_password_secret != (
                password := self._acquire_operator_password()
            ):
                self.database_manager.update_role_password(CASSANDRA_ADMIN_USERNAME, password)
                self.state.cluster.operator_password_secret = password
        except ValidationError as e:
            logger.error(f"Config haven't passed validation: {e}")
            return
        except BadSecretError as e:
            logger.error(f"Secret haven't passed validation: {e}")
            return

    def _on_peer_relation_joined(self, event: RelationJoinedEvent) -> None:
        if event.unit == self.charm.unit or not self.charm.unit.is_leader():
            return
        self.charm.on.update_auth_rf.emit()

    def _on_peer_relation_changed(self, event: RelationChangedEvent) -> None:
        if not self.state.unit.is_ready:
            logger.debug("Exiting on_peer_relation_changed due to unit not being ready")
            return
        if not self.state.unit.is_config_change_eligible:
            logger.debug(
                "Deferring on_peer_relation_changed due to unit not being ready change config"
            )
            event.defer()
            return
        if self.config_manager.render_cassandra_config():
            self.restart()
        if not self._repair_auth(event):
            event.defer()
            return

    def _on_peer_relation_departed(self, event: RelationDepartedEvent) -> None:
        if not self.state.unit.is_ready:
            logger.debug("Exiting on_peer_relation_departed due to unit not being ready")
            return

        if event.departing_unit == self.charm.unit:
            return

        if self.charm.unit.is_leader():
            if not self.state.unit.is_config_change_eligible:
                logger.debug(
                    "Deferring on_peer_relation_departed due to unit not being ready change config"
                )
                event.defer()
                return
            self._recover_seeds()
            self.charm.on.update_auth_rf.emit()

        if not self._repair_auth(event):
            event.defer()
            return

    def _on_leader_elected(self, event: LeaderElectedEvent) -> None:
        if not self.state.unit.is_ready:
            logger.debug("Exiting on_leader_elected due to unit not being ready")
            return
        if not self.state.unit.is_config_change_eligible:
            logger.debug("Deferring on_leader_elected due to unit not being ready change config")
            event.defer()
            return
        self._recover_seeds()
        self.charm.on.update_auth_rf.emit()
        if not self._repair_auth(event):
            event.defer()
            return

    def _on_update_status(self, _: UpdateStatusEvent) -> None:
        if not self.refresh_manager.ready:
            logger.error("Refresh manager is not ready")
            return

        if self.state.unit.is_operational and not self.workload.is_alive():
            logger.error("Restarting Cassandra service due to unexpected shutdown")
            self.restart()
            return

        if self.state.unit.workload_state == UnitWorkloadState.CANT_START:
            logger.error("Restarting Cassandra service again")
            self.restart()
            return

        if not self.state.unit.is_operational:
            logger.debug("Exiting on_update_status due to unit not being operational")
            return

        if self._update_network_address():
            if self.charm.unit.is_leader():
                self.state.seed_units = self.state.unit
            self.config_manager.render_cassandra_config(
                listen_address=self.state.unit.ip,
                seeds=self.state.cluster.seeds,
            )
            self.restart()
            return

        if self.charm.unit.is_leader():
            self.node_manager.remove_bad_nodes([unit.ip for unit in self.state.units])

    def _on_collect_unit_status(self, event: CollectStatusEvent) -> None:
        if (
            self.refresh_manager.is_initialized
            and self.refresh_manager.unit_status_higher_priority
        ):
            event.add_status(self.refresh_manager.unit_status_higher_priority)
            return

        try:
            self.charm.config
            self._acquire_operator_password()
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)
        except BadSecretError:
            event.add_status(Status.INVALID_SYSTEM_USERS_SECRET.value)

        if self.state.unit.workload_state == UnitWorkloadState.INSTALLING:
            event.add_status(Status.INSTALLING.value)

        self._collect_unit_tls_status(event)

        if (
            self.state.unit.workload_state == UnitWorkloadState.WAITING_FOR_START
            and not self.charm.unit.is_leader()
            and not self.state.cluster.is_active
        ):
            event.add_status(Status.WAITING_FOR_CLUSTER.value)

        if self.state.unit.workload_state in [
            UnitWorkloadState.WAITING_FOR_START,
            UnitWorkloadState.STARTING,
        ] and (self.charm.unit.is_leader() or self.state.cluster.is_active):
            event.add_status(Status.STARTING.value)

        if self.state.unit.workload_state == UnitWorkloadState.CANT_START:
            event.add_status(Status.CANT_START.value)

        if self.state.cluster.auth_repair != AuthRepairState.UNPLANNED:
            event.add_status(Status.REPAIRING_AUTH.value)

        if self.refresh_manager.is_initialized and (
            refresh_status := self.refresh_manager.unit_status_lower_priority(
                workload_is_running=self.workload.is_alive(),
            )
        ):
            event.add_status(refresh_status)
            return

        event.add_status(Status.ACTIVE.value)

    def _collect_unit_tls_status(self, event: CollectStatusEvent) -> None:
        if self.state.unit.peer_tls.ready and self.state.unit.peer_tls.rotation:
            event.add_status(Status.ROTATING_PEER_TLS.value)

        if self.state.unit.client_tls.ready and self.state.unit.client_tls.rotation:
            event.add_status(Status.ROTATING_CLIENT_TLS.value)

        if not self.state.cluster.internal_ca:
            event.add_status(Status.WAITING_FOR_INTERNAL_TLS.value)

        if self.state.cluster.tls_state and not self.tls_manager.client_tls_ready:
            event.add_status(Status.WAITING_FOR_TLS.value)

    def _on_collect_app_status(self, event: CollectStatusEvent) -> None:
        try:
            self.charm.config
            self._acquire_operator_password()
        except ValidationError:
            event.add_status(Status.INVALID_CONFIG.value)
        except BadSecretError:
            event.add_status(Status.INVALID_SYSTEM_USERS_SECRET.value)

        event.add_status(Status.ACTIVE.value)

    @property
    def _are_seeds_reachable(self) -> bool:
        return self.state.unit.is_seed or any(
            unit.is_operational and self.database_manager.check(hosts=[unit.ip])
            for unit in self.state.other_seed_units
        )

    def _recover_seeds(self) -> None:
        if not self.state.seed_units:
            self.state.seed_units = self.state.unit
            self.config_manager.render_cassandra_config(seeds=self.state.cluster.seeds)
            self.restart()

    def _update_network_address(self) -> bool:
        """Update hostname & ip in this unit context.

        Returns:
            whether the hostname or ip changed.
        """
        old_ip = self.state.unit.ip
        old_hostname = self.state.unit.hostname
        self.state.unit.ip, self.state.unit.hostname = self.node_manager.network_address()
        return (
            bool(old_ip)
            and bool(old_hostname)
            and (old_ip != self.state.unit.ip or old_hostname != self.state.unit.hostname)
        )

    def _on_storage_detaching(self, _: StorageDetachingEvent) -> None:
        if self.node_manager.is_bootstrap_decommissioning:
            logger.warning("Node is already decommissioned")
            return

        if not self.node_manager.is_healthy(self.state.unit.ip):
            raise Exception("Cluster is not healthy, cannot remove unit")

        if self.charm.app.planned_units() < len(self.state.units) - 1:
            logger.warning(
                """More than one unit removing at a time is not supported.
                   The charm may be in a broken, unrecoverable state"""
            )

        if self.charm.app.planned_units() == 1:
            logger.info("Updating system_auth replication factor to 1 accordingly")
            try:
                self.database_manager.update_system_auth_replication_factor(1)
            except Exception as e:
                logger.error(f"Failed to update system_auth replication factor: {e}")

        logger.info(f"Starting unit {self.state.unit.unit_name} node decommissioning")
        try:
            self.node_manager.decommission()
        except ExecError as e:
            logger.error(f"Failed to decommission unit: {e}")
            raise e

        logger.info("Hook for storage-detaching event completed")

    def _on_update_auth_rf(self, event: EventBase) -> None:
        if not self.charm.unit.is_leader():
            return
        self.state.cluster.auth_repair = AuthRepairState.PENDING
        if not self.state.unit.is_operational:
            logger.debug("Deferring on_peer_relation_departed due to unit not being operational")
            event.defer()
            return
        if not self.node_manager.ensure_cluster_topology(n := self.charm.app.planned_units()):
            event.defer()
            return
        self.database_manager.update_system_auth_replication_factor(min(n, 3))
        self.node_manager.repair_auth()
        self.state.unit.auth_repaired = True
        self.state.cluster.auth_repair = AuthRepairState.WAITING_FOR_REPAIR

    def _repair_auth(self, event: EventBase) -> bool:
        if self.charm.unit.is_leader():
            if self.state.cluster.auth_repair == AuthRepairState.WAITING_FOR_REPAIR and all(
                unit.auth_repaired for unit in self.state.units
            ):
                self.state.cluster.auth_repair = AuthRepairState.UNPLANNED
                self.state.unit.auth_repaired = False
        else:
            if self.state.cluster.auth_repair == AuthRepairState.WAITING_FOR_REPAIR:
                if not self.state.unit.auth_repaired:
                    if not self.state.unit.is_operational:
                        logger.debug(
                            "Deferring on_peer_relation_changed due to unit not being operational"
                        )
                        event.defer()
                        return False
                    self.node_manager.repair_auth()
                    self.state.unit.auth_repaired = True
            else:
                self.state.unit.auth_repaired = False
        return True
