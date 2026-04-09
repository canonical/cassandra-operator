#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for main Cassandra charm events."""

import json
import logging
from enum import Enum

from charms.data_platform_libs.v1.data_models import TypedCharmBase
from object_storage import S3Requirer
from ops import (
    ActionEvent,
    Object,
)

from core.config import CharmConfig
from core.literals import CASSANDRA_ADMIN_USERNAME, NODETOOL_USERNAME
from core.state import S3_RELATION, ApplicationState
from core.workload import WorkloadBase
from managers.backup import BackupManager, MedusaConfig
from managers.node import NodeManager
from managers.ssh import SSHManager
from workload import ExecError

logger = logging.getLogger(__name__)


class BackupMessages(str, Enum):
    """Enum for backup/restore messages."""

    NOT_READY = (
        "Can not initiate backup/restore operation. "
        "Check if s3-integrator is in active|idle state, "
        "and if the charm is integrated properly with s3-integrator."
    )
    WORKLOAD_NOT_READY = "Cassandra workload is busy. Wait for active|idle state and try again."
    OP_FAILED = 'Backup/restore operation failed, check "juju debug-log" for more info.'


class BackupEvents(Object):
    """Handle backup and restore actions/events."""

    def __init__(
        self,
        charm: TypedCharmBase[CharmConfig],
        state: ApplicationState,
        workload: WorkloadBase,
        node_manager: NodeManager,
        ssh_manager: SSHManager,
    ):
        super().__init__(charm, key="provider_events")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.node_manager = node_manager
        self.ssh_manager = ssh_manager

        self.s3_client = S3Requirer(self.charm, S3_RELATION)
        self.s3_context = self.state.s3(self.s3_client)
        self.backup_manager = BackupManager(self.workload)

        self.framework.observe(
            getattr(self.charm.on, "create_backup_action"), self._on_create_backup_action
        )
        self.framework.observe(
            getattr(self.charm.on, "list_backups_action"), self._on_list_backups_action
        )
        self.framework.observe(getattr(self.charm.on, "restore_action"), self._on_restore_action)

    def _on_create_backup_action(self, event: ActionEvent) -> None:
        """Handle the `create-backup` Juju action."""
        if not self._run_before_checks(event):
            return

        try:
            backup_name = self.backup_manager.create_backup()
            logger.info(f"Backup succeeded: with backup-id {backup_name}")
            event.set_results({"backup-status": "backup created"})
        except ExecError as e:
            logger.error(f"create-backup failed: {e.stdout} {e.stderr}")
            event.fail(BackupMessages.OP_FAILED.value)

    def _on_list_backups_action(self, event: ActionEvent) -> None:
        """Handle the `list-backups` Juju action."""
        if not self._run_before_checks(event):
            return

        try:
            backups = self.backup_manager.list_backups()
            repo = f"{self.s3_context.endpoint}/{self.s3_context.bucket}"
            event.set_results(
                {
                    "result": json.dumps(
                        [backup.as_da096_dict(repo) for backup in backups], indent=4
                    )
                }
            )
        except ExecError as e:
            logger.error(f"list-backups failed: {e.stdout} {e.stderr}")
            event.fail(BackupMessages.OP_FAILED.value)

    def _on_restore_action(self, event: ActionEvent) -> None:
        """Handle the `restore` Juju action."""
        if not self._run_before_checks(event):
            return

        logger.info(f"restore {repr(event)}")

    def _run_before_checks(self, event: ActionEvent) -> bool:
        if not self.ready:
            logger.error(f"precheck failed: {BackupMessages.NOT_READY.value}")
            event.fail(BackupMessages.NOT_READY.value)
            return False

        if not self._check_units():
            logger.error(f"precheck failed: {BackupMessages.WORKLOAD_NOT_READY.value}")
            event.fail(BackupMessages.WORKLOAD_NOT_READY.value)
            return False

        return True

    def _check_units(self) -> bool:
        """Check if units are in desirable state to initiate backup/restore operations."""
        for unit in self.state.units:
            if not unit.is_ready:
                logger.error(f"Unit {unit.unit_name} is not ready")
                return False

            if not unit.is_operational:
                logger.error(f"Unit {unit.unit_name} is not operational")
                return False

            if unit.peer_tls.rotation or unit.client_tls.rotation:
                logger.error(f"TLS CA rotation is in progress for unit {unit.unit_name}")
                return False

            if not self.node_manager.is_healthy(unit.ip, retry=True):
                logger.error(f"Unit {unit.unit_name} is not healthy")
                return False

        return True

    @property
    def ready(self) -> bool:
        """Runtime readiness check."""
        return all(
            [
                self.workload.installed,
                self.ssh_manager.public_key,
                self.state.cluster.operator_password_secret,
                self.state.cluster.nodetool_password_secret,
                self.s3_context.ready,
            ]
        )

    def reconcile(self) -> None:
        """Reconcile backup/restore-related state."""
        if not self.ready:
            # cleanup if necessary.
            for path in [
                self.workload.cassandra_paths.medusa_config,
                self.workload.cassandra_paths.s3_credentials,
            ]:
                if path.exists():
                    path.unlink()
            return

        cfg = MedusaConfig(
            cql_username=CASSANDRA_ADMIN_USERNAME,
            cql_password=self.state.cluster.operator_password_secret,
            nodetool_username=NODETOOL_USERNAME,
            nodetool_password=self.state.cluster.nodetool_password_secret,
            s3_bucket=self.s3_context.bucket,
            s3_endpoint="",
            s3_region=self.s3_context.region,
        )

        self.backup_manager.render_credentials(
            self.s3_context.access_key, self.s3_context.secret_key
        )
        self.backup_manager.render_medusa_config(cfg)
