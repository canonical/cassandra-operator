#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import time
from pathlib import Path

import jubilant
import pytest
import tenacity

from events.backup import BackupMessages
from integration.helpers.cassandra import OPERATOR_PASSWORD, connect_cql
from integration.helpers.juju import all_active_idle, app_secret_extract, get_hosts

logger = logging.getLogger(__name__)


class BackupRestoreTests:
    """Generic backup & restore test suite."""

    n_recs: int = 1000
    test_ks: str = "keyspace1"
    test_tbl: str = "standard1"
    integrator_app: str = "integrator"

    def test_deploy_active(
        self, juju: jubilant.Juju, cassandra_charm: Path, app_name: str
    ) -> None:
        juju.deploy(
            cassandra_charm,
            app=app_name,
            config={"profile": "testing"},
            num_units=3,
        )

        juju.wait(all_active_idle, timeout=1800)

    def test_run_actions_before_storage_integration_fails(
        self, juju: jubilant.Juju, app_name: str
    ):
        with pytest.raises(jubilant.TaskError) as exc:
            juju.run(f"{app_name}/0", "create-backup")

        assert exc.value.task.message == BackupMessages.NOT_READY.value

        with pytest.raises(jubilant.TaskError) as other_exc:
            juju.run(f"{app_name}/0", "list-backups")

        assert other_exc.value.task.message == BackupMessages.NOT_READY.value

    def test_load_some_data(self, juju: jubilant.Juju, app_name: str):
        hosts = get_hosts(juju, app_name)
        password = app_secret_extract(juju, app_name, OPERATOR_PASSWORD)
        juju.ssh(
            f"{app_name}/0",
            (
                "sudo charmed-cassandra.stress "
                f"write n={self.n_recs} "
                f"-node {hosts[0]} "
                "-mode native cql3 "
                "user=operator "
                f"password={password}"
            ),
        )

        with connect_cql(
            hosts=hosts, password=password, username="operator", keyspace=self.test_ks
        ) as session:
            rs = session.execute(f"SELECT COUNT(*) FROM {self.test_ks}.{self.test_tbl}")
            assert rs.current_rows[0].count == self.n_recs

    def test_storage_integration(self, juju: jubilant.Juju, app_name: str):
        juju.integrate(app_name, self.integrator_app)
        juju.wait(all_active_idle)

    def test_list_backups_succeeds_and_is_empty(self, juju: jubilant.Juju, app_name: str):
        task = juju.run(f"{app_name}/0", "list-backups")
        assert task.results.get("result") == "[]"

    def test_create_backup(self, juju: jubilant.Juju, app_name: str):
        juju.wait(all_active_idle)
        task = juju.run(f"{app_name}/0", "create-backup", wait=1200)
        assert task.results.get("backup-status") == "backup created"

    def test_list_backups_on_another_unit(self, juju: jubilant.Juju, app_name: str):
        juju.wait(all_active_idle, successes=10, delay=3)
        task = None
        for attempt in tenacity.Retrying(
            wait=tenacity.wait_fixed(10), stop=tenacity.stop_after_attempt(6), reraise=True
        ):
            with attempt:
                task = juju.run(f"{app_name}/1", "list-backups")

        if not task:
            raise RuntimeError("list-backups failed!")

        backups = json.loads(task.results.get("result", "[]"))
        assert len(backups) == 1
        assert backups[0].get("start-time") < backups[0].get("end-time")
        logger.info(f"One backup found: {backups[0]['id']}")

    def test_remove_first_app(self, juju: jubilant.Juju, app_name: str):
        juju.remove_relation(app_name, self.integrator_app)
        juju.wait(all_active_idle, successes=10, delay=3)
        destroy_cmd = [
            "juju",
            "remove-application",
            "-m",
            juju.model,
            "--force",
            "--destroy-storage",
            "--no-wait",
            "--no-prompt",
            app_name,
        ]
        assert os.system(" ".join(destroy_cmd)) == 0
        time.sleep(100)
        juju.wait(lambda status: app_name not in status.apps)

    def test_deploy_other_app_active(
        self, juju: jubilant.Juju, cassandra_charm: Path, other_app_name: str
    ) -> None:
        juju.deploy(
            cassandra_charm,
            app=other_app_name,
            config={"profile": "testing"},
            num_units=3,
        )
        juju.wait(lambda status: all_active_idle(status, other_app_name), timeout=1800)

    def test_storage_integration_other_app(self, juju: jubilant.Juju, other_app_name: str):
        juju.integrate(other_app_name, self.integrator_app)
        juju.wait(all_active_idle)

    def test_restore_backup_on_other_app(self, juju: jubilant.Juju, other_app_name: str):
        task = juju.run(f"{other_app_name}/0", "list-backups")
        backups = json.loads(task.results.get("result", "[]"))
        assert len(backups) > 0
        logger.info(json.dumps(backups))
        backup_id = backups[-1]["id"]
        task = juju.run(f"{other_app_name}/1", "restore", params={"backup-id": backup_id})
        juju.wait(
            lambda status: all_active_idle(status, other_app_name),
            successes=10,
            timeout=1800,
            delay=3,
        )

    def test_restore_integrity(self, juju: jubilant.Juju, other_app_name: str):
        hosts = get_hosts(juju, other_app_name)
        password = app_secret_extract(juju, other_app_name, OPERATOR_PASSWORD)

        with connect_cql(
            hosts=hosts, password=password, username="operator", keyspace=self.test_ks
        ) as session:
            rs = session.execute(f"SELECT COUNT(*) FROM {self.test_ks}.{self.test_tbl}")
            assert rs.current_rows[0].count == self.n_recs
