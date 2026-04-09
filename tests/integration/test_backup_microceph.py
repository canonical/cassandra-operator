#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import secrets
from dataclasses import dataclass
from pathlib import Path

import jubilant
import pytest
import tenacity

from events.backup import BackupMessages
from integration.helpers.juju import all_active_idle, exec_

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    """S3 connection config model."""

    bucket: str
    endpoint: str
    region: str
    access_key: str
    secret_key: str

    @classmethod
    def from_env(cls) -> "S3Config":
        return cls(**{k: os.environ.get(f"S3_{k.upper()}", "") for k in cls.__dataclass_fields__})


CONTAINER = f"microceph-{secrets.token_hex(4)}"
REQUIRED_ENV = [f"S3_{fld.upper()}" for fld in S3Config.__dataclass_fields__]
STORAGE_INTEGRATOR_APP = "s3-integrator"
STORAGE_INTEGRATOR_CHANNEL = "2/edge"


@pytest.fixture(autouse=True, scope="module")
def prepare_microceph():
    assert os.system(f"lxc launch ubuntu:24.04 {CONTAINER}") == 0
    # wait for container to boot & setup microceph.
    raw = None
    for attempt in tenacity.Retrying(
        wait=tenacity.wait_fixed(10), stop=tenacity.stop_after_delay(600)
    ):
        with attempt:
            exec_(f"lxc exec {CONTAINER} -- whoami")
            exec_(
                f"lxc file push tests/integration/helpers/setup-microceph.sh {CONTAINER}/root/setup.sh"  # noqa: E501
            )
            raw = exec_(f"lxc exec {CONTAINER} -- /root/setup.sh")

    if not raw:
        raise RuntimeError("microceph setup failed!")

    # set env. vars based on the setup script output.
    for kv in raw.strip().split():
        parts = kv.split("=")
        os.environ[parts[0]] = parts[1]

    yield
    assert os.system(f"lxc rm --force {CONTAINER}")


@pytest.fixture
def s3_config() -> S3Config:
    for k in REQUIRED_ENV:
        if k not in os.environ:
            raise ValueError(
                f"Following env. vars should be set for this test suite: {REQUIRED_ENV}"
            )

    return S3Config.from_env()


def test_deploy_active(
    juju: jubilant.Juju, cassandra_charm: Path, app_name: str, s3_config: S3Config
) -> None:
    # juju.deploy(
    #     cassandra_charm,
    #     app=app_name,
    #     config={"profile": "testing"},
    #     num_units=3,
    # )
    juju.deploy(
        STORAGE_INTEGRATOR_APP,
        channel=STORAGE_INTEGRATOR_CHANNEL,
        config={
            "endpoint": s3_config.endpoint,
            "bucket": s3_config.bucket,
            "region": s3_config.region,
        },
    )

    secret_name = f"test-{secrets.token_hex(4)}"
    secret_id = juju.add_secret(
        secret_name,
        content={"access-key": s3_config.access_key, "secret-key": s3_config.secret_key},
    )
    juju.cli("grant-secret", secret_name, STORAGE_INTEGRATOR_APP)
    juju.config(STORAGE_INTEGRATOR_APP, values={"credentials": secret_id})

    juju.wait(all_active_idle, timeout=1800)


def test_run_actions_before_s3_integration_fails(juju: jubilant.Juju, app_name: str):
    with pytest.raises(jubilant.TaskError) as exc:
        juju.run(f"{app_name}/0", "create-backup")

    assert exc.value.task.message == BackupMessages.NOT_READY.value

    with pytest.raises(jubilant.TaskError) as other_exc:
        juju.run(f"{app_name}/0", "list-backups")

    assert other_exc.value.task.message == BackupMessages.NOT_READY.value


def test_s3_integration(juju: jubilant.Juju, app_name: str):
    juju.integrate(app_name, STORAGE_INTEGRATOR_APP)
    juju.wait(all_active_idle)


def test_list_backups_succeeds_and_is_empty(juju: jubilant.Juju, app_name: str):
    task = juju.run(f"{app_name}/0", "list-backups")
    assert task.results.get("result") == "[]"


def test_create_backup(juju: jubilant.Juju, app_name: str):
    task = juju.run(f"{app_name}/0", "create-backup", wait=1200)
    assert task.results.get("backup-status") == "backup created"


def test_list_backups_on_another_unit(juju: jubilant.Juju, app_name: str):
    task = juju.run(f"{app_name}/1", "list-backups")
    backups = json.loads(task.results.get("result", "[]"))
    assert len(backups) == 1
    assert backups[0].get("start-time") < backups[0].get("end-time")
    logger.info(f"One backup found: {backups[0]['id']}")
