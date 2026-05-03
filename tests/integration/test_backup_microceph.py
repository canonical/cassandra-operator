#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os
import secrets
from dataclasses import dataclass
from pathlib import Path

import jubilant
import pytest
import tenacity

from integration.backup import BackupRestoreTests
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
    logger.info(raw)
    lines = [line.strip() for line in raw.split("\n") if line.strip()]
    # Last line has the env. vars
    for kv in lines[-1].split():
        parts = kv.split("=")
        if len(parts) != 2:
            continue
        os.environ[parts[0]] = parts[1]

    yield
    assert os.system(f"lxc rm --force {CONTAINER}") == 0


@pytest.fixture
def s3_config() -> S3Config:
    for k in REQUIRED_ENV:
        if k not in os.environ:
            raise ValueError(
                f"Following env. vars should be set for this test suite: {REQUIRED_ENV}"
            )

    return S3Config.from_env()


def test_deploy_s3_integrator(
    juju: jubilant.Juju, cassandra_charm: Path, app_name: str, s3_config: S3Config
) -> None:
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

    juju.wait(
        lambda status: all_active_idle(status, STORAGE_INTEGRATOR_APP), successes=10, delay=1
    )


class TestMicroceph(BackupRestoreTests):
    integrator_app = STORAGE_INTEGRATOR_APP
