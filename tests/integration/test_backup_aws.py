#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os
import secrets
import uuid
from dataclasses import dataclass
from pathlib import Path

import jubilant
import pytest

from integration.backup import BackupRestoreTests
from integration.helpers.juju import all_active_idle

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    """S3 connection config model."""

    access_key: str
    secret_key: str
    bucket: str = "data-charms-testing"
    endpoint: str = "https://s3.amazonaws.com"
    region: str = "us-east-1"
    path: str = f"cassandra-{uuid.uuid1()}"

    @classmethod
    def from_env(cls) -> "S3Config":
        return cls(
            **{
                k: os.environ.get(f"AWS_{k.upper()}", "")
                for k in cls.__dataclass_fields__
                if f"AWS_{k.upper()}" in os.environ
            }
        )


CONTAINER = f"microceph-{secrets.token_hex(4)}"
REQUIRED_ENV = ("AWS_ACCESS_KEY", "AWS_SECRET_KEY")
STORAGE_INTEGRATOR_APP = "s3-integrator"
STORAGE_INTEGRATOR_CHANNEL = "2/edge"


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
            "path": s3_config.path,
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
