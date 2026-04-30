#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os
import secrets
from dataclasses import dataclass

import jubilant
import pytest

from integration.backup import BackupRestoreTests
from integration.helpers.juju import all_active_idle

logger = logging.getLogger(__name__)


ENV_PREFIX = "GCS"


@dataclass
class GCSConfig:
    """GCS connection config model."""

    bucket: str
    secret_key: str

    @classmethod
    def from_env(cls) -> "GCSConfig":
        return cls(
            **{
                k: os.environ.get(f"{ENV_PREFIX}_{k.upper()}", "")
                for k in cls.__dataclass_fields__
            }
        )


REQUIRED_ENV = [f"{ENV_PREFIX}_{fld.upper()}" for fld in GCSConfig.__dataclass_fields__]
STORAGE_INTEGRATOR_APP = "gcs-integrator"
STORAGE_INTEGRATOR_CHANNEL = "1/edge"


@pytest.fixture
def gcs_config() -> GCSConfig:
    for k in REQUIRED_ENV:
        if k not in os.environ:
            raise ValueError(
                f"Following env. vars should be set for this test suite: {REQUIRED_ENV}"
            )

    return GCSConfig.from_env()


def test_deploy_storage_integrator_and_test_connection(juju: jubilant.Juju, gcs_config: GCSConfig):
    juju.deploy(
        STORAGE_INTEGRATOR_APP,
        channel=STORAGE_INTEGRATOR_CHANNEL,
        config={
            "bucket": gcs_config.bucket,
        },
    )
    secret_name = f"test-{secrets.token_hex(4)}"
    # jubilant does not support secrets from file, so revert to cli:
    raw = juju.cli("add-secret", secret_name, f"secret-key#file={gcs_config.secret_key}")
    secret_id = raw.strip()
    juju.cli("grant-secret", secret_name, STORAGE_INTEGRATOR_APP)
    juju.config(STORAGE_INTEGRATOR_APP, values={"credentials": secret_id})

    juju.wait(
        lambda status: all_active_idle(status, STORAGE_INTEGRATOR_APP), successes=10, delay=1
    )


class TestGCS(BackupRestoreTests):
    integrator_app = STORAGE_INTEGRATOR_APP
