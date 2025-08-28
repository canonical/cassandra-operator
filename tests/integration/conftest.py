#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from typing import Generator
import logging


import jubilant
import pytest
import yaml
import json
from help_types import IntegrationTestsCharms, TestCharm
from helpers import get_microk8s_controller, juju_controller_env

logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def juju_local(request: pytest.FixtureRequest) -> Generator[jubilant.Juju, None, None]:
    keep_models = bool(request.config.getoption("--keep-models"))

    with juju_controller_env("localhost-localhost"):
        with jubilant.temp_model(keep=keep_models, controller="localhost-localhost") as juju_local:
            juju_local.wait_timeout = 10 * 60

            yield juju_local  # run the test

            if request.session.testsfailed:
                log = juju_local.debug_log(limit=300)
                print(log, end="")

@pytest.fixture(scope="module")
def juju_k8s(request: pytest.FixtureRequest) -> Generator[jubilant.Juju, None, None]:
    microk8s = get_microk8s_controller(jubilant.Juju())

    if not microk8s:
        raise ValueError("microk8s controller is not ready")

    keep_models = bool(request.config.getoption("--keep-models"))
    with juju_controller_env(microk8s):
        with jubilant.temp_model(keep=keep_models, controller=microk8s) as juju_k8s:
            juju_k8s.wait_timeout = 10 * 60

            yield juju_k8s  # run the test

            if request.session.testsfailed:
                log = juju_k8s.debug_log(limit=300)
                print(log, end="")

def pytest_addoption(parser) -> None:
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )


@pytest.fixture(scope="module")
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        tls=TestCharm(
            name="self-signed-certificates",
            channel="latest/stable",
            revision=163,  # FIXME (certs): Unpin the revision once the charm is fixed
            base="ubuntu@22.04",
            alias="self-signed-certificates",
        ),
    )


@pytest.fixture(scope="module")
def cassandra_charm() -> Path:
    """Path to the packed cassandra charm."""
    if not (path := next(iter(Path.cwd().glob("*.charm")), None)):
        raise FileNotFoundError("Could not find packed cassandra charm.")

    return path


@pytest.fixture(scope="module")
def app_name() -> str:
    metadata = yaml.safe_load(Path("./charmcraft.yaml").read_text())
    return metadata["name"]
