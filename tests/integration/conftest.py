#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import contextlib
import logging
import os
import secrets
from pathlib import Path
from typing import Generator

import jubilant
import pytest
import yaml
from help_types import IntegrationTestsCharms, TestCharm
from helpers import get_microk8s_controller, using_k8s, using_vm

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def temp_model_named(
    model: str, keep: bool = False, controller: str | None = None
) -> Generator[jubilant.Juju]:
    """Context manager to create a temporary model for running tests in."""
    juju = jubilant.Juju()

    if not model:
        model = "jubilant-" + secrets.token_hex(4)  # 4 bytes (8 hex digits) should be plenty
    juju.add_model(model, controller=controller)
    try:
        yield juju
    finally:
        if not keep:
            juju.destroy_model(model, destroy_storage=True, force=True)


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest) -> Generator[jubilant.Juju, None, None]:
    keep_models = bool(request.config.getoption("--keep-models"))
    test_model_name = str(request.config.getoption("--model-name"))

    with using_vm():
        with temp_model_named(
            test_model_name,
            keep=keep_models,
            controller=os.environ["JUJU_CONTROLLER"],
        ) as juju_local:
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
    test_model_name = str(request.config.getoption("--model-name"))

    with using_k8s():
        with temp_model_named(
            test_model_name,
            keep=keep_models,
            controller=os.environ["JUJU_CONTROLLER"],
        ) as juju_k8s:
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
