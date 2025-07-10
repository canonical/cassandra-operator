#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from typing import Generator

import jubilant
import pytest
import yaml

from .types import IntegrationTestsCharms, TestCharm

@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest) -> Generator[jubilant.Juju, None, None]:
    keep_models = bool(request.config.getoption("--keep-models"))

    with jubilant.temp_model(keep=keep_models) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=300)
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
    metadata = yaml.safe_load(Path("./metadata.yaml").read_text())
    return metadata["name"]
