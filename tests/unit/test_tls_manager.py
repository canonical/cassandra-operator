# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
import contextlib
import http.server
import json
import logging
import os
import ssl
import subprocess
from multiprocessing import Process
from telnetlib import TLS
from typing import Mapping
from unittest.mock import MagicMock

from charmlibs import pathops

from core.workload import WorkloadBase, CassandraPaths
from managers.tls import TLSManager
from helpers import TLSArtifacts, generate_tls_artifacts
from core.state import TLSScope, TLSContext, ResolvedTLSContext
from ops.testing import Relation
import pytest
import os

logger = logging.getLogger(__name__)

UNIT_NAME = "cassandra/0"
INTERNAL_ADDRESS = "10.10.10.10"
BIND_ADDRESS = "10.20.20.20"
KEYTOOL = "keytool"

def _exec(
    command: list[str] | str,
    env: Mapping[str, str] | None = None,
    cwd: str | None = None,
    _: bool = False,
) -> str:
    _command = " ".join(command) if isinstance(command, list) else command
    print(_command)

    for bin in ("chown", "chmod"):
        if _command.startswith(bin):
            return "ok"

    try:
        output = subprocess.check_output(
            command,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            shell=isinstance(command, str),
            env=env,
            cwd=cwd,
        )
        return output
    except subprocess.CalledProcessError as e:
        raise e


try:
    _exec(KEYTOOL)
    _exec("java -version")
    JAVA_TESTS_DISABLED = False
except subprocess.CalledProcessError:
    JAVA_TESTS_DISABLED = True

@contextlib.contextmanager
def simple_ssl_server(certfile: str, keyfile: str, port: int = 10443):
    httpd = http.server.HTTPServer(("127.0.0.1", port), http.server.SimpleHTTPRequestHandler)
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)

    process = Process(target=httpd.serve_forever)
    process.start()
    yield

    process.kill()

@pytest.fixture()
def tls_manager(tmp_path_factory, monkeypatch):
    """A TLSManager instance with minimal functioning mock `Workload`."""
    # Создаем временные директории для config и data
    config_dir = tmp_path_factory.mktemp("config")
    data_dir = tmp_path_factory.mktemp("data")
    tls_dir = tmp_path_factory.mktemp("tls")


    # Настраиваем мок cassandra_paths
    cassandra_paths = CassandraPaths()
    cassandra_paths.config_dir = config_dir
    cassandra_paths.data_dir = data_dir
    cassandra_paths.tls_dir = tls_dir

    # Мокаем workload
    mock_workload = MagicMock(spec=WorkloadBase)
    mock_workload.cassandra_paths = cassandra_paths
    mock_workload.exec = _exec
    mock_workload.remove_file = MagicMock()
    mock_workload.remove_directory = MagicMock()
    mock_workload.path_exists = MagicMock(return_value=True)

    mgr = TLSManager(workload=mock_workload)
    mgr.keytool = KEYTOOL

    yield mgr

def _get_tls_context(
    mgr: TLSManager, tls_artifacts: TLSArtifacts | None = None, scope: TLSScope = TLSScope.CLIENT
) -> TLSContext:
    mock_relation = MagicMock()
    mock_relation.id = 1

    ctx = TLSContext(
        mock_relation,
        MagicMock(),
        MagicMock(),
        scope=scope,
    )

    ctx.relation_data = {}

    if tls_artifacts:
        ctx.ca = tls_artifacts.ca
        ctx.certificate = tls_artifacts.certificate
        ctx.chain = tls_artifacts.chain
        ctx.private_key = tls_artifacts.private_key

    logger.info(f"ctx.ca: {ctx.ca}")
    
    return ctx

def _tls_manager_set_everything(mgr: TLSManager, tls_context: ResolvedTLSContext) -> None:
    scope = tls_context.scope

    mgr.set_ca(tls_context.ca, scope)
    mgr.set_chain(tls_context.chain, scope)
    mgr.set_private_key(tls_context.private_key, scope)
    mgr.set_certificate(tls_context.certificate, scope)
    mgr.set_bundle(tls_context.bundle, scope)
    mgr.set_keystore("keystore-password", scope)
    mgr.set_truststore("truststore-password", scope)

@pytest.mark.skipif(
    JAVA_TESTS_DISABLED, reason=f"Can't locate {KEYTOOL} and/or java in the test environment."
)
@pytest.mark.parametrize(
    "with_intermediate", [False, True], ids=["NO intermediate CA", "ONE intermediate CA"]
)
def test_tls_manager_set_methods(
    tls_manager: TLSManager,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
    with_intermediate: bool,
):
    """Tests the lifecycle of adding/removing certs from Java and TLSManager points of view."""
    # Генерируем артефакты TLS

    peer_tls_artifacts = generate_tls_artifacts(with_intermediate=with_intermediate)

    # Создаем ResolvedTLSContext
    peer_tls_context = _get_tls_context(mgr=tls_manager, tls_artifacts=peer_tls_artifacts, scope=TLSScope.PEER)

    peer_tls_artifacts.bundle = peer_tls_context.bundle

    caplog.set_level(logging.DEBUG)
    _tls_manager_set_everything(tls_manager, peer_tls_context.resolved())

    # Проверяем, что файлы созданы и содержат правильные данные
    tls_dir = tls_manager.workload.cassandra_paths.tls_dir
    assert (tls_dir / "peer-unit.pem").read_text() == peer_tls_artifacts.certificate.raw
    assert (tls_dir / "peer-ca.pem").read_text() == peer_tls_artifacts.ca.raw
    assert (tls_dir / "peer-bundle0.pem").read_text() == peer_tls_artifacts.bundle[0].raw
    assert (tls_dir / "peer-bundle1.pem").read_text() == peer_tls_artifacts.bundle[1].raw
    assert (tls_dir / "peer-private.key").read_text() == peer_tls_artifacts.private_key.raw


    client_tls_artifacts = generate_tls_artifacts(with_intermediate=with_intermediate)

    client_tls_context = _get_tls_context(tls_manager, client_tls_artifacts, TLSScope.CLIENT)

    client_tls_artifacts.bundle = client_tls_context.bundle

    caplog.set_level(logging.DEBUG)
    _tls_manager_set_everything(tls_manager, client_tls_context.resolved())

    assert (tls_dir / "client-unit.pem").read_text() == client_tls_artifacts.certificate.raw
    assert (tls_dir / "client-ca.pem").read_text() == client_tls_artifacts.ca.raw
    assert (tls_dir / "client-bundle0.pem").read_text() == client_tls_artifacts.bundle[0].raw
    assert (tls_dir / "client-bundle1.pem").read_text() == client_tls_artifacts.bundle[1].raw
    assert (tls_dir / "client-private.key").read_text() == client_tls_artifacts.private_key.raw


@pytest.mark.skipif(
    JAVA_TESTS_DISABLED, reason=f"Can't locate {KEYTOOL} and/or java in the test environment."
)
@pytest.mark.parametrize(
    "with_intermediate", [False, True], ids=["NO intermediate CA", "ONE intermediate CA"]
)
def test_tls_manager_sans(
    tls_manager: TLSManager,
    with_intermediate: bool,
) -> None:
    """Tests the lifecycle of adding/removing certs from Java and TLSManager points of view."""
    tls_artifacts = generate_tls_artifacts(
        sans_ip=[INTERNAL_ADDRESS],
        sans_dns=[UNIT_NAME],
        with_intermediate=with_intermediate,
    )
    tls_context = _get_tls_context(tls_manager, tls_artifacts, TLSScope.CLIENT)
    _tls_manager_set_everything(tls_manager, tls_context.resolved())
    # check SANs
    current_sans =  {
        "sans_ip": [INTERNAL_ADDRESS],
        "sans_dns": [UNIT_NAME],
    }
    expected_sans = tls_manager.build_sans(sans_ip=[INTERNAL_ADDRESS], sans_dns=[UNIT_NAME])
    assert expected_sans == current_sans