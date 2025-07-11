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

import pytest
import yaml

KEYTOOL = "keytool"

def _exec(
    command: list[str] | str,
    env: Mapping[str, str] | None = None,
    working_dir: str | None = None,
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
            cwd=working_dir,
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

    # Настраиваем мок cassandra_paths
    cassandra_paths = CassandraPaths()
    cassandra_paths.config_dir = config_dir
    cassandra_paths.data_dir = data_dir

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
    ctx = TLSContext(
        None,
        MagicMock(),
        MagicMock(),
        scope=scope,
    )

    if tls_artifacts:
        ctx.ca = tls_artifacts.ca
        ctx.certificate = tls_artifacts.certificate
        ctx.chain = tls_artifacts.chain
        ctx.private_key = tls_artifacts.private_key
    
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

    tls_artifacts = generate_tls_artifacts(with_intermediate=with_intermediate)

    # Создаем ResolvedTLSContext
    peer_tls_context = _get_tls_context(tls_manager, tls_artifacts, TLSScope.PEER)

    caplog.set_level(logging.DEBUG)
    _tls_manager_set_everything(tls_manager, peer_tls_context.resolved())

    # Проверяем, что файлы созданы и содержат правильные данные
    tls_dir = tls_manager.workload.cassandra_paths.tls_dir
    assert (tls_dir / "peer-unit.pem").read_text() == tls_artifacts.certificate.raw
    assert (tls_dir / "peer-ca.pem").read_text() == tls_artifacts.ca.raw
    assert (tls_dir / "peer-bundle1.pem").read_text() == f"{tls_artifacts.certificate.raw}\n{tls_artifacts.ca.raw}\n"
    assert (tls_dir / "peer-private.key").read_text() == tls_artifacts.private_key.raw

    client_tls_context = _get_tls_context(tls_manager, tls_artifacts, TLSScope.CLIENT)

    caplog.set_level(logging.DEBUG)
    _tls_manager_set_everything(tls_manager, client_tls_context.resolved())

    assert (tls_dir / "client-unit.pem").read_text() == tls_artifacts.certificate.raw
    assert (tls_dir / "client-ca.pem").read_text() == tls_artifacts.ca.raw
    assert (tls_dir / "client-bundle1.pem").read_text() == f"{tls_artifacts.certificate.raw}\n{tls_artifacts.ca.raw}\n"
    assert (tls_dir / "client-private.key").read_text() == tls_artifacts.private_key.raw



