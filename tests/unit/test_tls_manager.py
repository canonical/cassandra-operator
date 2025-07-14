# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
import contextlib
import http.server
import logging
import ssl
import subprocess
from dataclasses import dataclass
from datetime import timedelta
from multiprocessing import Process
from typing import Mapping
from unittest.mock import MagicMock

import pytest
from charms.tls_certificates_interface.v4.tls_certificates import (
    Certificate,
    CertificateSigningRequest,
    PrivateKey,
    ProviderCertificate,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)

from core.state import ResolvedTLSContext, TLSContext, TLSScope
from core.workload import CassandraPaths, WorkloadBase
from managers.tls import TLSManager

logger = logging.getLogger(__name__)

UNIT_NAME = "cassandra/0"
INTERNAL_ADDRESS = "10.10.10.10"
BIND_ADDRESS = "10.20.20.20"
KEYTOOL = "keytool"


@dataclass
class TLSArtifacts:
    certificate: Certificate
    private_key: PrivateKey
    ca: Certificate
    chain: list[Certificate]
    bundle: list[Certificate]
    signing_cert: CertificateSigningRequest
    signing_key: PrivateKey
    provider_cert: ProviderCertificate


def generate_tls_artifacts(
    sans_dns: list[str] = ["localhost"],
    sans_ip: list[str] = ["127.0.0.1"],
    with_intermediate: bool = False,
) -> TLSArtifacts:
    """Generate all required TLS artifacts for TLS tests."""
    ca_key = generate_private_key()
    ca = generate_ca(
        private_key=ca_key,
        validity=timedelta(365),
        common_name="some-CN",
    )
    signing_cert, signing_key = ca, ca_key
    if with_intermediate:
        intermediate_key = generate_private_key()
        intermediate_csr = generate_csr(
            private_key=intermediate_key,
            common_name="some-inter-CN",
            sans_ip=frozenset(sans_ip),
            sans_dns=frozenset(sans_dns),
        )
        intermediate_cert = generate_certificate(
            csr=intermediate_csr,
            ca=ca,
            ca_private_key=ca_key,
            validity=timedelta(365),
        )
        signing_cert, signing_key = intermediate_cert, intermediate_key
    key = generate_private_key()
    csr = generate_csr(
        private_key=key,
        common_name="some-inter-CN",
        sans_ip=frozenset(sans_ip),
        sans_dns=frozenset(sans_dns),
    )
    cert = generate_certificate(csr, signing_cert, signing_key, validity=timedelta(365))
    chain = [cert, ca]
    return TLSArtifacts(
        certificate=cert,
        private_key=key,
        ca=ca,
        chain=chain,
        signing_cert=csr,
        signing_key=signing_key,
        bundle=[],
        provider_cert=ProviderCertificate(
            relation_id=0,
            certificate=cert,
            certificate_signing_request=csr,
            ca=ca,
            chain=[cert, ca],
            revoked=None,
        ),
    )


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
    java_tests_disabled = False
except subprocess.CalledProcessError:
    java_tests_disabled = True


@contextlib.contextmanager
def simple_ssl_server(certfile: str, keyfile: str, port: int = 10443):
    """Context manager for a simple SSL server during a test."""
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
    """Fixture: TLSManager with a minimal mocked Workload."""
    config_dir = tmp_path_factory.mktemp("config")
    data_dir = tmp_path_factory.mktemp("data")
    tls_dir = tmp_path_factory.mktemp("tls")

    cassandra_paths = CassandraPaths()
    cassandra_paths.config_dir = config_dir
    cassandra_paths.data_dir = data_dir
    cassandra_paths.tls_dir = tls_dir

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
    tls_artifacts: TLSArtifacts | None = None, scope: TLSScope = TLSScope.CLIENT
) -> TLSContext:
    """Create a TLSContext with artifacts."""
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
    """Set all values in TLSManager for the given context."""
    scope = tls_context.scope
    mgr.set_ca(tls_context.ca, scope)
    mgr.set_chain(tls_context.chain, scope)
    mgr.set_private_key(tls_context.private_key, scope)
    mgr.set_certificate(tls_context.certificate, scope)
    mgr.set_bundle(tls_context.bundle, scope)
    mgr.set_keystore("keystore-password", scope)
    mgr.set_truststore("truststore-password", scope)


# ===================== TESTS =====================


@pytest.mark.skipif(
    java_tests_disabled, reason=f"Can't locate {KEYTOOL} and/or java in the test environment."
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
    """Checks the lifecycle of adding/removing certificates with TLSManager."""
    peer_tls_artifacts = generate_tls_artifacts(with_intermediate=with_intermediate)
    peer_tls_context = _get_tls_context(tls_artifacts=peer_tls_artifacts, scope=TLSScope.PEER)
    peer_tls_artifacts.bundle = peer_tls_context.bundle

    caplog.set_level(logging.DEBUG)

    _tls_manager_set_everything(tls_manager, peer_tls_context.resolved())

    tls_dir = tls_manager.workload.cassandra_paths.tls_dir

    # Check that files are created and contain correct data
    assert (tls_dir / "peer-unit.pem").read_text() == peer_tls_artifacts.certificate.raw
    assert (tls_dir / "peer-ca.pem").read_text() == peer_tls_artifacts.ca.raw
    assert (tls_dir / "peer-bundle0.pem").read_text() == peer_tls_artifacts.bundle[0].raw
    assert (tls_dir / "peer-bundle1.pem").read_text() == peer_tls_artifacts.bundle[1].raw
    assert (tls_dir / "peer-private.key").read_text() == peer_tls_artifacts.private_key.raw

    client_tls_artifacts = generate_tls_artifacts(with_intermediate=with_intermediate)
    client_tls_context = _get_tls_context(client_tls_artifacts, TLSScope.CLIENT)
    client_tls_artifacts.bundle = client_tls_context.bundle

    caplog.set_level(logging.DEBUG)

    _tls_manager_set_everything(tls_manager, client_tls_context.resolved())

    assert (tls_dir / "client-unit.pem").read_text() == client_tls_artifacts.certificate.raw
    assert (tls_dir / "client-ca.pem").read_text() == client_tls_artifacts.ca.raw
    assert (tls_dir / "client-bundle0.pem").read_text() == client_tls_artifacts.bundle[0].raw
    assert (tls_dir / "client-bundle1.pem").read_text() == client_tls_artifacts.bundle[1].raw
    assert (tls_dir / "client-private.key").read_text() == client_tls_artifacts.private_key.raw


@pytest.mark.parametrize(
    "with_intermediate", [False, True], ids=["NO intermediate CA", "ONE intermediate CA"]
)
def test_tls_manager_sans(
    tls_manager: TLSManager,
    with_intermediate: bool,
) -> None:
    """Test: Checks SANs handling when generating and setting certificates."""
    tls_artifacts = generate_tls_artifacts(
        sans_ip=[INTERNAL_ADDRESS],
        sans_dns=[UNIT_NAME],
        with_intermediate=with_intermediate,
    )
    tls_context = _get_tls_context(tls_artifacts, TLSScope.CLIENT)
    _tls_manager_set_everything(tls_manager, tls_context.resolved())
    current_sans = {
        "sans_ip": [INTERNAL_ADDRESS],
        "sans_dns": [UNIT_NAME],
    }
    expected_sans = tls_manager.build_sans(sans_ip=[INTERNAL_ADDRESS], sans_dns=[UNIT_NAME])
    assert expected_sans == current_sans
