# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
from unittest.mock import PropertyMock, patch, MagicMock

import ops
import pytest
import dataclasses
import json

from charm import CassandraCharm
from core.state import PEER_RELATION, PEER_TLS_RELATION, CLIENT_TLS_RELATION, TLSScope
from ops.testing import Secret
from typing import Iterable

from ops import testing
from datetime import timedelta
from ops.testing import Context, PeerRelation
from charms.tls_certificates_interface.v4.tls_certificates import (
    Certificate,
    CertificateAvailableEvent,
    CertificateSigningRequest,
    PrivateKey,
    ProviderCertificate,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)

BOOTSTRAP_RELATION = "bootstrap"
CERTS_REL_NAME = "certificates"
TLS_NAME = "self-signed-certificates"

@dataclasses.dataclass
class DefaultCertificateContext:
    context: Context[CassandraCharm]
    peer_relation: testing.PeerRelation
    peer_tls_relation: testing.Relation
    client_tls_relation: testing.Relation
    bootstrap_relation: testing.PeerRelation

    default_provider_private_key: PrivateKey
    default_provider_ca_certificate: Certificate

    default_requirer_private_key: PrivateKey
    default_peer_certificate: Certificate
    default_peer_csr: CertificateSigningRequest
    default_peer_provider_certificate: ProviderCertificate

@dataclasses.dataclass
class CertificateAvailableContext:
    context: Context[CassandraCharm]
    peer_relation: testing.PeerRelation
    peer_tls_relation: testing.Relation
    client_tls_relation: testing.Relation
    bootstrap_relation: testing.PeerRelation

    provider_private_key: PrivateKey
    provider_ca_certificate: Certificate

    requirer_private_key: PrivateKey
    peer_csr: CertificateSigningRequest
    peer_certificate: Certificate
    peer_provider_certificate: ProviderCertificate

    client_csr: CertificateSigningRequest
    client_certificate: Certificate
    client_provider_certificate: ProviderCertificate


@pytest.fixture
def ctx() -> Context[CassandraCharm]:
    return Context(CassandraCharm, unit_id=0)

def default_certificate_context(ctx: Context[CassandraCharm]) -> DefaultCertificateContext:
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    peer_tls_relation = testing.Relation(id=2, endpoint=PEER_TLS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION)
    bootstrap_relation = testing.PeerRelation(id=4, endpoint=BOOTSTRAP_RELATION)

    default_provider_private_key = generate_private_key()
    default_provider_ca_certificate = generate_ca(
        private_key=default_provider_private_key,
        common_name="example.com",
        validity=timedelta(days=365),
    )

    default_requirer_private_key = generate_private_key()
    default_peer_csr = generate_csr(
        private_key=default_requirer_private_key,
        common_name="cas-test-1",
        organization=TLSScope.PEER.value,
    )
    default_peer_certificate = generate_certificate(
        ca_private_key=default_provider_private_key,
        csr=default_peer_csr,
        ca=default_provider_ca_certificate,
        validity=timedelta(days=1),
    )
    default_peer_provider_certificate = ProviderCertificate(
        relation_id=peer_relation.id,
        certificate=default_peer_certificate,
        certificate_signing_request=default_peer_csr,
        ca=default_provider_ca_certificate,
        chain=[default_provider_ca_certificate, default_peer_certificate],
        revoked=False,
    )

    return DefaultCertificateContext(
        context=ctx,
        peer_relation=peer_relation,
        peer_tls_relation=peer_tls_relation,
        client_tls_relation=client_tls_relation,
        bootstrap_relation=bootstrap_relation,
        default_provider_private_key=default_provider_private_key,
        default_provider_ca_certificate=default_provider_ca_certificate,
        default_requirer_private_key=default_requirer_private_key,
        default_peer_certificate=default_peer_certificate,
        default_peer_csr=default_peer_csr,
        default_peer_provider_certificate=default_peer_provider_certificate,
    )

def apply_default_certificates(relation: PeerRelation, default_certificates: DefaultCertificateContext):
    unit_secret = {
        "peer-private-key": default_certificates.default_requirer_private_key.raw,
        "peer-ca-cert": default_certificates.default_provider_ca_certificate.raw,
        "peer-certificate": default_certificates.default_peer_certificate.raw,
        "peer-csr": default_certificates.default_peer_csr.raw,
        "peer-chain": json.dumps([str(c) for c in default_certificates.default_peer_provider_certificate.chain]),
    }
    app_secret = {
        "internal-ca": default_certificates.default_provider_ca_certificate.raw,
        "internal-ca-key": default_certificates.default_provider_private_key.raw,
    }
    relation.local_unit_data.update(unit_secret)
    relation.local_app_data.update(app_secret)

def apply_available_certificates(relation: PeerRelation, new_certificates: CertificateAvailableContext):
    unit_secret = {
        "peer-private-key": new_certificates.requirer_private_key.raw,
        "peer-ca-cert": new_certificates.provider_ca_certificate.raw,
        "peer-certificate": new_certificates.peer_certificate.raw,
        "peer-csr": new_certificates.peer_csr.raw,
        "peer-chain": json.dumps([str(c) for c in new_certificates.peer_provider_certificate.chain]),
        "client-private-key": new_certificates.requirer_private_key.raw,
        "client-ca-cert": new_certificates.provider_ca_certificate.raw,
        "client-certificate": new_certificates.client_certificate.raw,
        "client-csr": new_certificates.client_csr.raw,
        "client-chain": json.dumps([str(c) for c in new_certificates.client_provider_certificate.chain]),
    }
    app_secret = {
        "internal-ca": new_certificates.provider_ca_certificate.raw,
        "internal-ca-key": new_certificates.provider_private_key.raw,
    }
    relation.local_unit_data.update(unit_secret)
    relation.local_app_data.update(app_secret)

def certificate_available_context(ctx: Context[CassandraCharm], is_leader: bool = True) -> CertificateAvailableContext:
    """Create a context for testing certificate available event."""
    peer_relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
    )
    peer_tls_relation = testing.Relation(id=2, endpoint=PEER_TLS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION)
    bootstrap_relation = testing.PeerRelation(id=4, endpoint=BOOTSTRAP_RELATION)

    provider_private_key = generate_private_key()
    provider_ca_certificate = generate_ca(
        private_key=provider_private_key,
        common_name="example.com",
        validity=timedelta(days=365),
    )
    requirer_private_key = generate_private_key()
    peer_csr = generate_csr(
        private_key=requirer_private_key,
        common_name="cas-test-1",
        organization=TLSScope.PEER.value,
    )
    peer_certificate = generate_certificate(
        ca_private_key=provider_private_key,
        csr=peer_csr,
        ca=provider_ca_certificate,
        validity=timedelta(days=1),
    )
    peer_provider_certificate = ProviderCertificate(
        relation_id=peer_tls_relation.id,
        certificate=peer_certificate,
        certificate_signing_request=peer_csr,
        ca=provider_ca_certificate,
        chain=[provider_ca_certificate, peer_certificate],
        revoked=False,
    )
    client_csr = generate_csr(
        private_key=requirer_private_key,
        common_name="cas-test-1",
        organization=TLSScope.CLIENT.value,
    )
    client_certificate = generate_certificate(
        ca_private_key=provider_private_key,
        csr=client_csr,
        ca=provider_ca_certificate,
        validity=timedelta(days=1),
    )
    client_provider_certificate = ProviderCertificate(
        relation_id=client_tls_relation.id,
        certificate=client_certificate,
        certificate_signing_request=client_csr,
        ca=provider_ca_certificate,
        chain=[provider_ca_certificate, client_certificate],
        revoked=False,
    )
    return CertificateAvailableContext(
        context=ctx,
        peer_relation=peer_relation,
        peer_tls_relation=peer_tls_relation,
        client_tls_relation=client_tls_relation,
        bootstrap_relation=bootstrap_relation,
        provider_private_key=provider_private_key,
        provider_ca_certificate=provider_ca_certificate,
        requirer_private_key=requirer_private_key,
        peer_csr=peer_csr,
        peer_certificate=peer_certificate,
        peer_provider_certificate=peer_provider_certificate,
        client_csr=client_csr,
        client_certificate=client_certificate,
        client_provider_certificate=client_provider_certificate,
    )

from ops.framework import Object

class TestObserver(Object):
    def __init__(self, charm):
        super().__init__(charm, "test_observer")
        self.called = False
    def handler(self, event):
        self.called = True

def get_secrets_latest_content_by_label(
    secrets: Iterable["Secret"], label: str, owner: str
) -> dict[str, str]:
    result = {}
    for secret in secrets:
        if owner and getattr(secret, "owner", None) != owner:
            continue
        if getattr(secret, "label", None) == label:
            if hasattr(secret, "latest_content"):
                if secret.latest_content:
                    result.update(secret.latest_content)
    return result

# ===================== TESTS =====================

@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_relation_broken_resets_certificates_and_triggers_config(ctx, is_leader):
    """Test: When the TLS relation is broken, certificates are reset, config is triggered, and correct manager methods are called."""
    new_tls_context = certificate_available_context(ctx, is_leader)
    default_tls_context = default_certificate_context(ctx)
    apply_available_certificates(new_tls_context.peer_relation, new_tls_context)
    state_in = testing.State(
        relations=[
            new_tls_context.peer_relation,
            new_tls_context.peer_tls_relation,
            new_tls_context.client_tls_relation,
            new_tls_context.bootstrap_relation,
        ],
        leader=is_leader,
    )
    with (
        new_tls_context.context(new_tls_context.context.on.relation_broken(new_tls_context.peer_tls_relation), state=state_in) as manager,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure") as mock_configure,
        patch("managers.tls.TLSManager.remove_stores") as mock_remove_stores,
        patch("managers.tls.TLSManager.generate_internal_ca") as mock_gen_ca,
        patch("managers.tls.TLSManager.generate_internal_credentials") as mock_gen_internal_creds,
    ):
        charm: CassandraCharm = manager.charm
        config_changed_observer = TestObserver(charm)
        charm.framework.observe(charm.on.config_changed, config_changed_observer.handler)
        mock_gen_internal_creds.return_value = (
            default_tls_context.default_peer_provider_certificate,
            default_tls_context.default_requirer_private_key,
        )
        manager.run()
        peer_tls = charm.tls_events.requirer_state(charm.tls_events.peer_certificate)
        assert peer_tls.certificate != new_tls_context.peer_certificate
        assert peer_tls.ca == new_tls_context.provider_ca_certificate
        assert peer_tls.chain != new_tls_context.peer_provider_certificate.chain
        assert peer_tls.csr != new_tls_context.peer_csr
        assert peer_tls.rotation is True
        mock_remove_stores.assert_called_once()
        mock_configure.assert_called_once()
        mock_gen_ca.assert_not_called()
        mock_gen_internal_creds.assert_called_once()
        assert config_changed_observer.called

@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_enabled_but_not_ready_sets_waiting_status(ctx, is_leader):
    """Test: If TLS is enabled but files are not set, unit goes to WaitingStatus."""
    default_tls_context = default_certificate_context(ctx)
    default_tls_context.peer_relation.local_unit_data.update({"workload_state": "active"})
    apply_default_certificates(default_tls_context.peer_relation, default_tls_context)
    state_in = testing.State(
        relations=[
            default_tls_context.peer_relation,
            default_tls_context.peer_tls_relation,
            default_tls_context.client_tls_relation,
            default_tls_context.bootstrap_relation,
        ],
        leader=is_leader,
    )
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("core.state.UnitContext.keystore_password", new_callable=PropertyMock(return_value="keystore_password")),
        patch("core.state.UnitContext.truststore_password", new_callable=PropertyMock(return_value="truststore_password")),
        patch("managers.tls.TLSManager.configure"),
        patch("charm.CassandraWorkload") as workload,
        patch("core.state.ClusterContext.tls_state", new_callable=PropertyMock(return_value="active")),
    ):
        workload.return_value.client_tls_ready = False
        state_out = ctx.run(ctx.on.relation_created(default_tls_context.client_tls_relation), state_in)
        assert state_out.unit_status == ops.WaitingStatus('waiting for TLS setup')

@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_relation_created_sets_tls_state(ctx, is_leader):
    """Test: Relating the charm toggles TLS state in the databag depending on leadership."""
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    client_tls_relation = testing.PeerRelation(id=3, endpoint=CLIENT_TLS_RELATION)
    state = testing.State(leader=is_leader, relations={relation, bootstrap_relation, client_tls_relation})
    state_out = ctx.run(ctx.on.relation_created(client_tls_relation), state)
    assert state_out.get_relation(relation.id).local_app_data.get("tls_state", "") == (
        "active" if is_leader else ""
    )

def test_tls_default_certificates_files_setup(ctx):
    """Test: Default internal TLS of a new cluster creates all required files."""
    default_tls_context = default_certificate_context(ctx)
    state_in = testing.State(
        relations=[
            default_tls_context.peer_relation,
            default_tls_context.bootstrap_relation
        ],
        leader=True,
    )
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("core.state.UnitContext.keystore_password", new_callable=PropertyMock(return_value="keystore_password")),
        patch("core.state.UnitContext.truststore_password", new_callable=PropertyMock(return_value="truststore_password")),
        patch("charm.CassandraWorkload"),
        patch("managers.tls.TLSManager.configure"),
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=True),
        ),
    ):
        state = ctx.run(ctx.on.start(), state_in)
        latest_content = get_secrets_latest_content_by_label(
            state.secrets, "cassandra-peers.cassandra.app", "application"
        )
        assert "internal-ca" in latest_content
        assert "internal-ca-key" in latest_content
        latest_content = get_secrets_latest_content_by_label(
            state.secrets, "cassandra-peers.cassandra.unit", "unit"
        )
        assert "peer-ca-cert" in latest_content
        assert "peer-private-key" in latest_content
        assert "peer-chain" in latest_content
        assert "peer-csr" in latest_content
        assert "peer-certificate" in latest_content
        assert state.unit_status == ops.ActiveStatus()
    default_tls_context = default_certificate_context(ctx)
    state_in = testing.State(
        relations=[
            default_tls_context.peer_relation,
            default_tls_context.peer_tls_relation,
            default_tls_context.client_tls_relation,
            default_tls_context.bootstrap_relation
        ],
        leader=True,
    )
    default_tls_context.peer_relation.local_app_data.update({"cluster_state": "active"})
    default_tls_context.peer_relation.local_unit_data.update({"workload_state": "active"})
    with (
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
        patch("core.state.UnitContext.keystore_password", new_callable=PropertyMock(return_value="keystore_password")),
        patch("core.state.UnitContext.truststore_password", new_callable=PropertyMock(return_value="truststore_password")),
        patch("core.state.ClusterContext.internal_ca", new_callable=PropertyMock(return_value=default_tls_context.default_provider_ca_certificate)),
        patch("core.state.ClusterContext.internal_ca_key", new_callable=PropertyMock(return_value=default_tls_context.default_provider_private_key)),
        patch("charm.CassandraWorkload"),
        patch(
            "managers.cluster.ClusterManager.is_healthy",
            new_callable=PropertyMock(return_value=True),
        ),
        patch(
            "charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock", autospec=True
        ),
    ):
        state_out = ctx.run(ctx.on.start(), state_in)
        latest_content = get_secrets_latest_content_by_label(
            state.secrets, "cassandra-peers.cassandra.unit", "unit"
        )
        assert "peer-ca-cert" in latest_content
        assert "peer-private-key" in latest_content
        assert "peer-chain" in latest_content
        assert "peer-csr" in latest_content
        assert "peer-certificate" in latest_content
        assert state_out.unit_status == ops.ActiveStatus()

@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_certificate_available_event_triggers_config_and_rotation(ctx, is_leader):
    """Test: CertificateAvailable event triggers config-changed and certificate rotation logic."""
    new_tls_context = certificate_available_context(ctx, is_leader)
    default_tls_context = default_certificate_context(ctx)
    context = new_tls_context.context
    peer_relation = new_tls_context.peer_relation
    bootstrap_relation = new_tls_context.bootstrap_relation
    peer_tls_relation = new_tls_context.peer_tls_relation
    client_tls_relation = new_tls_context.client_tls_relation
    provider_ca_certificate = new_tls_context.provider_ca_certificate
    peer_provider_certificate = new_tls_context.peer_provider_certificate
    peer_certificate = new_tls_context.peer_certificate
    requirer_private_key = new_tls_context.requirer_private_key
    client_provider_certificate = new_tls_context.client_provider_certificate
    client_certificate = new_tls_context.client_certificate
    state_in = testing.State(
        relations=[peer_relation, peer_tls_relation, client_tls_relation, bootstrap_relation],
        leader=is_leader,
    )
    apply_default_certificates(new_tls_context.peer_relation, default_tls_context)
    with (
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificate",
            return_value=(peer_provider_certificate, requirer_private_key),
        ),
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificates",
            return_value=(peer_provider_certificate, requirer_private_key),
        ),
        context(context.on.relation_created(peer_tls_relation), state=state_in) as manager,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
    ):
        charm: CassandraCharm = manager.charm
        config_changed_observer = TestObserver(charm)
        charm.framework.observe(charm.on.config_changed, config_changed_observer.handler)
        certificate_available_event = MagicMock(spec=CertificateAvailableEvent)
        certificate_available_event.certificate = peer_certificate
        certificate_available_event.ca = provider_ca_certificate
        certificate_available_event.certificate_signing_request = peer_provider_certificate.certificate_signing_request
        certificate_available_event.chain = peer_provider_certificate.chain
        charm.tls_events._on_peer_certificate_available(certificate_available_event)
        manager.run()
        assert config_changed_observer.called, "Expected config_changed to be emitted"
        tls_state = charm.tls_events.requirer_state(charm.tls_events.peer_certificate)
        assert tls_state.certificate == peer_certificate
        assert tls_state.ca == provider_ca_certificate
        assert tls_state.rotation is True
    apply_default_certificates(new_tls_context.peer_relation, default_tls_context)
    with (
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificate",
            return_value=(client_provider_certificate, requirer_private_key),
        ),
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificates",
            return_value=(client_provider_certificate, requirer_private_key),
        ),
        context(context.on.relation_created(client_tls_relation), state=state_in) as manager,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
    ):
        charm: CassandraCharm = manager.charm
        config_changed_observer = TestObserver(charm)
        charm.framework.observe(charm.on.config_changed, config_changed_observer.handler)
        certificate_available_event = MagicMock(spec=CertificateAvailableEvent)
        certificate_available_event.certificate = client_certificate
        certificate_available_event.ca = provider_ca_certificate
        certificate_available_event.certificate_signing_request = client_provider_certificate.certificate_signing_request
        certificate_available_event.chain = client_provider_certificate.chain
        certificate_available_event.private_key = requirer_private_key
        charm.tls_events._on_client_certificate_available(certificate_available_event)
        manager.run()
        assert config_changed_observer.called, "Expected config_changed to be emitted"
        tls_state = charm.tls_events.requirer_state(charm.tls_events.client_certificate)
        assert tls_state.certificate == client_certificate
        assert tls_state.ca == provider_ca_certificate
        assert tls_state.rotation is False
