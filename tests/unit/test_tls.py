# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
import dataclasses
import json
from datetime import timedelta
from typing import Iterable
from unittest.mock import MagicMock, PropertyMock, patch

import ops
import pytest
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
from ops import testing
from ops.testing import Context, PeerRelation, Secret

from charm import CassandraCharm
from core.state import CLIENT_TLS_RELATION, PEER_RELATION, PEER_TLS_RELATION, TLSScope

BOOTSTRAP_RELATION = "bootstrap"
TLS_NAME = "self-signed-certificates"


@dataclasses.dataclass
class DefaultCertificateContext:
    context: Context[CassandraCharm]
    peer_relation: testing.PeerRelation
    peer_tls_relation: testing.Relation
    client_tls_relation: testing.Relation
    bootstrap_relation: testing.PeerRelation

    default_provider_pk: PrivateKey
    default_provider_ca_crt: Certificate

    default_requirer_private_key: PrivateKey
    default_peer_crt: Certificate
    default_peer_csr: CertificateSigningRequest
    default_peer_provider_crt: ProviderCertificate


@dataclasses.dataclass
class CertificateAvailableContext:
    context: Context[CassandraCharm]
    peer_relation: testing.PeerRelation
    peer_tls_relation: testing.Relation
    client_tls_relation: testing.Relation
    bootstrap_relation: testing.PeerRelation

    provider_pk: PrivateKey
    provider_ca_crt: Certificate

    requirer_private_key: PrivateKey
    peer_csr: CertificateSigningRequest
    peer_crt: Certificate
    peer_provider_crt: ProviderCertificate

    client_csr: CertificateSigningRequest
    client_crt: Certificate
    client_provider_crt: ProviderCertificate


@pytest.fixture
def ctx() -> Context[CassandraCharm]:
    return Context(CassandraCharm, unit_id=0)


def default_certificate_context(ctx: Context[CassandraCharm]) -> DefaultCertificateContext:
    peer_relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"workload_state": "active"}
    )
    peer_tls_relation = testing.Relation(id=2, endpoint=PEER_TLS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION)
    bootstrap_relation = testing.PeerRelation(id=4, endpoint=BOOTSTRAP_RELATION)

    default_provider_pk = generate_private_key()
    default_provider_ca_crt = generate_ca(
        private_key=default_provider_pk,
        common_name="example.com",
        validity=timedelta(days=365),
    )

    default_requirer_private_key = generate_private_key()
    default_peer_csr = generate_csr(
        private_key=default_requirer_private_key,
        common_name="cas-test-1",
        organization=TLSScope.PEER.value,
    )
    default_peer_crt = generate_certificate(
        ca_private_key=default_provider_pk,
        csr=default_peer_csr,
        ca=default_provider_ca_crt,
        validity=timedelta(days=1),
    )
    default_peer_provider_crt = ProviderCertificate(
        relation_id=peer_relation.id,
        certificate=default_peer_crt,
        certificate_signing_request=default_peer_csr,
        ca=default_provider_ca_crt,
        chain=[default_provider_ca_crt, default_peer_crt],
        revoked=False,
    )

    return DefaultCertificateContext(
        context=ctx,
        peer_relation=peer_relation,
        peer_tls_relation=peer_tls_relation,
        client_tls_relation=client_tls_relation,
        bootstrap_relation=bootstrap_relation,
        default_provider_pk=default_provider_pk,
        default_provider_ca_crt=default_provider_ca_crt,
        default_requirer_private_key=default_requirer_private_key,
        default_peer_crt=default_peer_crt,
        default_peer_csr=default_peer_csr,
        default_peer_provider_crt=default_peer_provider_crt,
    )


def apply_default_certificates(relation: PeerRelation, default_crts: DefaultCertificateContext):
    unit_secret = {
        "peer-private-key-secret": default_crts.default_requirer_private_key.raw,
        "peer-ca-cert-secret": default_crts.default_provider_ca_crt.raw,
        "peer-certificate-secret": default_crts.default_peer_crt.raw,
        "peer-csr-secret": default_crts.default_peer_csr.raw,
        "peer-chain-secret": json.dumps(
            [str(c) for c in default_crts.default_peer_provider_crt.chain]
        ),
    }
    app_secret = {
        "internal-ca-secret": default_crts.default_provider_ca_crt.raw,
        "internal-ca-key-secret": default_crts.default_provider_pk.raw,
    }
    relation.local_unit_data.update(unit_secret)
    relation.local_app_data.update(app_secret)


def apply_available_certificates(relation: PeerRelation, new_crts: CertificateAvailableContext):
    unit_secret = {
        "peer-private-key-secret": new_crts.requirer_private_key.raw,
        "peer-ca-cert-secret": new_crts.provider_ca_crt.raw,
        "peer-certificate-secret": new_crts.peer_crt.raw,
        "peer-csr-secret": new_crts.peer_csr.raw,
        "peer-chain-secret": json.dumps([str(c) for c in new_crts.peer_provider_crt.chain]),
        "client-private-key-secret": new_crts.requirer_private_key.raw,
        "client-ca-cert-secret-secret": new_crts.provider_ca_crt.raw,
        "client-certificate-secret": new_crts.client_crt.raw,
        "client-csr-secret": new_crts.client_csr.raw,
        "client-chain-secret": json.dumps([str(c) for c in new_crts.client_provider_crt.chain]),
    }
    app_secret = {
        "internal-ca-secret": new_crts.provider_ca_crt.raw,
        "internal-ca-key-secret": new_crts.provider_pk.raw,
    }
    relation.local_unit_data.update(unit_secret)
    relation.local_app_data.update(app_secret)


def certificate_available_context(ctx: Context[CassandraCharm]) -> CertificateAvailableContext:
    """Create a context for testing certificate available event."""
    peer_relation = testing.PeerRelation(
        id=1, endpoint=PEER_RELATION, local_unit_data={"workload_state": "active"}
    )
    peer_tls_relation = testing.Relation(id=2, endpoint=PEER_TLS_RELATION)
    client_tls_relation = testing.Relation(id=3, endpoint=CLIENT_TLS_RELATION)
    bootstrap_relation = testing.PeerRelation(id=4, endpoint=BOOTSTRAP_RELATION)

    provider_pk = generate_private_key()
    provider_ca_crt = generate_ca(
        private_key=provider_pk,
        common_name="example.com",
        validity=timedelta(days=365),
    )
    requirer_private_key = generate_private_key()
    peer_csr = generate_csr(
        private_key=requirer_private_key,
        common_name="cas-test-1",
        organization=TLSScope.PEER.value,
    )
    peer_crt = generate_certificate(
        ca_private_key=provider_pk,
        csr=peer_csr,
        ca=provider_ca_crt,
        validity=timedelta(days=1),
    )
    peer_provider_crt = ProviderCertificate(
        relation_id=peer_tls_relation.id,
        certificate=peer_crt,
        certificate_signing_request=peer_csr,
        ca=provider_ca_crt,
        chain=[provider_ca_crt, peer_crt],
        revoked=False,
    )
    client_csr = generate_csr(
        private_key=requirer_private_key,
        common_name="cas-test-1",
        organization=TLSScope.CLIENT.value,
    )
    client_crt = generate_certificate(
        ca_private_key=provider_pk,
        csr=client_csr,
        ca=provider_ca_crt,
        validity=timedelta(days=1),
    )
    client_provider_crt = ProviderCertificate(
        relation_id=client_tls_relation.id,
        certificate=client_crt,
        certificate_signing_request=client_csr,
        ca=provider_ca_crt,
        chain=[provider_ca_crt, client_crt],
        revoked=False,
    )
    return CertificateAvailableContext(
        context=ctx,
        peer_relation=peer_relation,
        peer_tls_relation=peer_tls_relation,
        client_tls_relation=client_tls_relation,
        bootstrap_relation=bootstrap_relation,
        provider_pk=provider_pk,
        provider_ca_crt=provider_ca_crt,
        requirer_private_key=requirer_private_key,
        peer_csr=peer_csr,
        peer_crt=peer_crt,
        peer_provider_crt=peer_provider_crt,
        client_csr=client_csr,
        client_crt=client_crt,
        client_provider_crt=client_provider_crt,
    )


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
    """When the TLS relation is broken, certificates are reset, config is triggered."""
    new_tls_context = certificate_available_context(ctx)
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
        patch("workload.snap.SnapCache"),
        patch("charm.CassandraCharm.restart") as restart,
        new_tls_context.context(
            new_tls_context.context.on.relation_broken(new_tls_context.peer_tls_relation),
            state=state_in,
        ) as manager,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure") as mock_configure,
        patch("managers.tls.TLSManager.remove_stores") as mock_remove_stores,
        patch("managers.tls.TLSManager.generate_internal_ca") as mock_gen_ca,
        patch("managers.tls.TLSManager.generate_internal_credentials") as mock_gen_internal_creds,
    ):
        charm: CassandraCharm = manager.charm

        mock_gen_internal_creds.return_value = (
            default_tls_context.default_peer_provider_crt,
            default_tls_context.default_requirer_private_key,
        )

        manager.run()
        peer_tls = charm.tls_events.requirer_state(charm.tls_events.peer_certificate)

        assert peer_tls.certificate != new_tls_context.peer_crt
        assert peer_tls.ca == new_tls_context.provider_ca_crt
        assert peer_tls.chain != new_tls_context.peer_provider_crt.chain
        assert peer_tls.csr != new_tls_context.peer_csr

        mock_remove_stores.assert_called_once()
        mock_configure.assert_called_once()
        mock_gen_ca.assert_not_called()
        mock_gen_internal_creds.assert_called_once()

        restart.assert_called_once()


@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_enabled_but_not_ready_sets_waiting_status(ctx, is_leader):
    """If TLS is enabled but files are not set, unit goes to WaitingStatus."""
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
        patch("charm.CassandraWorkload") as workload,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch(
            "core.state.ClusterContext.tls_state", new_callable=PropertyMock(return_value="active")
        ),
    ):
        workload.return_value.generate_string.return_value = "password"

        state_out = ctx.run(
            ctx.on.relation_created(default_tls_context.client_tls_relation), state_in
        )
        assert state_out.unit_status == ops.WaitingStatus("waiting for TLS setup")


@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_relation_created_sets_tls_state(ctx, is_leader):
    """Relating the charm toggles TLS state in the databag depending on leadership."""
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    bootstrap_relation = testing.PeerRelation(id=2, endpoint=BOOTSTRAP_RELATION)
    client_tls_relation = testing.PeerRelation(id=3, endpoint=CLIENT_TLS_RELATION)
    state = testing.State(
        leader=is_leader, relations={relation, bootstrap_relation, client_tls_relation}
    )

    with (
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("charm.CassandraWorkload") as workload,
    ):
        workload.return_value.generate_string.return_value = "password"

        state_out = ctx.run(ctx.on.relation_created(client_tls_relation), state)

        assert state_out.get_relation(relation.id).local_app_data.get("tls_state", "") == (
            "active" if is_leader else ""
        )


def test_tls_default_certificates_files_setup(ctx):
    """Default internal TLS of a new cluster creates all required files."""
    default_tls_context = default_certificate_context(ctx)
    state_in = testing.State(
        relations=[default_tls_context.peer_relation, default_tls_context.bootstrap_relation],
        leader=True,
    )
    with (
        patch("charm.CassandraWorkload") as workload,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch("managers.database.DatabaseManager.init_admin"),
        patch(
            "managers.node.NodeManager.is_healthy",
            return_value=True,
        ),
        patch("charm.CassandraCharm.restart"),
    ):
        workload.return_value.generate_string.return_value = "password"

        state_out = ctx.run(ctx.on.start(), state_in)
        latest_content = get_secrets_latest_content_by_label(
            state_out.secrets, "cassandra-peers.cassandra.app", "application"
        )

        assert "internal-ca-secret" in latest_content
        assert "internal-ca-key-secret" in latest_content
        latest_content = get_secrets_latest_content_by_label(
            state_out.secrets, "cassandra-peers.cassandra.unit", "unit"
        )

        assert "peer-ca-cert-secret" in latest_content
        assert "peer-private-key-secret" in latest_content
        assert "peer-chain-secret" in latest_content
        assert "peer-csr-secret" in latest_content
        assert "peer-certificate-secret" in latest_content

    default_tls_context = default_certificate_context(ctx)
    state_in = testing.State(
        relations=[
            default_tls_context.peer_relation,
            default_tls_context.peer_tls_relation,
            default_tls_context.client_tls_relation,
            default_tls_context.bootstrap_relation,
        ],
        leader=True,
    )
    default_tls_context.peer_relation.local_app_data.update({"cluster_state": "active"})
    default_tls_context.peer_relation.local_unit_data.update({"workload_state": "active"})
    with (
        patch("charm.CassandraWorkload") as workload,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
        patch(
            "managers.tls.TLSManager.client_tls_ready",
            new_callable=PropertyMock(return_value=False),
        ),
        patch(
            "core.state.ClusterContext.internal_ca",
            new_callable=PropertyMock(return_value=default_tls_context.default_provider_ca_crt),
        ),
        patch(
            "core.state.ClusterContext.internal_ca_key",
            new_callable=PropertyMock(return_value=default_tls_context.default_provider_pk),
        ),
        patch("managers.database.DatabaseManager.init_admin"),
        patch("managers.node.NodeManager.is_healthy", return_value=True),
        patch("charm.CassandraCharm.restart"),
    ):
        workload.return_value.generate_string.return_value = "password"

        state_out = ctx.run(ctx.on.start(), state_in)
        latest_content = get_secrets_latest_content_by_label(
            state_out.secrets, "cassandra-peers.cassandra.unit", "unit"
        )
        assert "peer-ca-cert-secret" in latest_content
        assert "peer-private-key-secret" in latest_content
        assert "peer-chain-secret" in latest_content
        assert "peer-csr-secret" in latest_content
        assert "peer-certificate-secret" in latest_content


@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_certificate_available_event_triggers_config_and_rotation(ctx, is_leader):
    """CertificateAvailable event triggers config-changed and certificate rotation logic."""
    new_tls_context = certificate_available_context(ctx)
    default_tls_context = default_certificate_context(ctx)
    context = new_tls_context.context
    peer_relation = new_tls_context.peer_relation
    bootstrap_relation = new_tls_context.bootstrap_relation
    peer_tls_relation = new_tls_context.peer_tls_relation
    client_tls_relation = new_tls_context.client_tls_relation
    provider_ca_crt = new_tls_context.provider_ca_crt
    peer_provider_crt = new_tls_context.peer_provider_crt
    peer_crt = new_tls_context.peer_crt
    requirer_private_key = new_tls_context.requirer_private_key
    client_provider_crt = new_tls_context.client_provider_crt
    client_crt = new_tls_context.client_crt
    state_in = testing.State(
        relations=[
            peer_relation,
            peer_tls_relation,
            client_tls_relation,
            bootstrap_relation,
        ],
        leader=is_leader,
    )

    apply_default_certificates(new_tls_context.peer_relation, default_tls_context)
    with (
        patch("workload.snap.SnapCache"),
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificate",
            return_value=(peer_provider_crt, requirer_private_key),
        ),
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificates",
            return_value=(peer_provider_crt, requirer_private_key),
        ),
        patch("charm.CassandraCharm.restart"),
        context(context.on.relation_created(peer_tls_relation), state=state_in) as manager,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
    ):
        charm: CassandraCharm = manager.charm

        certificate_available_event = MagicMock(spec=CertificateAvailableEvent)
        certificate_available_event.certificate = peer_crt
        certificate_available_event.ca = provider_ca_crt
        certificate_available_event.certificate_signing_request = (
            peer_provider_crt.certificate_signing_request
        )
        certificate_available_event.chain = peer_provider_crt.chain

        charm.tls_events._on_peer_certificate_available(certificate_available_event)

        manager.run()

        tls_state = charm.tls_events.requirer_state(charm.tls_events.peer_certificate)
        assert tls_state.certificate == peer_crt
        assert tls_state.ca == provider_ca_crt

    apply_default_certificates(new_tls_context.peer_relation, default_tls_context)
    with (
        patch("workload.snap.SnapCache"),
        patch(
            "core.state.UnitContext.keystore_password",
            new_callable=PropertyMock(return_value="keystore_password"),
        ),
        patch(
            "core.state.UnitContext.truststore_password",
            new_callable=PropertyMock(return_value="truststore_password"),
        ),
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificate",
            return_value=(client_provider_crt, requirer_private_key),
        ),
        patch(
            "events.tls.TLSCertificatesRequiresV4.get_assigned_certificates",
            return_value=(client_provider_crt, requirer_private_key),
        ),
        patch("charm.CassandraCharm.restart"),
        context(context.on.relation_created(client_tls_relation), state=state_in) as manager,
        patch("managers.config.ConfigManager.render_env"),
        patch("managers.config.ConfigManager.render_cassandra_config"),
        patch("managers.tls.TLSManager.configure"),
    ):
        charm: CassandraCharm = manager.charm

        certificate_available_event = MagicMock(spec=CertificateAvailableEvent)
        certificate_available_event.certificate = client_crt
        certificate_available_event.ca = provider_ca_crt
        certificate_available_event.certificate_signing_request = (
            client_provider_crt.certificate_signing_request
        )
        certificate_available_event.chain = client_provider_crt.chain
        certificate_available_event.private_key = requirer_private_key

        charm.tls_events._on_client_certificate_available(certificate_available_event)

        manager.run()

        tls_state = charm.tls_events.requirer_state(charm.tls_events.client_certificate)
        assert tls_state.certificate == client_crt
        assert tls_state.ca == provider_ca_crt
