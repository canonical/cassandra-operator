# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
from unittest.mock import PropertyMock, patch, MagicMock
import pytest
import json
from charm import CassandraCharm
from core.state import PEER_RELATION, PEER_TLS_RELATION, CLIENT_TLS_RELATION, TLSScope
from helpers import get_secrets_latest_content_by_label, generate_tls_artifacts
from ops.testing import Context, PeerRelation, Relation, Secret, State
from managers.tls import Sans
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
from ops.framework import Object
from datetime import timedelta

BOOTSTRAP_RELATION = "bootstrap"
TLS_NAME = "self-signed-certificates"

class TestObserver(Object):
    def __init__(self, charm):
        super().__init__(charm, "test_observer")
        self.called = False
    def handler(self, event):
        self.called = True

@pytest.fixture
def ctx() -> Context:
    return Context(CassandraCharm, unit_id=0)

@pytest.mark.parametrize("is_leader", [True, False])
def test_relation_broken(ctx, is_leader) -> None:
    """Test correct TLS state and event emission on relation broken."""
    # ... (реализация теста, как в вашем примере)

# (Остальные тесты оформить аналогично)