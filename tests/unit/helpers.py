# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing
#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable
from unittest.mock import MagicMock

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
from ops.testing import Secret


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
    """Generate necessary TLS artifacts for TLS tests.

    Args:
        subject (str, optional): Certificate Subject Name. Defaults to "some-app/0".
        sans_dns (list[str], optional): List of SANS DNS addresses. Defaults to ["localhost"].
        sans_ip (list[str], optional): List of SANS IP addresses. Defaults to ["127.0.0.1"].
        with_intermediate (bool, optional): Whether or not should use and intermediate CA
                                            to sign the end cert. Defaults to False.

    Returns:
        TLSArtifacts: Object containing required TLS Artifacts.
    """
    # CA
    ca_key = generate_private_key()
    ca = generate_ca(
        private_key=ca_key,
        validity=timedelta(365),
        common_name="some-CN",
    )

    signing_cert, signing_key = ca, ca_key

    # Intermediate?
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


def get_secrets_latest_content_by_label(
    secrets: Iterable["Secret"], label: str, owner: str
) -> dict[str, str]:
    """Return a concatenated dictionary of Secret objects."""
    result = {}
    for secret in secrets:
        if owner and getattr(secret, "owner", None) != owner:
            continue
        if getattr(secret, "label", None) == label:
            if hasattr(secret, "latest_content"):
                if secret.latest_content:
                    result.update(secret.latest_content)
    return result


def make_refresh_like():
    m = MagicMock()
    m.in_progress = False
    m.next_unit_allowed_to_refresh = True
    m.workload_allowed_to_start = True
    m.app_status_higher_priority = False
    m.unit_status_higher_priority = False
    m.unit_status_lower_priority.return_value = False
    return m
