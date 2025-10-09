#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
from help_types import IntegrationTestsCharms
from helpers import check_node_is_up, check_tls, get_unit_address, get_secrets_by_label, app_secret_extract, unit_secret_extract

logger = logging.getLogger(__name__)


TRUSTSTORE_PASSWORD = "truststore-password"
KEYSTORE_PASSWORD = "keystore-password"
CLIENT_CA_CERT = "client-ca-cert-secret"
PEER_CA_CERT = "peer-ca-cert-secret"

TLS_NAME = "self-signed-certificates"

PEER_PORT = 7000
CLIENT_PORT = 9042


def test_deploy_internal_tls(
    juju: jubilant.Juju,
    cassandra_charm: Path,
    app_name: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=2,
    )

    juju.deploy(
        **charm_versions.tls.deploy_dict(),
        config={"ca-common-name": "cassandra"},
    )

    juju.wait(jubilant.all_active, timeout=2000, delay=3)


def test_default_tls(juju: jubilant.Juju, app_name: str) -> None:
    num_unit = 0

    unit_addreses = [
        get_unit_address(juju=juju, app_name=app_name, unit_num=0),
        get_unit_address(juju=juju, app_name=app_name, unit_num=1),
    ]

    peer_ca = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=PEER_CA_CERT,
    )

    assert peer_ca

    for uaddr in unit_addreses:
        assert check_tls(ip=uaddr, port=PEER_PORT)

    # Enshure all nodes are joined to the cluster
    for i, uaddr in enumerate(unit_addreses):
        assert check_node_is_up(juju=juju, app_name=app_name, unit_num=i, unit_addr=uaddr)


def test_enable_peer_self_signed_tls(
    juju: jubilant.Juju,
    app_name: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    num_unit = 0

    unit_addreses = [
        get_unit_address(juju=juju, app_name=app_name, unit_num=0),
        get_unit_address(juju=juju, app_name=app_name, unit_num=1),
    ]

    peer_ca_1 = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=PEER_CA_CERT,
    )

    juju.integrate(f"{charm_versions.tls.app}:certificates", f"{app_name}:peer-certificates")

    # Wait for peer_certs rotation
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        successes=4,
        timeout=1000,
    )

    for uaddr in unit_addreses:
        assert check_tls(ip=uaddr, port=PEER_PORT)

    # Enshure all nodes are joined to the cluster
    for i, uaddr in enumerate(unit_addreses):
        assert check_node_is_up(
            juju=juju,
            app_name=app_name,
            unit_num=i,
            unit_addr=uaddr,
        )

    peer_ca_2 = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=PEER_CA_CERT,
    )

    assert peer_ca_1 != peer_ca_2


def test_enable_client_self_signed_tls(
    juju: jubilant.Juju,
    app_name: str,
    charm_versions: IntegrationTestsCharms,
) -> None:
    num_unit = 0

    unit_addreses = [
        get_unit_address(juju=juju, app_name=app_name, unit_num=0),
        get_unit_address(juju=juju, app_name=app_name, unit_num=1),
    ]

    juju.integrate(f"{charm_versions.tls.app}:certificates", f"{app_name}:client-certificates")

    # Wait for client_certs rotation
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        successes=4,
        timeout=1000,
    )

    for uaddr in unit_addreses:
        assert check_tls(ip=uaddr, port=CLIENT_PORT)

    # Enshure all nodes are joined to the cluster
    for i, uaddr in enumerate(unit_addreses):
        assert check_node_is_up(juju=juju, app_name=app_name, unit_num=i, unit_addr=uaddr)

    client_ca_2 = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=CLIENT_CA_CERT,
    )

    assert client_ca_2


def test_disable_peer_self_signed_tls(
    juju: jubilant.Juju, app_name: str, charm_versions: IntegrationTestsCharms
) -> None:
    num_unit = 0

    unit_addreses = [
        get_unit_address(juju=juju, app_name=app_name, unit_num=0),
        get_unit_address(juju=juju, app_name=app_name, unit_num=1),
    ]

    logger.info("[test_disable_peer_self_signed_tls] Get peer ca 1")
    peer_ca_1 = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=PEER_CA_CERT,
    )

    juju.remove_relation(f"{charm_versions.tls.app}:certificates", f"{app_name}:peer-certificates")

    # Wait for peer_certs rotation. Sometimes it does not waits for cert rotation due to low delay
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        successes=4,
        timeout=1000,
    )

    for uaddr in unit_addreses:
        assert check_tls(ip=uaddr, port=PEER_PORT)

    # Enshure all nodes are joined to the cluster
    for i, uaddr in enumerate(unit_addreses):
        assert check_node_is_up(
            juju=juju,
            app_name=app_name,
            unit_num=i,
            unit_addr=uaddr,
        )

    logger.info("[test_disable_peer_self_signed_tls] Get peer ca 2")
    peer_ca_2 = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=PEER_CA_CERT,
    )

    assert peer_ca_1 != peer_ca_2

def test_disable_client_self_signed_tls(
    juju: jubilant.Juju, app_name: str, charm_versions: IntegrationTestsCharms
) -> None:
    num_unit = 0

    unit_addreses = [
        get_unit_address(juju=juju, app_name=app_name, unit_num=0),
        get_unit_address(juju=juju, app_name=app_name, unit_num=1),
    ]

    juju.remove_relation(
        f"{charm_versions.tls.app}:certificates", f"{app_name}:client-certificates"
    )

    # Wait for client_certs rotation.
    # Sometimes it does not waits for cert rotation due to low delay
    juju.wait(
        ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
        delay=20,
        successes=4,
        timeout=1000,
    )

    for uaddr in unit_addreses:
        assert not check_tls(ip=uaddr, port=CLIENT_PORT)

    # Enshure all nodes are joined to the cluster
    for i, uaddr in enumerate(unit_addreses):
        assert check_node_is_up(
            juju=juju,
            app_name=app_name,
            unit_num=i,
            unit_addr=uaddr,
        )

    client_ca_2 = unit_secret_extract(
        juju,
        unit_name=f"{app_name}/{num_unit}",
        secret_name=CLIENT_CA_CERT,
    )

    assert not client_ca_2

