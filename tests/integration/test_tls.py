#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import json
import logging
import os
from pathlib import Path
from helpers import check_node_is_up, check_tls, get_address, get_secret_by_label
import tempfile

import jubilant

TRUSTSTORE_PASSWORD = "truststore-password"
KEYSTORE_PASSWORD = "keystore-password"
CLIENT_CA_CERT = "client-ca-cert"
CLIENT_CERTIFICATE = "client-certificate"
CLIENT_CHAIN = "client-chain"
CLIENT_CSR = "client-csr"
CLIENT_PRIVATE_KEY = "client-private-key"
PEER_CA_CERT = "peer-ca-cert"
PEER_CERTIFICATE = "peer-certificate"
PEER_CHAIN = "peer-chain"
PEER_CSR = "peer-csr"
PEER_PRIVATE_KEY = "peer-private-key"
INTERNAL_PRIVATE_KEY = "internal-ca-key"

PEER_PORT = 7000
CLIENT_PORT = 9042

def test_deploy_internal_tls(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    juju.deploy(
        cassandra_charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=2,
    )
    
    juju.wait(jubilant.all_active, timeout=1200, delay=3)

def test_default_tls(juju: jubilant.Juju, app_name: str) -> None:
    unit_addreses = [
        get_address(juju=juju, app_name=app_name, unit_num=0),
        get_address(juju=juju, app_name=app_name, unit_num=1),
    ]

    for uaddr in unit_addreses:
        assert check_tls(ip=uaddr, port=PEER_PORT)
        
    # Enshure all nodes are joined to the cluster
    for i, uaddr in enumerate(unit_addreses):
        assert check_node_is_up(juju=juju, app_name=app_name, unit_num=i, unit_addr=uaddr)

    

def unit_secret_extract(juju: jubilant.Juju, unit_name: str, secret_name: str) -> str | None:
    user_secret = get_secret_by_label(
        juju,
        label=f"cluster.{unit_name.split('/')[0]}.unit",
        owner=unit_name,
    )

    return user_secret.get(secret_name)

def app_secret_extract(juju: jubilant.Juju, cluster_name: str, secret_name: str) -> str | None:
    user_secret = get_secret_by_label(
        juju,
        label=f"cluster.{cluster_name}.application",
        owner=cluster_name,
    )

    return user_secret.get(secret_name)



