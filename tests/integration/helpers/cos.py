#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import requests

logger = logging.getLogger(__name__)


COS_METRICS_PORT = 7071


class COS:
    TRAEFIK = "traefik"
    GRAFANA = "grafana"
    LOKI = "loki"
    PROMETHEUS = "prometheus"
    GRAFANA_AGENT = "grafana-agent"
    PROMETHEUS_OFFER = "prometheus-receive-remote-write"
    LOKI_OFFER = "loki-logging"
    GRAFANA_OFFER = "grafana-dashboards"


class COSAssertions:
    APP = "cassandra"
    DASHBOARD_TITLE = "Cassandra Overview"
    PANELS_COUNT = 61
    PANELS_TO_CHECK = ("JVM", "CQL", "Disk usage", "Compaction")
    ALERTS_COUNT = 2
    LOG_STREAMS = (
        "/var/snap/charmed-cassandra/common/var/log/cassandra/debug.log",
        "/var/snap/charmed-cassandra/common/var/log/cassandra/gc.log",
    )


def prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{COS_METRICS_PORT}/metrics"
    logger.info(f"prometheus_exporter_data making request: {url}")
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.info(f"prometheus_exporter_data exception: {e}")
        return None

    if response.status_code == 200:
        return response.text

    return None


def all_prometheus_exporters_data(juju: jubilant.Juju, check_field: str, app_name: str) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    status = juju.status()
    for unit in status.apps[app_name].units.values():
        result = result and check_field in (prometheus_exporter_data(unit.public_address) or "")
    return result
