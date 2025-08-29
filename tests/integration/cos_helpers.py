#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


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
