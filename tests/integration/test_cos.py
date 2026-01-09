#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import jubilant
import requests
from requests.auth import HTTPBasicAuth

from integration.helpers.cos import COS, COSAssertions, all_prometheus_exporters_data
from integration.helpers.juju import using_k8s, using_vm

logger = logging.getLogger(__name__)
COS_AGENT_APP_NAME = "grafana-agent"


def test_deploy_cos(juju_k8s: jubilant.Juju, cos_name: str = "cos"):
    with using_k8s():
        juju_k8s.deploy(
            charm="cos-lite",
            trust=True,
        )

        juju_k8s.wait(lambda status: jubilant.all_active(status), delay=10)
        juju_k8s.wait(lambda status: jubilant.all_agents_idle(status), delay=10)

        juju_k8s.offer(
            f"{juju_k8s.model}.grafana", endpoint="grafana-dashboard", name="grafana-dashboards"
        )
        juju_k8s.offer(f"{juju_k8s.model}.loki", endpoint="logging", name="loki-logging")
        juju_k8s.offer(
            f"{juju_k8s.model}.prometheus",
            endpoint="receive-remote-write",
            name="prometheus-receive-remote-write",
        )


def test_deploy(juju: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    with using_vm():
        juju.deploy(
            cassandra_charm,
            app=app_name,
            config={"profile": "testing"},
            num_units=2,
        )
        juju.wait(
            ready=lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status),
            delay=20,
            timeout=1200,
        )


def test_cos_monitoring_setup(juju: jubilant.Juju, juju_k8s: jubilant.Juju, app_name: str) -> None:
    with using_k8s():
        cos_model = juju_k8s.model
        cos_controller = juju_k8s.status().model.controller
        juju_k8s.cli(
            "offer",
            f"{cos_model}.grafana:grafana-dashboard",
            "grafana-dashboards",
            include_model=False,
        )
        juju_k8s.cli("offer", f"{cos_model}.loki:logging", "loki-logging", include_model=False)
        juju_k8s.cli(
            "offer",
            f"{cos_model}.prometheus:receive-remote-write",
            "prometheus-receive-remote-write",
            include_model=False,
        )

    with using_vm():
        assert all_prometheus_exporters_data(
            juju, check_field="cassandra_keyspace", app_name=app_name
        )

        juju.deploy(COS_AGENT_APP_NAME, num_units=1, base="ubuntu@24.04")
        juju.integrate(f"{app_name}:cos-agent", COS_AGENT_APP_NAME)

        juju.wait(lambda s: jubilant.all_blocked(s, COS_AGENT_APP_NAME), delay=5, timeout=1000)

        juju.cli("consume", f"{cos_controller}:admin/{cos_model}.grafana-dashboards")
        juju.cli("consume", f"{cos_controller}:admin/{cos_model}.loki-logging")
        juju.cli("consume", f"{cos_controller}:admin/{cos_model}.prometheus-receive-remote-write")

        juju.integrate(COS_AGENT_APP_NAME, "grafana-dashboards")
        juju.integrate(COS_AGENT_APP_NAME, "loki-logging")
        juju.integrate(COS_AGENT_APP_NAME, "prometheus-receive-remote-write")

        juju.wait(lambda s: jubilant.all_active(s, COS_AGENT_APP_NAME), delay=10)
        juju.wait(lambda s: jubilant.all_agents_idle(s, COS_AGENT_APP_NAME), delay=10)

    with using_k8s():
        grafana_unit = next(iter(juju_k8s.status().apps[COS.GRAFANA].units.keys()))

        res = juju_k8s.run(grafana_unit, "get-admin-password")
        assert res.results["admin-password"]
        assert res.results["url"]


def test_grafana(juju_k8s: jubilant.Juju):
    """Checks Grafana dashboard is created with desired attributes."""
    with using_k8s():
        grafana_unit = next(iter(juju_k8s.status().apps[COS.GRAFANA].units.keys()))

        res = juju_k8s.run(grafana_unit, "get-admin-password")

        admin_pass = res.results["admin-password"]
        grafana_url = res.results["url"]

        auth = HTTPBasicAuth("admin", admin_pass)
        dashboards = requests.get(f"{grafana_url}/api/search?query=cassandra", auth=auth).json()

        assert dashboards

        match = [dash for dash in dashboards if dash["title"] == COSAssertions.DASHBOARD_TITLE]

        assert match

        app_dashboard = match[0]
        dashboard_uid = app_dashboard["uid"]

        details = requests.get(
            f"{grafana_url}/api/dashboards/uid/{dashboard_uid}", auth=auth
        ).json()

        panels = details["dashboard"]["panels"]

        assert len(panels) == COSAssertions.PANELS_COUNT

        panel_titles = [_panel.get("title") for _panel in panels]

        logger.warning(
            f"{len([i for i in panel_titles if not i])} panels don't \
            have title which might be an issue."
        )
        logger.warning(
            f'{len([i for i in panel_titles if i and i.title() != i])} \
            panels don\'t observe "Panel Title" format.'
        )

        for item in COSAssertions.PANELS_TO_CHECK:
            assert item in panel_titles

        logger.info(f"{COSAssertions.DASHBOARD_TITLE} dashboard has following panels:")
        for panel in panel_titles:
            logger.info(f"|__ {panel}")


def test_metrics_and_alerts(juju_k8s: jubilant.Juju):
    """Checks alert rules are submitted and metrics are being sent to prometheus."""
    # wait a couple of minutes for metrics to show up
    time.sleep(300)
    with using_k8s():
        traefik_unit = next(iter(juju_k8s.status().apps[COS.TRAEFIK].units.keys()))

        res = juju_k8s.run(traefik_unit, "show-proxied-endpoints")
        prometheus_url = json.loads(res.results["proxied-endpoints"])[f"{COS.PROMETHEUS}/0"]["url"]

        # metrics

        response = requests.get(f"{prometheus_url}/api/v1/label/__name__/values").json()
        metrics = [i for i in response["data"] if COSAssertions.APP in i]

        assert metrics, f"No {COSAssertions.APP} metrics found!"
        logger.info(f'{len(metrics)} metrics found for "{COSAssertions.APP}" in prometheus.')

        # alerts

        response = requests.get(f"{prometheus_url}/api/v1/rules?type=alert").json()

        match = [
            group
            for group in response["data"]["groups"]
            if COSAssertions.APP in group["name"].lower()
        ]

        assert match

        alerts = match[0]

        assert len(alerts["rules"]) == COSAssertions.ALERTS_COUNT

        logger.info(f"{len(alerts['rules'])} alert rules are registered:")
        for rule in alerts["rules"]:
            logger.info(f"|__ {rule['name']}")


def test_loki(juju_k8s: jubilant.Juju):
    """Checks log streams are being pushed to loki."""
    with using_k8s():
        traefik_unit = next(iter(juju_k8s.status().apps[COS.TRAEFIK].units.keys()))
        res = juju_k8s.run(traefik_unit, "show-proxied-endpoints")

        loki_url = json.loads(res.results["proxied-endpoints"])[f"{COS.LOKI}/0"]["url"]

        endpoint = f"{loki_url}/loki/api/v1/query_range"
        query = '{juju_application="cassandra"}'
        headers = {"Accept": "application/json"}
        limit = 5000
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {"query": query, "end": now, "limit": limit, "direction": "backward"}

        filenames = []

        response = requests.get(
            endpoint, params=payload, headers=headers, verify=False, timeout=30
        )
        response.raise_for_status()
        results = response.json().get("data", {}).get("result", [])

        for stream in results:
            filename = stream.get("stream", {}).get("filename")
            if filename:
                filenames.append(filename)

        assert len(filenames) >= len(COSAssertions.LOG_STREAMS)
        for _stream in COSAssertions.LOG_STREAMS:
            assert _stream in filenames

        logger.info("Displaying some of the logs pushed to loki:")
        for item in results:
            # we should have some logs
            assert len(item["values"]) > 0, f"No log pushed for {item['stream']['filename']}"

            logger.info(f"Stream: {item['stream']['filename']}")
            for _, log in item["values"][:10]:
                logger.info(f"|__ {log}")
            logger.info("\n")
