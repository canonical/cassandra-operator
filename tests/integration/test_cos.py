#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os
from pathlib import Path

import jubilant
from cassandra.cluster import ResultSet
from helpers import all_prometheus_exporters_data, get_microk8s_controller, juju_controller_env

logger = logging.getLogger(__name__)
COS_AGENT_APP_NAME = "grafana-agent"
COS_MODEL = ""

def test_deploy_cos(juju_k8s: jubilant.Juju, cos_name: str = "cos"):
    with juju_controller_env("microk8s-localhost"):
        juju_k8s.deploy(
            charm="cos-lite",
            trust=True,
        )

        juju_k8s.wait(lambda status: jubilant.all_active(status), delay=10)
        juju_k8s.wait(lambda status: jubilant.all_agents_idle(status), delay=10)

        juju_k8s.offer(f"{juju_k8s.model}.grafana", endpoint="grafana-dashboard", name="grafana-dashboards")
        juju_k8s.offer(f"{juju_k8s.model}.loki", endpoint="logging", name="loki-logging")
        juju_k8s.offer(f"{juju_k8s.model}.prometheus", endpoint="receive-remote-write", name="prometheus-receive-remote-write")

def test_deploy(juju_local: jubilant.Juju, cassandra_charm: Path, app_name: str) -> None:
    with juju_controller_env("localhost-localhost"):
        juju_local.deploy(
            cassandra_charm,
            app=app_name,
            config={"profile": "testing"},
            num_units=2,
        )
        juju_local.wait(jubilant.all_active, timeout=1000)

def test_cos_monitoring_setup(juju_local: jubilant.Juju, juju_k8s: jubilant.Juju, app_name: str) -> None:
    with juju_controller_env("microk8s-localhost"):
        k8s_model = juju_k8s.model
        juju_k8s.cli("offer", f"{k8s_model}.grafana:grafana-dashboard", "grafana-dashboards", include_model=False)
        juju_k8s.cli("offer", f"{k8s_model}.loki:logging", "loki-logging", include_model=False)
        juju_k8s.cli("offer", f"{k8s_model}.prometheus:receive-remote-write", "prometheus-receive-remote-write", include_model=False)

    with juju_controller_env("localhost-localhost"):
        assert all_prometheus_exporters_data(
            juju_local, check_field="cassandra_keyspace", app_name=app_name
        )

        juju_local.deploy(COS_AGENT_APP_NAME, num_units=1, base="ubuntu@24.04")
        juju_local.integrate(f"{app_name}:cos-agent", COS_AGENT_APP_NAME)

        juju_local.wait(lambda s: jubilant.all_blocked(s, COS_AGENT_APP_NAME), delay=5, timeout=1000)

        juju_local.cli("consume", f"microk8s-localhost:admin/{k8s_model}.grafana-dashboards")
        juju_local.cli("consume", f"microk8s-localhost:admin/{k8s_model}.loki-logging")
        juju_local.cli("consume", f"microk8s-localhost:admin/{k8s_model}.prometheus-receive-remote-write") 

        juju_local.integrate(COS_AGENT_APP_NAME, "grafana-dashboards")
        juju_local.integrate(COS_AGENT_APP_NAME, "loki-logging")
        juju_local.integrate(COS_AGENT_APP_NAME, "prometheus-receive-remote-write")

        juju_local.wait(lambda s: jubilant.all_active(s, COS_AGENT_APP_NAME), delay=10)
        juju_local.wait(lambda s: jubilant.all_agents_idle(s, COS_AGENT_APP_NAME), delay=10)

    with juju_controller_env("microk8s-localhost"):
        res = juju_k8s.run("grafana/0", "get-admin-password")
        assert res.results["admin-password"]
        assert res.results["url"]
