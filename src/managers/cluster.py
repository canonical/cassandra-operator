#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
import socket
from typing import List

from common.literals import CassandraClusterState, Status
from common.management_client import ManagementClient, Node
from common.workload import WorkloadBase
from core.state import ApplicationState

logger = logging.getLogger(__name__)


class ClusterManager:
    """Manage cluster members, quorum and authorization."""

    def __init__(self, state: ApplicationState, workload: WorkloadBase):
        self.state = state
        self.workload = workload
        self.cluster_endpoints = [server.peer_url for server in self.state.nodes]

    def start_node(self) -> None:
        """Start a cluster node and update its status."""
        self.workload.start()

        self.state.unit_context
        self.state.unit_context.state = "started"
        if not self.state.cluster_context.cluster_state:
            # mark the cluster as initialized
            self.state.cluster_context.cluster_state = CassandraClusterState.EXISTING.value
            self.state.cluster_context.cluster_nodes = self.state.unit_context.node_endpoint

    def node(self) -> Node:
        """TODO."""
        logger.debug(f"Getting node for unit {self.state.unit_context.node_name}")

        client = ManagementClient(
            self.state.unit_context.ip,
        )

        node_list = client.node_list()

        if node_list is None:
            raise ValueError("member list command failed")
        if self.state.unit_context.node_name not in node_list:
            raise ValueError("member name not found")

        logger.debug(f"Member: {node_list[self.state.unit_context.node_name].id}")
        return node_list[self.state.unit_context.node_name]

    def get_host_mapping(self) -> dict[str, str]:
        """Collect hostname mapping for current unit.

        Returns:
            dict[str, str]: Dict of string keys 'hostname', 'ip' and their values
        """
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)

        return {"hostname": hostname, "ip": ip}

    def compute_component_status(self) -> List[Status]:
        """Compute the Cluster manager's statuses."""
        status_list = []

        if self.state.unit_context.is_started:
            if self.state.cluster_context.cluster_state != CassandraClusterState.EXISTING.value:
                status_list.append(Status.CLUSTER_NOT_INITIALIZED)

            # if not self.state.cluster_context.auth_enabled:
            #     status_list.append(Status.AUTHENTICATION_NOT_ENABLED)

        if not self.state.peer_relation:
            status_list.append(Status.SERVICE_INSTALLING)

        if not self.state.cluster_context.cluster_state:
            status_list.append(Status.CLUSTER_INITIALIZING)

        return status_list

    def is_healthy(self) -> bool:
        """Perform cassandra helth and readiness checks and return True if healthy.

        Returns:
            bool: True if the cluster or node is healthy.
        """
        client = ManagementClient(self.state.unit_context.client_mgmt_url)
        return client.is_healthy()

    def broadcast_peer_url(self, peer_urls: str) -> None:
        """Broadcast the peer URL to all units in the cluster.

        Args:
            peer_urls (str): The peer URLs to broadcast.
        """
        logger.debug(
            f"Broadcasting peer URL: {peer_urls} for unit {self.state.unit_context.node_name}"
        )
        pass

    def restart_node(self) -> bool:
        """TODO."""
        logger.debug("Restarting workload")
        self.workload.restart()
        return self.is_healthy()
