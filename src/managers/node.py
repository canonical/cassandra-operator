#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cluster manager."""

import logging
import re
import socket
from dataclasses import dataclass

from common.exceptions import ExecError
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)

NODETOOL = "charmed-cassandra.nodetool"


@dataclass
class GossipNode:
    """Node gossip info."""

    ip: str
    rpc_ready: str | None = None
    status_with_port: str | None = None


class NodeManager:
    """Manager of Cassandra cluster, including this unit."""

    def __init__(self, workload: WorkloadBase):
        self._workload = workload

    def is_healthy(self, ip: str) -> bool:
        """Whether Cassandra healthy and ready in this unit."""
        if not self._is_in_cluster(ip):
            return False
        if not self._is_gossip_ready:
            return False
        if not self._is_gossip_active:
            return False
        return True

    @property
    def is_bootstrap_pending(self) -> bool:
        """Whether `nodetool info` bootstrap state is NEEDS_BOOTSTRAP."""
        try:
            stdout, _ = self._workload.exec([NODETOOL, "info"], suppress_error_log=True)
            return "Bootstrap state        : NEEDS_BOOTSTRAP" in stdout
        except ExecError:
            return False

    @property
    def is_bootstrap_in_unknown_state(self) -> bool:
        """Whether `nodetool info` bootstrap state is not IN_PROGRESS or COMPLETED."""
        try:
            stdout, _ = self._workload.exec([NODETOOL, "info"], suppress_error_log=True)
            return (
                "Bootstrap state        : IN_PROGRESS" not in stdout
                and "Bootstrap state        : COMPLETED" not in stdout
            )
        except ExecError:
            return False

    @property
    def is_bootstrap_decommissioning(self) -> bool:
        """Check if the node is in the process of, or has completed, decommissioning."""
        try:
            stdout, _ = self._workload.exec([NODETOOL, "info"], suppress_error_log=True)
            if "Bootstrap state        : DECOMMISSIONED" in stdout:
                return True

            stdout, _ = self._workload.exec([NODETOOL, "info"], suppress_error_log=True)
            return "Decommissioning        : true" in stdout

        except ExecError:
            return False

    def network_address(self) -> tuple[str, str]:
        """Get IP and hostname of this unit."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname

    def resume_bootstrap(self) -> bool:
        """Resume Cassandra bootstrap.

        Returns:
            whether operation was successful.
        """
        try:
            self._workload.exec([NODETOOL, "bootstrap", "resume"])
            return True
        except ExecError:
            return False

    def prepare_shutdown(self) -> None:
        """Prepare Cassandra unit for safe shutdown using nodetool drain command."""
        self._workload.exec([NODETOOL, "drain"])

    def decommission(self) -> None:
        """Disconnect node from the cluster using nodetool decommission command."""
        self._workload.exec([NODETOOL, "decommission", "-f"])

    def ensure_cluster_topology(self, nodes: int) -> bool:
        """Check stability & consistency of the cluster topology using nodetool status command.

        Returns:
            whether the cluster topology is stable and consistent with provided nodes count.
        """
        try:
            stdout, _ = self._workload.exec([NODETOOL, "status"], suppress_error_log=True)
            if "DN  " in stdout:
                return False
            return stdout.count("UN  ") == nodes
        except ExecError:
            return False

    def remove_bad_nodes(self, good_node_ips: list[str]) -> None:
        """Remove unknown nodes in down state using nodetool removenode command."""
        try:
            status_stdout, _ = self._workload.exec([NODETOOL, "status"])
        except ExecError:
            return

        down_nodes = re.findall(
            r"^DN  (\S+)\s+(?:\?|\S+\s+\S+)(?:\s+\S+){2}\s+(\S+).+$", status_stdout, re.MULTILINE
        )

        for down_node in down_nodes:
            ip, host_id = down_node
            if ip in good_node_ips:
                continue
            try:
                self._workload.exec([NODETOOL, "removenode", host_id])
                logger.info(f"Removed bad node {ip} from Cassandra cluster")
            except ExecError:
                logger.error(f"Failed to remove bad node {ip} from Cassandra cluster")

    def repair_auth(self) -> None:
        """Run full repair on system_auth keyspace."""
        self._workload.exec([NODETOOL, "repair", "system_auth", "--full"])

    def _is_in_cluster(self, ip: str) -> bool:
        if not ip:
            return False
        try:
            stdout, _ = self._workload.exec([NODETOOL, "status"], suppress_error_log=True)
            return f"UN  {ip}" in stdout
        except ExecError:
            return False

    @property
    def _is_gossip_active(self) -> bool:
        try:
            stdout, _ = self._workload.exec([NODETOOL, "info"], suppress_error_log=True)
            return "Gossip active          : true" in stdout
        except ExecError:
            return False

    @property
    def _is_gossip_ready(self) -> bool:
        gossip_info = self.get_gossipinfo().get(self.network_address()[0])
        if not gossip_info:
            gossip_info = self.get_gossipinfo().get("127.0.0.1")

        if not gossip_info or "NORMAL" not in str(gossip_info.status_with_port):
            return False

        return True

    def get_gossipinfo(self) -> dict[str, GossipNode]:
        """Get `nodetool gossipinfo` command.

        Result in to the dict:
        { <IP>: GossipNode, ... }
        """
        try:
            text, _ = self._workload.exec([NODETOOL, "gossipinfo"], suppress_error_log=True)
        except ExecError:
            return {}

        result = {}
        blocks = text.strip().split("\n/")
        for block in blocks:
            lines = block.strip().splitlines()
            if not lines:
                continue
            first_line = lines[0].lstrip("/")
            ip = first_line.split()[0]
            node_data = {}
            for line in lines[1:]:
                if ":" not in line:
                    continue
                key, *rest = line.strip().split(":", maxsplit=2)
                if len(rest) == 1:
                    value = rest[0]
                elif len(rest) == 2:
                    _, value = rest
                else:
                    value = ":".join(rest)
                node_data[key.strip().upper()] = value.strip()

            node = GossipNode(
                ip=ip,
                rpc_ready=node_data.get("RPC_READY"),
                status_with_port=node_data.get("STATUS_WITH_PORT"),
            )
            result[ip] = node
        return result
