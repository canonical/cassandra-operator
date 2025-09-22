#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cluster manager."""

import logging
import socket

from common.exceptions import ExecError
from core.workload import WorkloadBase
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_NODETOOL = "charmed-cassandra.nodetool"

@dataclass
class GossipNode:
    ip: str
    rpc_ready: str | None = None
    status_with_port: str | None = None

class ClusterManager:
    """Manager of Cassandra cluster, including this unit."""

    def __init__(self, workload: WorkloadBase):
        self._workload = workload

    @property
    def is_healthy(self) -> bool:
        """Whether Cassandra healthy and ready in this unit."""
        gossip_info = self.get_gossipinfo().get(self.network_address()[0])
        if not gossip_info:
            gossip_info = self.get_gossipinfo().get("127.0.0.1")

        if not gossip_info or "NORMAL" not in str(gossip_info.status_with_port):
            return False
        
        try:
            stdout, _ = self._workload.exec([_NODETOOL, "info"], suppress_error_log=True)
            return "Native Transport active: true" in stdout
        except ExecError:
            return False

    def network_address(self) -> tuple[str, str]:
        """Get IP and hostname of this unit."""
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname), hostname

    def prepare_shutdown(self) -> None:
        """Prepare Cassandra unit for safe shutdown."""
        self._workload.exec([_NODETOOL, "drain"])

    def decommission(self) -> None:
        """Disconnect node from the cluster."""
        self._workload.exec([_NODETOOL, "decommission", "-f"])
        
    def cluster_healthy(self) -> bool:
        gossip = self.get_gossipinfo()
        if not gossip:
            return False

        for node in gossip.values():
            if "NORMAL" not in str(node.status_with_port):
                return False

        return True

    def get_gossipinfo(self) -> dict[str, GossipNode]:
        """
        Get `nodetool gossipinfo` command result in to the dict:
        { <IP>: GossipNode, ... }
        """

        try:
            text, _ = self._workload.exec([_NODETOOL, "gossipinfo"], suppress_error_log=True)
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
    
