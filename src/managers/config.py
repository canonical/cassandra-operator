#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Config manager."""

import logging
import hashlib
import random
from typing import Iterable

import yaml

from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manager of config files."""

    def __init__(
        self,
        workload: WorkloadBase,
    ):
        self.workload = workload

    def render_cassandra_config(
        self, cluster_name: str, listen_address: str, seeds: list[str], enable_tls: bool
    ) -> None:
        """Generate and write cassandra config."""
        config = {
            "allocate_tokens_for_local_replication_factor": 3,
            "authenticator": "AllowAllAuthenticator",
            "authorizer": "AllowAllAuthorizer",
            "cas_contention_timeout": "1000ms",
            "cidr_authorizer": {"class_name": "AllowAllCIDRAuthorizer"},
            "cluster_name": cluster_name,
            "commitlog_directory": self.workload.cassandra_paths.commitlog_directory.as_posix(),
            "commitlog_sync": "periodic",
            "commitlog_sync_period": "10000ms",
            "crypto_provider": [
                {
                    "class_name": "org.apache.cassandra.security.DefaultCryptoProvider",
                    "parameters": [{"fail_on_missing_provider": "false"}],
                }
            ],
            "data_file_directories": [
                self.workload.cassandra_paths.data_file_directory.as_posix()
            ],
            "disk_failure_policy": "stop",
            "endpoint_snitch": "SimpleSnitch",
            "hints_directory": self.workload.cassandra_paths.hints_directory.as_posix(),
            "inter_dc_tcp_nodelay": False,
            "internode_compression": "dc",
            "listen_address": listen_address,
            "broadcast_address": listen_address,
            "memtable": {
                "configurations": {
                    "default": {"inherits": "skiplist"},
                    "skiplist": {"class_name": "SkipListMemtable"},
                    "trie": {"class_name": "TrieMemtable"},
                },
            },
            "native_transport_port": 9042,
            "network_authorizer": "AllowAllNetworkAuthorizer",
            "num_tokens": 16,
            "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
            "replica_filtering_protection": {
                "cached_rows_fail_threshold": 32000,
                "cached_rows_warn_threshold": 2000,
            },
            "role_manager": "CassandraRoleManager",
            "rpc_address": listen_address,
            "broadcast_rpc_address": listen_address,
            "saved_caches_directory": (
                self.workload.cassandra_paths.saved_caches_directory.as_posix()
            ),
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": ",".join(seeds)}],
                }
            ],
            "storage_compatibility_mode": "CASSANDRA_4",
            "storage_port": 7000,
        }

        if enable_tls:
            config["server_encryption_options"] = {
                "internode_encryption": "all",
                "keystore": self.workload.cassandra_paths.peer_keystore.as_posix(),
                "keystore_password": "myStorePass",
                "truststore": self.workload.cassandra_paths.peer_truststore.as_posix(),
                "truststore_password": "myStorePass",
                "require_client_auth": True,
                "algorithm": "SunX509",
                "store_type": "JKS",
                "protocol": "TLS",
            }

            config["client_encryption_options"] = {
                "enabled": True,
                "optional": False,
                "keystore": self.workload.cassandra_paths.client_keystore.as_posix(),
                "keystore_password": "myStorePass",
                "truststore": self.workload.cassandra_paths.client_truststore.as_posix(),
                "truststore_password": "myStorePass",
                "require_client_auth": True,
                "algorithm": "SunX509",
                "store_type": "JKS",
                "protocol": "TLS",
            }

        self.workload.cassandra_paths.config.write_text(
            yaml.dump(config, allow_unicode=True, default_flow_style=False)
        )

    def render_env(self, cassandra_limit_memory_mb: int | None) -> None:
        """Update environment config."""
        self.workload.cassandra_paths.env.write_text(
            self._render_env(
                [
                    self._map_env(self.workload.cassandra_paths.env.read_text().split("\n")),
                    self._env_heap_config(cassandra_limit_memory_mb=cassandra_limit_memory_mb),
                    self._env_jvm_ops(ring_delay_ms=30000),
                ]
            )
        )

    @staticmethod
    def _map_env(env: Iterable[str]) -> dict[str, str]:
        """Parse env var into a dict."""
        map_env = {}
        for var in env:
            key = var.split("=", maxsplit=1)[0]
            value = "".join(var.split("=", maxsplit=1)[1:])
            if key:
                # only check for keys, as we can have an empty value for a variable
                map_env[key] = value
        return map_env

    @staticmethod
    def _render_env(envs: Iterable[dict[str, str]]) -> str:
        res = {}
        for env in envs:
            res.update(env)
        return "\n".join([f"{key}={value}" for key, value in res.items()])

    @staticmethod
    def _env_heap_config(cassandra_limit_memory_mb: int | None) -> dict[str, str]:
        if cassandra_limit_memory_mb is not None and cassandra_limit_memory_mb < 1024:
            raise ValueError("cassandra_limit_memory_mb should be at least 1024")
        return {
            "MAX_HEAP_SIZE": f"{cassandra_limit_memory_mb}M" if cassandra_limit_memory_mb else "",
            "HEAP_NEWSIZE": f"{cassandra_limit_memory_mb // 2}M"
            if cassandra_limit_memory_mb
            else "",
        }

    @staticmethod
    def _env_jvm_ops(ring_delay_ms: int | None) -> dict[str, str]:
        if ring_delay_ms is not None and ring_delay_ms <= 0:
            raise ValueError("ring_delay_ms should be at least 1000ms")
        return {
            "JVM_OPTS": f"$JVM_OPTS -Dcassandra.ring_delay_ms={ring_delay_ms}ms"
        }

