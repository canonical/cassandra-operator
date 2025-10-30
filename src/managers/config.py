#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Config manager."""

import logging
from typing import Any, Iterable

import yaml

from core.state import JMX_EXPORTER_PORT, TLSScope
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manager of config files."""

    def __init__(
        self,
        workload: WorkloadBase,
        cluster_name: str,
        listen_address: str,
        seeds: set[str],
        enable_peer_tls: bool,
        enable_client_tls: bool,
        keystore_password: str | None,
        truststore_password: str | None,
        authentication: bool,
    ):
        self.workload = workload
        self.cluster_name = cluster_name
        self.listen_address = listen_address
        self.seeds = seeds
        self.enable_peer_tls = enable_peer_tls
        self.enable_client_tls = enable_client_tls
        self.keystore_password = keystore_password
        self.truststore_password = truststore_password
        self.authentication = authentication

    def render_cassandra_config(
        self,
        cluster_name: str | None = None,
        listen_address: str | None = None,
        seeds: set[str] | None = None,
        enable_peer_tls: bool | None = None,
        enable_client_tls: bool | None = None,
        keystore_password: str | None = None,
        truststore_password: str | None = None,
        authentication: bool | None = None,
    ) -> bool:
        """Generate and write cassandra config.

        Returns:
            whether config was changed.
        """
        content = yaml.dump(
            self._cassandra_default_config()
            | self._cassandra_directories_config()
            | self._cassandra_connectivity_config(
                cluster_name=cluster_name or self.cluster_name,
                listen_address=listen_address or self.listen_address,
                seeds=seeds or self.seeds,
            )
            | self._cassandra_authentication_config(
                authentication if authentication is not None else self.authentication
            )
            | self._cassandra_peer_tls_config(
                enabled=enable_peer_tls if enable_peer_tls is not None else self.enable_peer_tls,
                keystore_password=keystore_password or self.keystore_password,
                truststore_password=truststore_password or self.truststore_password,
            )
            | self._cassandra_client_tls_config(
                enabled=enable_client_tls
                if enable_client_tls is not None
                else self.enable_client_tls,
                keystore_password=keystore_password or self.keystore_password,
                truststore_password=truststore_password or self.truststore_password,
            ),
            allow_unicode=True,
            default_flow_style=False,
        )

        if (
            self.workload.cassandra_paths.config.exists()
            and self.workload.cassandra_paths.config.read_text() == content
        ):
            return False

        self.workload.cassandra_paths.config.write_text(content)
        return True

    def render_env(self, cassandra_limit_memory_mb: int | None) -> bool:
        """Update environment config.

        Returns:
            whether config was changed.
        """
        content = self._render_env(
            self._map_env(self.workload.cassandra_paths.env.read_text().split("\n"))
            | self._env_heap_config(cassandra_limit_memory_mb=cassandra_limit_memory_mb)
            | self._env_jmx_exporter_config(
                self.workload.cassandra_paths.jmx_exporter.as_posix(),
                self.workload.cassandra_paths.jmx_exporter_config.as_posix(),
            )
        )

        if (
            self.workload.cassandra_paths.env.exists()
            and self.workload.cassandra_paths.env.read_text() == content
        ):
            return False

        self.workload.cassandra_paths.env.write_text(content)
        return True

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
    def _render_env(env: dict[str, str]) -> str:
        return "\n".join([f"{key}={value}" for key, value in env.items()])

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
    def _env_jmx_exporter_config(
        agent_path: str | None, agent_config_path: str | None
    ) -> dict[str, str]:
        return {
            "JVM_EXTRA_OPTS": f"-javaagent:{agent_path}={JMX_EXPORTER_PORT}:{agent_config_path}"
            if agent_path
            else "",
        }

    def _cassandra_client_tls_config(
        self,
        enabled: bool,
        keystore_password: str | None,
        truststore_password: str | None,
    ) -> dict[str, Any]:
        return (
            {
                "client_encryption_options": {
                    "enabled": True,
                    "optional": False,
                    "keystore": self.workload.cassandra_paths.get_keystore(
                        TLSScope.CLIENT
                    ).as_posix(),
                    "keystore_password": keystore_password,
                    "truststore": self.workload.cassandra_paths.get_truststore(
                        TLSScope.CLIENT
                    ).as_posix(),
                    "truststore_password": truststore_password,
                    "require_client_auth": False,  # mTLS is disabled
                    "algorithm": "SunX509",
                    "store_type": "JKS",
                    "protocol": "TLS",
                }
            }
            if enabled
            else {}
        )

    def _cassandra_peer_tls_config(
        self,
        enabled: bool,
        keystore_password: str | None,
        truststore_password: str | None,
    ) -> dict[str, Any]:
        return (
            {
                "server_encryption_options": {
                    "internode_encryption": "all",
                    "keystore": self.workload.cassandra_paths.get_keystore(
                        TLSScope.PEER
                    ).as_posix(),
                    "keystore_password": keystore_password,
                    "truststore": self.workload.cassandra_paths.get_truststore(
                        TLSScope.PEER
                    ).as_posix(),
                    "truststore_password": truststore_password,
                    "require_client_auth": True,
                    "algorithm": "SunX509",
                    "store_type": "JKS",
                    "protocol": "TLS",
                }
            }
            if enabled
            else {}
        )

    @staticmethod
    def _cassandra_authentication_config(enabled: bool) -> dict[str, Any]:
        return {
            "authenticator": "PasswordAuthenticator" if enabled else "AllowAllAuthenticator",
        }

    @staticmethod
    def _cassandra_connectivity_config(
        cluster_name: str, listen_address: str, seeds: set[str]
    ) -> dict[str, Any]:
        return {
            "cluster_name": cluster_name,
            "listen_address": listen_address,
            "rpc_address": listen_address,
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": ",".join(seeds)}],
                }
            ],
        }

    def _cassandra_directories_config(self) -> dict[str, Any]:
        return {
            "commitlog_directory": self.workload.cassandra_paths.commitlog_directory.as_posix(),
            "data_file_directories": [
                self.workload.cassandra_paths.data_file_directory.as_posix()
            ],
            "hints_directory": self.workload.cassandra_paths.hints_directory.as_posix(),
            "saved_caches_directory": (
                self.workload.cassandra_paths.saved_caches_directory.as_posix()
            ),
        }

    @staticmethod
    def _cassandra_default_config() -> dict[str, Any]:
        return {
            "allocate_tokens_for_local_replication_factor": 3,
            "authorizer": "CassandraAuthorizer",
            "cas_contention_timeout": "1000ms",
            "cidr_authorizer": {"class_name": "AllowAllCIDRAuthorizer"},
            "commitlog_sync": "periodic",
            "commitlog_sync_period": "10000ms",
            "crypto_provider": [
                {
                    "class_name": "org.apache.cassandra.security.DefaultCryptoProvider",
                    "parameters": [{"fail_on_missing_provider": "false"}],
                }
            ],
            "disk_failure_policy": "stop",
            "endpoint_snitch": "SimpleSnitch",
            "inter_dc_tcp_nodelay": False,
            "internode_compression": "dc",
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
            "storage_compatibility_mode": "CASSANDRA_4",
            "storage_port": 7000,
        }
