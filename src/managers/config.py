#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling configuration building + writing."""

import logging

import yaml

from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class ConfigManager:
    """Handle the configuration of Cassandra."""

    def __init__(
        self,
        workload: WorkloadBase,
    ):
        self.workload = workload

    def render_cassandra_config(
        self, cluster_name: str, listen_address: str, seeds: list[str]
    ) -> None:
        """TODO."""
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

        self.workload.cassandra_paths.config.write_text(
            yaml.dump(config, allow_unicode=True, default_flow_style=False)
        )
