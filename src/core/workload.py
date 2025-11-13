#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Workload definition."""

import secrets
import string
from abc import ABC, abstractmethod
from typing import Literal

from charmlibs import pathops

from core.state import TLSScope

Substrate = Literal["vm", "k8s"]

JMX_EXPORTER_AGENT_FILE = "jmx_prometheus_javaagent-1.0.1.jar"


class CassandraPaths:
    """Filesystem paths of the Cassandra workload."""

    env: pathops.PathProtocol
    config_dir: pathops.PathProtocol
    data_dir: pathops.PathProtocol
    tls_dir: pathops.PathProtocol
    lib_dir: pathops.PathProtocol

    @property
    def config(self) -> pathops.PathProtocol:
        """Main config file."""
        return self.config_dir / "cassandra.yaml"

    @property
    def commitlog_directory(self) -> pathops.PathProtocol:
        """Commitlog data directory."""
        return self.data_dir / "commitlog"

    @property
    def data_file_directory(self) -> pathops.PathProtocol:
        """Main data directory."""
        return self.data_dir / "data"

    @property
    def hints_directory(self) -> pathops.PathProtocol:
        """Hints data directory."""
        return self.data_dir / "hints"

    @property
    def saved_caches_directory(self) -> pathops.PathProtocol:
        """Saved caches data directory."""
        return self.data_dir / "saved_caches"

    def get_truststore(self, scope: TLSScope) -> pathops.PathProtocol:
        """Get truststore path for the TLS scope."""
        return self.tls_dir / f"{scope.value}-truststore.jks"

    def get_keystore(self, scope: TLSScope) -> pathops.PathProtocol:
        """Get keystore path for the TLS scope."""
        return self.tls_dir / f"{scope.value}-keystore.p12"

    def get_ca(self, scope: TLSScope) -> pathops.PathProtocol:
        """Get ca path for the TLS scope."""
        return self.tls_dir / f"{scope.value}-ca.pem"

    def get_certificate(self, scope: TLSScope) -> pathops.PathProtocol:
        """Get certificate path for the TLS scope."""
        return self.tls_dir / f"{scope.value}-unit.pem"

    def get_private_key(self, scope: TLSScope) -> pathops.PathProtocol:
        """Get private key path for the TLS scope."""
        return self.tls_dir / f"{scope.value}-private.key"

    @property
    def jmx_exporter(self) -> pathops.PathProtocol:
        """Main config file."""
        return self.lib_dir / JMX_EXPORTER_AGENT_FILE

    @property
    def jmx_exporter_config(self) -> pathops.PathProtocol:
        """Main config file."""
        return self.config_dir / "jmx_exporter.yaml"


class WorkloadBase(ABC):
    """Base interface for workload operations."""

    substrate: Substrate
    cassandra_paths: CassandraPaths

    @abstractmethod
    def start(self) -> None:
        """Start Cassandra service."""
        pass

    @abstractmethod
    def install(self) -> None:
        """Install Cassandra."""
        pass

    @property
    @abstractmethod
    def installed(self) -> bool:
        """Whether Cassandra is installed."""
        pass

    @abstractmethod
    def is_alive(self) -> bool:
        """Whether Cassandra service running."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop Cassandra service."""
        pass

    @abstractmethod
    def restart(self) -> None:
        """Restart Cassandra service."""
        pass

    @abstractmethod
    def remove_file(self, file: str) -> None:
        """Remove file."""
        pass

    @abstractmethod
    def remove_directory(self, directory: str) -> None:
        """Remove directory recursively."""
        pass

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """Check if file or directory exists."""
        pass

    @abstractmethod
    def exec(
        self,
        command: list[str],
        cwd: str | None = None,
        suppress_error_log: bool = False,
    ) -> tuple[str, str]:
        """Run a command on the workload substrate."""
        pass

    @staticmethod
    def generate_string(length: int = 32, prefix: str = "") -> str:
        """Create randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        alphabet = string.ascii_letters + string.digits
        random_part = "".join(secrets.choice(alphabet) for _ in range(length))
        return prefix + random_part
