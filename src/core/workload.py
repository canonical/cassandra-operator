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
        """Get keystore path for the scope."""
        return self.tls_dir / f"{scope.value}-truststore.jks"

    def get_keystore(self, scope: TLSScope) -> pathops.PathProtocol:
        """Get keystore path for the scope."""
        return self.tls_dir / f"{scope.value}-keystore.p12"

    @property
    def jmx_exporter(self) -> pathops.PathProtocol:
        """Main config file."""
        return self.lib_dir / "jmx_prometheus_javaagent-1.0.0.jar"

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
    def generate_password() -> str:
        """Create randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])
