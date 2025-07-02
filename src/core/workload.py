#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from abc import ABC, abstractmethod
from typing import Literal

from charmlibs import pathops

from core.state import TLSScope

Substrate = Literal["vm", "k8s"]


class CassandraPaths:
    """TODO."""

    env: pathops.PathProtocol
    config_dir: pathops.PathProtocol
    data_dir: pathops.PathProtocol

    @property
    def config(self) -> pathops.PathProtocol:
        """TODO."""
        return self.config_dir / "cassandra.yaml"

    @property
    def tls_directory(self) -> pathops.PathProtocol:
        """TODO."""
        return self.config_dir / "tls"

    @property
    def peer_truststore(self) -> pathops.PathProtocol:
        """TODO."""
        return self.tls_directory / f"{TLSScope.PEER.value}-truststore.jks"    

    @property
    def peer_keystore(self) -> pathops.PathProtocol:
        """TODO."""
        return self.tls_directory / f"{TLSScope.PEER.value}-keystore.p12"    

    @property
    def client_truststore(self) -> pathops.PathProtocol:
        """TODO."""
        return self.tls_directory / f"{TLSScope.CLIENT.value}-truststore.jks"    

    @property
    def client_keystore(self) -> pathops.PathProtocol:
        """TODO."""
        return self.tls_directory / f"{TLSScope.CLIENT.value}-keystore.p12"    

    
    @property
    def commitlog_directory(self) -> pathops.PathProtocol:
        """TODO."""
        return self.data_dir / "commitlog"

    @property
    def data_file_directory(self) -> pathops.PathProtocol:
        """TODO."""
        return self.data_dir / "data"

    @property
    def hints_directory(self) -> pathops.PathProtocol:
        """TODO."""
        return self.data_dir / "hints"

    @property
    def saved_caches_directory(self) -> pathops.PathProtocol:
        """TODO."""
        return self.data_dir / "saved_caches"


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    substrate: Substrate
    cassandra_paths: CassandraPaths

    @abstractmethod
    def start(self) -> None:
        """Start the workload service."""
        pass

    @abstractmethod
    def install(self) -> None:
        """Install the cassandra snap."""
        pass

    @abstractmethod
    def alive(self) -> bool:
        """Check if the workload is running.

        Returns:
            bool: True if the workload is running, False otherwise.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the workload service."""
        pass

    @abstractmethod
    def restart(self) -> None:
        """Restart the workload service."""
        pass

    @abstractmethod
    def remove_file(self, file: str) -> None:
        """Remove a file.

        Args:
            file (str): Path to the file.
        """
        pass

    @abstractmethod
    def remove_directory(self, directory: str) -> None:
        """Remove a directory.

        Args:
            directory (str): Path to the directory.
        """
        pass

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """Check if a file or directory exists.

        Args:
            path (str): Path to the file or directory.

        Returns:
            bool: True if the file or directory exists, False otherwise.
        """
        pass

    @abstractmethod
    def exec(
        self, command: list[str], cwd: str | None = None) -> tuple[str, str]:
        """Run a command on the workload substrate."""
        pass
