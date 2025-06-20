#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

from abc import ABC, abstractmethod

SNAP_VAR_CURRENT_PATH = "/var/snap/charmed-cassandra/current"
SNAP_CURRENT_PATH = "/snap/charmed-cassandra/current"

SNAP_CONF_PATH = f"{SNAP_VAR_CURRENT_PATH}/etc"

CAS_CONF_PATH = f"{SNAP_CONF_PATH}/cassandra"

CAS_CONF_FILE = f"{CAS_CONF_PATH}/cassandra.yaml"
CAS_ENV_CONF_FILE = f"{CAS_CONF_PATH}/cassandra-env.sh"

MGMT_API_DIR = f"{SNAP_CURRENT_PATH}/opt/mgmt-api"


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

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
    def write_file(self, content: str, file: str) -> None:
        """Write content to a file.

        Args:
            content (str): Content to write to the file.
            file (str): Path to the file.
        """
        pass

    @abstractmethod
    def read_file(self, file: str) -> str:
        """Read content from file.

        Args:
            file (str): Path to the file.
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
    def exec(self, command: list[str]) -> tuple[str, str]:
        """Run a command on the workload substrate."""
        pass
