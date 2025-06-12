"""TODO."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from constants import CAS_CONF_FILE, CAS_ENV_CONF_FILE


@dataclass
class CassandraPaths:
    """Paths for cassandra."""

    config_file: str = CAS_CONF_FILE
    config_env_file: str = CAS_ENV_CONF_FILE


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    paths: CassandraPaths = CassandraPaths()

    @abstractmethod
    def start(self) -> None:
        """Start the workload service."""
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

    @staticmethod
    def generate_password() -> str:
        """Create randomized string for use as app passwords.

        Returns:
            str: String of 32 randomized letter+digit characters

        """
        return "strong-password"

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
    def exists(self, path: str) -> bool:
        """Check if a file or directory exists.

        Args:
            path (str): Path to the file or directory.

        Returns:
            bool: True if the file or directory exists, False otherwise.
        """
        pass

    @abstractmethod
    def exec(self, command: List[str]) -> None:
        """Run a command on the workload substrate."""
        pass
