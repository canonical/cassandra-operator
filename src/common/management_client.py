#!/usr/bin/env python3
"""TODO."""

import logging
from typing import List

import requests
import tenacity

from common.exceptions import (
    HealthCheckFailedError,
)

logger = logging.getLogger(__name__)


class ManagementClient:
    """TODO."""

    def __init__(
        self,
        host: str,
    ):
        self.base_url = f"http://{host}:8080/api/v0"

    def get_keyspace_list(self) -> List[str]:
        """TODO."""
        url = f"{self.base_url}/ops/keyspace"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            if "keyspaces" in data and isinstance(data["keyspaces"], list):
                return data
            else:
                logger.warning(f"Unexpected response format: {data}")
                return []
        except requests.RequestException as e:
            logger.error(f"Failed to get keyspace list from {url}: {e}")
            return []

    def is_healthy(self) -> bool:
        """Perform cassandra helth and readiness checks and return True if healthy.

        Returns:
            bool: True if the cluster or node is healthy.
        """
        logger.debug("Running cassandra health check.")
        try:
            for attempt in tenacity.Retrying(
                stop=tenacity.stop_after_attempt(5),
                wait=tenacity.wait_fixed(5),
                reraise=True,
            ):
                with attempt:
                    logger.debug(f"Checking health attempt: {attempt.retry_state.attempt_number}")
                    live_result = self._cassandra_live()

                    if live_result is False:
                        raise HealthCheckFailedError("Cassandra is not live")

                    ready_result = self._cassandra_ready()

                    if ready_result is False:
                        raise HealthCheckFailedError("Cassandra is not ready")

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
        logger.debug("Health check passed.")
        return True

    def _cassandra_ready(self) -> bool:
        url = f"{self.base_url}/probes/readiness"
        try:
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                return True
            elif 500 <= response.status_code < 600:
                logger.warning(f"Cassandra not ready, status: {response.status_code}")
                return False
            else:
                logger.warning(
                    f"Unexpected status code from readiness probe: {response.status_code}"
                )
                return False
        except requests.RequestException as e:
            logger.error(f"Error checking Cassandra readiness at {url}: {e}")
            return False

    def _cassandra_live(self) -> bool:
        url = f"{self.base_url}/probes/liveness"
        try:
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                return True
            elif 500 <= response.status_code < 600:
                logger.warning(f"Cassandra not ready, status: {response.status_code}")
                return False
            else:
                logger.warning(
                    f"Unexpected status code from readiness probe: {response.status_code}"
                )
                return False
        except requests.RequestException as e:
            logger.error(f"Error checking Cassandra readiness at {url}: {e}")
            return False

    def broadcast_peer_url(self, peer_urls: str) -> None:
        """TODO: change configs by adding seed_nodes across all nodes!

        Args:
            peer_urls (str): The peer URLs to broadcast.
        """
        pass
