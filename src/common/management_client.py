#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import logging
from typing import Literal

import requests
from requests import ConnectionError, HTTPError

_TIMEOUT = 5
DEFAULT_MGMT_URL = "http://127.0.0.1:8080/api/v0"

logger = logging.getLogger(__name__)


class ManagementClient:
    """TODO."""

    def __init__(
        self,
        base_url: str = DEFAULT_MGMT_URL,
    ):
        self.base_url = base_url

    def _request(self, method: Literal["GET"], endpoint: str) -> str:
        response = requests.request(
            method=method, url=f"{self.base_url}{endpoint}", timeout=_TIMEOUT
        )
        response.raise_for_status()
        return response.content.decode()

    def is_healthy(self) -> bool:
        """Perform cassandra helth and readiness checks and return True if healthy.

        Returns:
            bool: True if the cluster or node is healthy.
        """
        try:
            self._request("GET", "/probes/liveness")
            self._request("GET", "/probes/readiness")
        except (HTTPError, ConnectionError):
            return False

        return True
