#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO."""

import json
import logging
from typing import Any, Dict, List, Optional, Union

import requests
import urllib3
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from common.exceptions import (
    HealthCheckFailedError,
)
from common.models import Node

logger = logging.getLogger(__name__)

_CASSANDRA_READY_TIMEOUT = 5
_CASSANDRA_LIVE_TIMEOUT = 5


class ManagementClientHttpError(Exception):
    """Exception thrown when an OpenSearch REST call fails."""

    def __init__(self, response_text: Optional[str] = None, response_code: Optional[int] = None):
        if response_text is None:
            self.response_text = "response is empty"
        else:
            self.response_text = response_text

        try:
            self.response_body = json.loads(self.response_text)
        except (json.JSONDecodeError, TypeError):
            self.response_body = {}
        self.response_code = response_code
        if self.response_body:
            message = f"HTTP error {self.response_code=}\n{self.response_body=}"
        else:
            message = f"HTTP error {self.response_code=}\n{self.response_text=}"
        super().__init__(message)


class ManagementClient:
    """TODO."""

    def __init__(
        self,
        host: str,
    ):
        self.base_url = f"http://{host}:8080/api/v0"

    def request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Union[str, Dict[str, Any], List[Dict[str, Any]]]] = None,
        resp_status_code: bool = False,
        retries: int = 0,
        timeout: int = 5,
    ) -> Union[Dict[str, Any], List[Any], int]:
        """Make an HTTP request.

        Args:
            method: matching the known http methods.
            endpoint: relative to the base uri.
            payload: str, JSON obj or array body payload.
            host: host of the node we wish to make a request on, by default current host.
            resp_status_code: whether to only return the HTTP code from the response.
            retries: number of retries
            timeout: number of seconds before a timeout happens

        Raises:
            ValueError if method or endpoint are missing
            OpenSearchHttpError if hosts are unreachable
        """

        def call(url: str) -> requests.Response:
            """Performs an HTTP request."""
            for attempt in Retrying(
                retry=retry_if_exception_type(requests.RequestException)
                | retry_if_exception_type(urllib3.exceptions.HTTPError),
                stop=stop_after_attempt(retries),
                wait=wait_fixed(1),
                before_sleep=_error_http_retry_log(logger, retries, method, url, payload),
                reraise=True,
            ):
                with attempt, requests.Session() as s:
                    request_kwargs = {
                        "method": method.upper(),
                        "url": url,
                        "headers": {
                            "Accept": "application/json",
                            "Content-Type": "application/json",
                        },
                        "timeout": (timeout, timeout),
                    }
                    if payload:
                        request_kwargs["data"] = (
                            json.dumps(payload) if not isinstance(payload, str) else payload
                        )

                    response = s.request(**request_kwargs)
                    response.raise_for_status()
                    return response

            raise RuntimeError("HTTP request failed after all retries.")

        if None in [endpoint, method]:
            raise ValueError("endpoint or method missing")

        if endpoint.startswith("/"):
            endpoint = endpoint[1:]

        url = f"{self.base_url}/{endpoint}"

        resp = None
        try:
            resp = call(url)
            if resp_status_code:
                return resp.status_code

            return resp.json()
        except (requests.RequestException, urllib3.exceptions.HTTPError) as e:
            if not isinstance(e, requests.RequestException) or e.response is None:
                raise ManagementClientHttpError(response_text=str(e))

            if resp_status_code:
                return e.response.status_code

            raise ManagementClientHttpError(
                response_text=e.response.text, response_code=e.response.status_code
            )
        except Exception as e:
            raise ManagementClientHttpError(response_text=str(e))

    def get_keyspace_list(self) -> List[str]:
        """TODO."""
        url = f"{self.base_url}"
        try:
            response = self.request(method="GET", endpoint="/ops/keyspace", retries=5)

            if isinstance(response, dict) and "keyspaces" in response:
                return response["keyspaces"]
            else:
                logger.warning(f"Unexpected response format: {response}")
                return []
        except ManagementClientHttpError as e:
            logger.error(f"Failed to get keyspace list from {url}: {e}")
            return []

    def is_healthy(self) -> bool:
        """Perform cassandra helth and readiness checks and return True if healthy.

        Returns:
            bool: True if the cluster or node is healthy.
        """
        logger.debug("Running cassandra health check.")
        live_result = self._cassandra_ready()
        if live_result is False:
            raise HealthCheckFailedError("Cassandra is not live")

        ready_result = self._cassandra_ready()

        if ready_result is False:
            raise HealthCheckFailedError("Cassandra is not ready")

        logger.debug("Health check passed.")
        return True

    def _cassandra_ready(self) -> bool:
        endpoint = "/probes/readiness"
        try:
            response = self.request(
                method="GET",
                endpoint=endpoint,
                resp_status_code=True,
                retries=3,
                timeout=_CASSANDRA_READY_TIMEOUT,
            )

            if not isinstance(response, int):
                logger.warning(f"Unexpected response format: {response}")
                return False

            if response == 200:
                return True
            elif 500 <= response < 600:
                logger.warning(f"Cassandra not ready, status: {response}")
                return False
            else:
                logger.warning(f"Unexpected status code from readiness probe: {response}")
                return False
        except ManagementClientHttpError as e:
            logger.error(f"Error checking Cassandra readiness at {endpoint}: {e}")
            return False

    def _cassandra_live(self) -> bool:
        endpoint = "/probes/liveness"
        try:
            response = self.request(
                method="GET",
                endpoint="/probes/liveness",
                resp_status_code=True,
                retries=3,
                timeout=_CASSANDRA_LIVE_TIMEOUT,
            )

            if not isinstance(response, int):
                logger.warning(f"Unexpected response format: {response}")
                return False

            if response == 200:
                return True
            elif 500 <= response < 600:
                logger.warning(f"Cassandra not ready, status: {response}")
                return False
            else:
                logger.warning(f"Unexpected status code from readiness probe: {response}")
                return False
        except ManagementClientHttpError as e:
            logger.error(f"Error checking Cassandra readiness at {endpoint}: {e}")
            return False

    def node_list(self) -> dict[str, Node] | None:
        """TODO."""
        pass


def _error_http_retry_log(
    logger,
    retry_max: int,
    method: str,
    url: str,
    payload: Optional[Union[str, Dict[str, Any], List[Dict[str, Any]]]],
):
    """Return a custom log function to run before a new Tenacity retry."""

    def log_error(retry_state: RetryCallState):
        if retry_state.outcome is None:
            return
        logger.debug(
            f"Request {method} to {url} with payload: {payload} failed."
            f"(Attempts left: {retry_max - retry_state.attempt_number})\n"
            f"\tError: {retry_state.outcome.exception()}"
        )

    return log_error
