#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling configuration building + writing."""

import logging
import re
from pathlib import Path

import yaml
from pydantic import ValidationError

from common.literals import CAS_CONF_FILE, CAS_ENV_CONF_FILE, MGMT_API_DIR, CharmConfig
from common.workload import WorkloadBase
from core.cluster import ApplicationState

logger = logging.getLogger(__name__)


class ConfigManager:
    """Handle the configuration of Cassandra."""

    def __init__(
        self,
        state: ApplicationState,
        workload: WorkloadBase,
        config: CharmConfig,
    ):
        self.state = state
        self.workload = workload
        self.config = config

    def set_config_properties(self) -> None:
        """Write the config properties to the config files."""
        logger.debug("Writing configuration")

        try:
            self.config
        except ValidationError as e:
            logger.debug(f"Config haven't passed validation: {e}")
            raise e

        self._render_cassandra_env_config(
            max_heap_size_mb=1024 if self.config.profile == "testing" else None,
            enable_mgmt_server=True,
        )
        self._set_cassandra_config()

    def _set_cassandra_config(self) -> None:
        with open(CAS_CONF_FILE) as config:
            # load the config properties provided from the template in this repo
            # it does NOT load the template from disk in the charm unit
            # this is in order to avoid config drift
            config_properties = yaml.safe_load(config)

        if not isinstance(config_properties, dict):
            raise ValueError("Current cassandra config file is not valid")

        if self.config["cluster_name"] is not None:
            config_properties.update({"cluster_name": self.config["cluster_name"]})

        self.workload.write_file(
            yaml.dump(config_properties, allow_unicode=True, default_flow_style=False),
            CAS_CONF_FILE,
        )

    def _render_cassandra_env_config(
        self, max_heap_size_mb: int | None, enable_mgmt_server: bool = True
    ) -> None:
        content = self.workload.read_file(CAS_ENV_CONF_FILE)

        content, _ = re.subn(
            pattern=r'^\s*#?MAX_HEAP_SIZE="[^"]*"$',
            repl=f'MAX_HEAP_SIZE="{max_heap_size_mb}M"'
            if max_heap_size_mb
            else '#MAX_HEAP_SIZE=""',
            string=content,
            count=1,
            flags=re.MULTILINE,
        )

        content, _ = re.subn(
            pattern=r'^\s*#?HEAP_NEWSIZE="[^"]*"$',
            repl=f'HEAP_NEWSIZE="{max_heap_size_mb // 2}M"'
            if max_heap_size_mb
            else '#HEAP_NEWSIZE=""',
            string=content,
            count=1,
            flags=re.MULTILINE,
        )

        if enable_mgmt_server:
            mgmtapi_agent_line = (
                f'JVM_OPTS="$JVM_OPTS -javaagent:{MGMT_API_DIR}/libs/datastax-mgmtapi-agent.jar"'
            )
            if mgmtapi_agent_line not in content:
                if not content.endswith("\n"):
                    content += "\n"
                content += mgmtapi_agent_line + "\n"

        self.workload.write_file(content, CAS_ENV_CONF_FILE)
