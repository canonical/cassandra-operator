#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling configuration building + writing."""

import logging
import re

import yaml

from common.literals import CAS_CONF_FILE, CAS_ENV_CONF_FILE, MGMT_API_DIR
from common.workload import WorkloadBase

logger = logging.getLogger(__name__)


class ConfigManager:
    """Handle the configuration of Cassandra."""

    def __init__(
        self,
        workload: WorkloadBase,
    ):
        self.workload = workload

    def render_cassandra_config(self, cluster_name: str) -> None:
        """TODO."""
        config_properties = yaml.safe_load(self.workload.read_file(CAS_CONF_FILE))

        if not isinstance(config_properties, dict):
            raise ValueError("Current cassandra config file is not valid")

        config_properties.update({"cluster_name": cluster_name})

        self.workload.write_file(
            yaml.dump(config_properties, allow_unicode=True, default_flow_style=False),
            CAS_CONF_FILE,
        )

    def render_cassandra_env_config(self, max_heap_size_mb: int | None) -> None:
        """TODO."""
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

        mgmtapi_agent_line = (
            f'JVM_OPTS="$JVM_OPTS -javaagent:{MGMT_API_DIR}/libs/datastax-mgmtapi-agent.jar"'
        )
        if mgmtapi_agent_line not in content:
            content += f"\n{mgmtapi_agent_line}\n"

        self.workload.write_file(content, CAS_ENV_CONF_FILE)
