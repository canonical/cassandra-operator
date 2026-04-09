#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Backup manager using medusa."""

import dataclasses
import datetime
import inspect
import logging
import re
from functools import cached_property
from typing import Literal
from urllib.parse import ParseResult, urlparse

from jinja2 import Environment, FileSystemLoader

from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


BackupMode = Literal["full", "differential"]


@dataclasses.dataclass
class BackupInfo:
    """Data container for backups."""

    name: str
    started: datetime.datetime
    finished: datetime.datetime | None
    state: Literal["finished", "incomplete"]
    mode: BackupMode = "full"

    def as_da096_dict(self, base_repo_url: str) -> dict[str, str | float | None]:
        """Serialize as a DA-096 compatible dict."""
        return {
            "id": self.name,
            "type": self.mode,
            "log-sequence-number": self.name,
            "repository": base_repo_url,
            "start-time": self.started.timestamp(),
            "end-time": self.finished.timestamp() if self.finished else None,
            "reference-backup-id": "NONE",
        }


@dataclasses.dataclass
class MedusaConfig:
    """Data container for medusa config."""

    cql_username: str
    cql_password: str
    nodetool_username: str
    nodetool_password: str
    storage_bucket: str
    storage_endpoint: str
    storage_region: str

    @cached_property
    def parsed_endpoint(self) -> ParseResult:
        """Return the parsed url object."""
        return urlparse(self.storage_endpoint)

    @property
    def host(self) -> str:
        """Return the S3 compatible storage host."""
        return self.parsed_endpoint.netloc

    @property
    def storage_provider(self) -> Literal["s3", "s3_compatible", "gcs"]:
        """Return the storage provider type."""
        if "aws" in self.host:
            return "s3"
        else:
            return "s3_compatible"

    @property
    def secure(self) -> bool:
        """Is the storage using HTTPS?"""
        return True if self.parsed_endpoint.scheme == "https" else False


class BackupManager:
    """Manager of medusa-driven backup/restores."""

    def __init__(self, workload: WorkloadBase, s3_endpoint: str | None = None):
        self._workload = workload
        self._endpoint = s3_endpoint

    def medusa_exec(self, *args: str, timeout: int = 3600) -> str:
        """Run a medusa command."""
        stdout, _ = self._workload.exec(
            [
                "medusa",
                "--config-file",
                self._workload.cassandra_paths.medusa_config.as_posix(),
                *args,
            ],
            timeout=timeout,
        )
        return stdout

    def create_backup(self, mode: BackupMode = "full") -> str:
        """Create a new cluster backup."""
        dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        backup_name = f"managed-{dt}"
        self.medusa_exec(
            "backup-cluster", "--backup-name", backup_name, "--mode", mode, "--enable-md5-checks"
        )
        return backup_name

    def list_backups(self) -> list[BackupInfo]:
        """Run medusa list-backups and parse the results."""
        stdout = self.medusa_exec("list-backups", timeout=60)
        ret = []
        for line in stdout.split("\n"):
            if not line:
                break

            # complete line sample:
            # {backup_id} (started: {ISO DT str}, finished: {ISO DT str})
            # incomplete line sample:
            # {backup_id} (started: {ISO DT str}, finished: Incomplete [2 of 3 nodes finished])
            matches = re.findall(r"^([\S]+) \(started: ([^,]+), finished: ([^)]+)\).*", line)
            if len(matches) != 1 or len(matches[0]) != 3:
                continue

            state = "incomplete" if "incomplete" in matches[0][2].lower() else "finished"
            start_dt = datetime.datetime.fromisoformat(matches[0][1])
            finish_dt = (
                datetime.datetime.fromisoformat(matches[0][2]) if state == "finished" else None
            )

            ret.append(
                BackupInfo(name=matches[0][0], started=start_dt, finished=finish_dt, state=state)
            )

        return ret

    def render_credentials(self, access_key: str, secret_key: str):
        """Write S3 credentials file."""
        credentials = inspect.cleandoc(f"""
            [default]
            aws_access_key_id = {access_key}
            aws_secret_access_key = {secret_key}
        """)

        self._workload.cassandra_paths.s3_credentials.write_text(credentials + "\n")

    def render_medusa_config(self, config: MedusaConfig):
        """Write medusa.ini config."""
        env = Environment(loader=FileSystemLoader("src/templates"))
        template = env.get_template("medusa.ini.j2")
        data = {
            "cql_username": config.cql_username,
            "cql_password": config.cql_password,
            "nodetool_username": config.nodetool_username,
            "nodetool_password": config.nodetool_password,
            "storage_type": config.storage_provider,
            "storage_bucket": config.storage_bucket,
            "storage_region": config.storage_region,
            "storage_host": config.host,
            "storage_secure": config.secure,
        }
        self._workload.cassandra_paths.medusa_config.write_text(template.render(data) + "\n")
