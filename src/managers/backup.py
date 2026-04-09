#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Backup manager using medusa."""

import dataclasses
import datetime
import inspect
import logging
import re
from typing import Literal

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
    s3_bucket: str
    s3_endpoint: str
    s3_region: str


class BackupManager:
    """Manager of medusa-driven backup/restores."""

    def __init__(
        self,
        workload: WorkloadBase,
    ):
        self._workload = workload

    def medusa_exec(self, *args: str) -> str:
        """Run a medusa command."""
        stdout, _ = self._workload.exec(
            [
                "medusa",
                "--config-file",
                self._workload.cassandra_paths.medusa_config.as_posix(),
                *args,
            ],
            timeout=3600,
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
        stdout = self.medusa_exec("list-backups")
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
            "s3_bucket": config.s3_bucket,
            "s3_region": config.s3_region,
        }
        self._workload.cassandra_paths.medusa_config.write_text(template.render(data) + "\n")
