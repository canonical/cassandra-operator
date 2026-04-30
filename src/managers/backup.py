#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Backup manager using medusa."""

import dataclasses
import datetime
import inspect
import json
import logging
import re
from typing import Literal
from urllib.parse import ParseResult, urlparse

from jinja2 import Environment, FileSystemLoader

from core.state import StorageClientContext
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

    def as_dict(self, base_repo_url: str) -> dict[str, str | float | None]:
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
    storage_type: Literal["s3", "azure", "gcs"]
    storage_path: str | None = None

    @property
    def parsed_endpoint(self) -> ParseResult:
        """Return the parsed url object."""
        return urlparse(self.storage_endpoint)

    @property
    def host(self) -> str:
        """Return the S3 compatible storage host."""
        return self.parsed_endpoint.netloc

    @property
    def storage_provider(self) -> Literal["s3", "s3_compatible", "google_storage", "azure_blobs"]:
        """Return the medusa string repr. of storage provider type."""
        # https://github.com/thelastpickle/cassandra-medusa/blob/c5517e6c5d34f2aeac7b25eab19bd15034d2879d/medusa/storage/__init__.py#84
        if self.storage_type == "gcs":
            return "google_storage"
        elif self.storage_type == "azure":
            return "azure_blobs"

        if "aws" in self.host:
            return "s3"
        else:
            return "s3_compatible"

    @property
    def secure(self) -> bool:
        """Is the storage using HTTPS?"""
        return self.parsed_endpoint.scheme == "https"


class BackupManager:
    """Manager of medusa-driven backup/restores."""

    def __init__(self, workload: WorkloadBase):
        self._workload = workload

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

    def medusa_running(self) -> bool:
        """Is a medusa process running?"""
        raw, _ = self._workload.exec(["ps", "-eaf"])
        processes = [line.strip() for line in raw.split("\n") if line.strip()]
        medusa_processes = [p for p in processes if "medusa" in p]
        return bool(medusa_processes)

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
        stdout = self.medusa_exec("list-backups", "--show-all", timeout=60)
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

    def restore(self, backup_name: str) -> None:
        """Restore a backup."""
        default_args = ["--keep-auth", "-y", "--verify"]
        if self.medusa_running():
            logger.info("Backup/restore process is running. Waiting...")
            return

        self.medusa_exec("restore-cluster", "--backup-name", backup_name, *default_args)

    def render_credentials(self, context: StorageClientContext) -> None:
        """Write storage credentials file."""
        if context.type == "s3":
            return self._render_s3_credentials(context.access_key, context.secret_key)
        elif context.type == "azure":
            return self._render_azure_credentials(context.storage_account, context.secret_key)
        else:
            self._workload.cassandra_paths.storage_credentials.write_text(
                context.secret_key + "\n"
            )

    def render_medusa_config(self, config: MedusaConfig) -> None:
        """Write medusa.ini config."""
        env = Environment(loader=FileSystemLoader("src/templates"))
        template = env.get_template("medusa.ini.j2")
        data = {
            "cql_username": config.cql_username,
            "cql_password": config.cql_password,
            "nodetool_username": config.nodetool_username,
            "nodetool_password": config.nodetool_password,
            "storage_provider": config.storage_provider,
            "storage_bucket": config.storage_bucket,
            "storage_region": config.storage_region,
            "storage_host": config.host,
            "storage_secure": config.secure,
            "storage_path": config.storage_path,
        }
        self._workload.cassandra_paths.medusa_config.write_text(template.render(data) + "\n")

    def _render_s3_credentials(self, access_key: str, secret_key: str) -> None:
        """Write S3 credentials file."""
        credentials = inspect.cleandoc(f"""
            [default]
            aws_access_key_id = {access_key}
            aws_secret_access_key = {secret_key}
        """)

        self._workload.cassandra_paths.storage_credentials.write_text(credentials + "\n")

    def _render_azure_credentials(self, storage_account: str, secret_key: str) -> None:
        """Write Azure storage credentials file."""
        credentials = {
            "storage_account": storage_account,
            "key": secret_key,
        }

        self._workload.cassandra_paths.storage_credentials.write_text(
            json.dumps(credentials) + "\n"
        )
