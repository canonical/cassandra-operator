#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""SSH manager."""

import logging
import typing

from charmlibs import pathops

from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class SSHManager:
    """Manager of SSH on units."""

    base_path: str = "/root/.ssh"
    key_prefix = "charmed-"

    def __init__(
        self, workload: WorkloadBase, key_type: typing.Literal["rsa", "ed25519"] = "ed25519"
    ):
        self._workload = workload
        self._key_type = key_type
        self._authorized_path = pathops.LocalPath(f"{self.base_path}/authorized_keys")

        self.keygen()

    @property
    def authorized_keys(self) -> set[str]:
        """Return set of authorized keys."""
        if not self._authorized_path.exists():
            return set()

        return {
            line.strip() for line in self._authorized_path.read_text().split("\n") if line.strip()
        }

    @property
    def key_name(self) -> str:
        """Return key name."""
        return f"{self.key_prefix}id_{self._key_type}"

    @property
    def public_key(self) -> str | None:
        """Return the generated public key."""
        path = pathops.LocalPath(f"{self.base_path}/{self.key_name}.pub")
        if not path.exists():
            return None

        return path.read_text().strip()

    def ensure_authorized(self, public_keys: set[str]) -> None:
        """Ensure ONLY the given set of public keys are authorized on this unit.

        This operation is idempotent.
        """
        current = self.authorized_keys
        if public_keys == current:
            return

        self._authorized_path.write_text("\n".join(public_keys) + "\n")

    def keygen(self, renew: bool = False, keysize: int = 4096) -> str:
        """Generate SSH keypair.

        Args:
            renew (bool, optional): _description_. Defaults to False.
            keysize (int, optional): _description_. Defaults to 4096.
        """
        if not renew and self.public_key:
            return self.public_key

        cmd = ["ssh-keygen", "-t", self._key_type]
        if self._key_type == "rsa":
            cmd += ["-b", str(keysize)]
        cmd += ["-f", f"{self.base_path}/{self.key_name}"]
        cmd += ["-N", ""]  # no password
        cmd += ["-q"]
        self._workload.exec(cmd)

        if not self.public_key:
            raise RuntimeError("SSH Key generation failed.")

        return self.public_key
