#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Exceptions."""


class ExecError(Exception):
    """Error during executing command in workload."""

    def __init__(self, stdout: str, stderr: str) -> None:
        super().__init__("Error during command execution")
        self.stdout = stdout
        self.stderr = stderr

    stdout: str
    stderr: str


class BadSecretError(Exception):
    """Error during user-defined secret validation."""

    def __init__(self) -> None:
        super().__init__("User-defined secret validation error")
