#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Exceptions."""


class ExecError(Exception):
    """Error during executing command in workload."""

    def __init__(self, stdout: str, stderr: str) -> None:
        self.stdout = stdout
        self.stderr = stderr

    stdout: str
    stderr: str
