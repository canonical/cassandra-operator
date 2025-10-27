#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import subprocess

import jubilant


def kill_unit(juju: jubilant.Juju, unit_name: str) -> None:
    subprocess.check_call(
        [
            "lxc",
            "stop",
            juju.ssh(unit_name, "hostname").strip(),
            "--force",
        ]
    )


def make_unit_checker(
    app_name: str,
    unit_name: str,
    machine_id: str,
    workload: str | None = None,
    machine: str | None = None,
):
    def check(status: jubilant.Status) -> bool:
        unit = status.apps[app_name].units[unit_name]
        m = status.machines[machine_id]
        return all(
            [
                workload is None or unit.workload_status.current == workload,
                machine is None or m.juju_status.current == machine,
            ]
        )

    return check
