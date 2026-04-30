# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import datetime
from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from managers.backup import BackupManager, MedusaConfig
from workload import CassandraPaths, CassandraWorkload

LIST_BACKUPS = """managed-2026-04-09T06:43:22Z (started: 2026-04-09 06:43:32, finished: Incomplete [2 of 3 nodes finished])
managed-2026-04-09T06:54:36Z (started: 2026-04-09 06:54:45, finished: 2026-04-09 07:08:07)

Incomplete backups found. You can run "medusa status --backup-name <name>" for more details"""


DUMMY_PS_OUTPUT = r"""
UID          PID    PPID  C STIME TTY          TIME CMD
root           1       0  0 Feb23 ?        00:01:14 /usr/lib/systemd/systemd --system --deserialize=49
root         282       1  0 Feb23 ?        00:00:09 /usr/sbin/cron -f -P
message+     283       1  0 Feb23 ?        00:00:11 @dbus-daemon --system --address=systemd: --nofork --nopidfile --systemd-activation --syslog-only
root         293       1  0 Feb23 ?        00:00:07 /usr/lib/systemd/systemd-logind
root         319       1  0 Feb23 ?        00:00:00 /usr/bin/python3 /usr/share/unattended-upgrades/unattended-upgrade-shutdown --wait-for-signal
root      306633       1  0 Mar14 pts/0    00:00:00 /sbin/agetty -o -p -- \u --noclear --keep-baud - 115200,38400,9600 linux
root      708285       0  0 13:17 pts/1    00:00:00 su -l
root      708289       1  0 13:17 ?        00:00:00 /usr/lib/systemd/systemd --user
root      708290  708289  0 13:17 ?        00:00:00 (sd-pam)
root      708301  708285  0 13:17 pts/1    00:00:00 -bash
root      708312  708301  0 13:17 pts/1    00:00:00 ps -eaf
polkitd   986419       1  0 Mar25 ?        00:00:27 /usr/lib/polkit-1/polkitd --no-debug
syslog    986432       1  0 Mar25 ?        00:42:23 /usr/sbin/rsyslogd -n -iNONE
root     1487058       1  0 Mar07 ?        01:20:26 /usr/bin/containerd
root     1487179       1  0 Mar07 ?        02:27:06 /usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock
root     1490530       1  0 Mar07 ?        00:12:57 /usr/bin/containerd-shim-runc-v2 -namespace moby -id 9abb4658922372256ca75f331edcb9ed008f98bf76874326fffff04edfdb15df -address /run/containerd/containerd.sock
root     1490554 1490530  0 Mar07 ?        00:00:00 /usr/bin/dumb-init -- /entrypoint api-server
root     1490620 1487179  0 Mar07 ?        00:00:02 /usr/bin/docker-proxy -proto tcp -host-ip 0.0.0.0 -host-port 8080 -container-ip 172.18.0.2 -container-port 8080 -use-listen-fd
root     1490627 1487179  0 Mar07 ?        00:00:02 /usr/bin/docker-proxy -proto tcp -host-ip :: -host-port 8080 -container-ip 172.18.0.2 -container-port 8080 -use-listen-fd
systemd+ 3406392       1  0 Apr09 ?        00:00:19 /usr/lib/systemd/systemd-resolved
root     3406393       1  0 Apr09 ?        00:00:06 /usr/libexec/udisks2/udisksd
root     3406396       1  0 Apr09 ?        00:00:00 /usr/lib/systemd/systemd-udevd
root     3406403       1  1 Apr09 ?        03:24:01 /usr/lib/systemd/systemd-journald
systemd+ 3406404       1  0 Apr09 ?        00:00:01 /usr/lib/systemd/systemd-networkd
"""


@pytest.fixture(scope="module")
def workload():
    _workload = MagicMock(spec=CassandraWorkload)
    _workload.cassandra_paths = MagicMock(spec=CassandraPaths)
    return _workload


def test_list_backups(workload):
    workload.reset_mock()
    workload.exec.return_value = LIST_BACKUPS, ""
    mgr = BackupManager(workload=workload)

    backups = mgr.list_backups()
    assert len(backups) == 2
    assert {b.state for b in backups} == {"finished", "incomplete"}
    finished_backup = next(b for b in backups if b.state == "finished")
    assert finished_backup.finished
    assert finished_backup.finished > finished_backup.started
    incomplete_backup = next(b for b in backups if b.state == "incomplete")
    assert not incomplete_backup.finished
    assert incomplete_backup.started == datetime.datetime(2026, 4, 9, 6, 43, 32)


def test_medusa_running(workload):
    workload.reset_mock()
    workload.exec.return_value = DUMMY_PS_OUTPUT, ""
    mgr = BackupManager(workload=workload)
    assert not mgr.medusa_running()


class TestRenderConfig:
    @pytest.fixture(scope="class")
    def manager(self, workload) -> BackupManager:
        return BackupManager(workload=workload)

    @pytest.fixture(scope="class")
    def medusa_config(self) -> MedusaConfig:
        return MedusaConfig(
            cql_username="operator",
            cql_password="cqlPass",
            nodetool_username="charmed-operator",
            nodetool_password="nodetoolPass",
            storage_bucket="testbucket",
            storage_endpoint="http://10.10.10.10",
            storage_region="default",
            storage_type="s3",
        )

    @staticmethod
    def rendered_config(workload):
        return workload.cassandra_paths.medusa_config.write_text.call_args[0][0]

    def test_basic_rendering(self, workload, manager, medusa_config):
        workload.reset_mock()
        manager.render_medusa_config(medusa_config)
        rendered_config = self.rendered_config(workload)

        assert "cql_username = operator" in rendered_config
        assert "cql_password = cqlPass" in rendered_config
        assert "nodetool_username = charmed-operator" in rendered_config
        assert "nodetool_password = nodetoolPass" in rendered_config
        assert "secure = False" in rendered_config
        assert "storage_provider = s3_compatible" in rendered_config

    def test_secure_flag_toggled(self, workload, manager, medusa_config):
        workload.reset_mock()
        cfg = replace(medusa_config, storage_endpoint="https://10.10.10.10")
        manager.render_medusa_config(cfg)
        rendered_config = self.rendered_config(workload)
        assert "secure = True" in rendered_config

    def test_detection_of_storage_provider(self, workload, manager, medusa_config):
        workload.reset_mock()
        cfg = replace(medusa_config, storage_endpoint="https://s3.amazonaws.com/")
        manager.render_medusa_config(cfg)
        rendered_config = self.rendered_config(workload)
        assert "storage_provider = s3" in rendered_config
