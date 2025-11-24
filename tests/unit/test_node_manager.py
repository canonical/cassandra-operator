# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


from unittest.mock import MagicMock, call

from managers.node import NODETOOL, NodeManager


def test_remove_bad_nodes():
    status_all_active = """Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load        Tokens  Owns (effective)  Host ID                               Rack 
UN  10.222.10.92   163.81 KiB  16      48.5%             0b731c0a-641f-4533-b8ad-5527ca96d1f5  rack1
UN  10.222.10.254  195.76 KiB  16      48.8%             362fd8fd-169f-4949-bbc4-4a81d9aa92c6  rack1
UN  10.222.10.113  194.71 KiB  16      50.7%             8f6406bd-96f5-43c3-9433-8439773b7495  rack1
UN  10.222.10.5    96.45 KiB   16      52.0%             1a820086-663b-496f-87b7-d6ccf99df077  rack1"""  # noqa: E501 W291

    status_one_active = """Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load        Tokens  Owns (effective)  Host ID                               Rack 
UN  10.222.10.92   210.03 KiB  16      48.5%             0b731c0a-641f-4533-b8ad-5527ca96d1f5  rack1
DN  10.222.10.254  ?           16      48.8%             362fd8fd-169f-4949-bbc4-4a81d9aa92c6  rack1
DN  10.222.10.113  ?           16      50.7%             8f6406bd-96f5-43c3-9433-8439773b7495  rack1
DN  10.222.10.5    ?           16      52.0%             1a820086-663b-496f-87b7-d6ccf99df077  rack1"""  # noqa: E501 W291

    status_one_removed = """Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address        Load        Tokens  Owns (effective)  Host ID                               Rack 
UN  10.222.10.92   210.03 KiB  16      48.5%             0b731c0a-641f-4533-b8ad-5527ca96d1f5  rack1
UN  10.222.10.254  233.69 KiB  16      48.8%             362fd8fd-169f-4949-bbc4-4a81d9aa92c6  rack1
UN  10.222.10.113  246.63 KiB  16      50.7%             8f6406bd-96f5-43c3-9433-8439773b7495  rack1
DN  10.222.10.5    148.26 KiB  16      52.0%             1a820086-663b-496f-87b7-d6ccf99df077  rack1"""  # noqa: E501 W291

    workload = MagicMock(cassandra_paths=MagicMock(env=MagicMock()))
    node_manager = NodeManager(workload=workload)

    workload.exec.return_value = (status_all_active, None)
    node_manager.remove_bad_nodes(good_node_ips=["10.222.10.92", "10.222.10.254"])
    workload.exec.assert_called_once_with([NODETOOL, "status"])
    workload.exec.reset_mock()

    workload.exec.return_value = (status_one_active, None)
    node_manager.remove_bad_nodes(good_node_ips=["10.222.10.92", "10.222.10.254"])
    assert workload.exec.call_count == 3
    workload.exec.assert_has_calls(
        calls=[
            call([NODETOOL, "status"]),
            call([NODETOOL, "removenode", "8f6406bd-96f5-43c3-9433-8439773b7495"]),
            call([NODETOOL, "removenode", "1a820086-663b-496f-87b7-d6ccf99df077"]),
        ],
        any_order=True,
    )
    workload.exec.reset_mock()

    workload.exec.return_value = (status_one_removed, None)
    node_manager.remove_bad_nodes(good_node_ips=["10.222.10.92", "10.222.10.254"])
    assert workload.exec.call_count == 2
    workload.exec.assert_has_calls(
        calls=[
            call([NODETOOL, "status"]),
            call([NODETOOL, "removenode", "1a820086-663b-496f-87b7-d6ccf99df077"]),
        ],
        any_order=True,
    )
    workload.exec.reset_mock()
