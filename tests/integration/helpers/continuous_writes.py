#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from multiprocessing import Event, Process, synchronize

from cassandra.cluster import ResultSet
from tenacity import Retrying, stop_after_delay, wait_fixed

from integration.helpers.cassandra import connect_cql

logger = logging.getLogger(__name__)


class ContinuousWrites:
    def __init__(self, keyspace_name: str = "test_keyspace", timeout: int = 600) -> None:
        self.keyspace_name = keyspace_name
        self.timeout = timeout
        self.force_timeout = timeout * 2
        self.stop_event = Event()
        self.write_event = Event()
        self.hosts: list[str] | None = None
        self.password: str | None = None
        self.process: Process | None = None

    def start(
        self,
        hosts: list[str] | None = None,
        password: str | None = None,
        replication_factor: int = 1,
    ) -> None:
        assert password
        assert (
            not self.hosts
            and not self.password
            and not self.process
            and not self.stop_event.is_set()
            and not self.write_event.is_set()
        )

        self.hosts = hosts
        self.password = password

        self._init_keyspace(replication_factor)
        self.process = Process(
            target=self._continuous_writes,
            args=(
                self.stop_event,
                self.write_event,
                hosts,
                password,
                self.keyspace_name,
                self.timeout,
            ),
        )
        self.process.start()
        self.write_event.wait(self.force_timeout)

    def stop_and_assert_writes(self, hosts: list[str] | None = None) -> None:
        assert self.process and self.process.is_alive()

        self.assert_new_writes(hosts)

        self.stop_event.set()
        self.process.join(self.force_timeout)
        if self.process.is_alive():
            logger.error("continuous writes process was killed forcefully")
            self.process.terminate()

        self._assert_writes(hosts)

        self._clear_keyspace()
        self.stop_event.clear()
        self.write_event.clear()
        self.hosts = None
        self.password = None
        self.process = None

    def force_stop(self) -> None:
        if self.process and self.process.is_alive():
            self.process.terminate()

    def assert_new_writes(self, hosts: list[str] | None = None) -> None:
        assert self.process and self.process.is_alive()

        position = self._get_max_position(hosts)

        self.write_event.clear()
        self.write_event.wait(self.force_timeout)

        next_position = self._get_max_position(hosts)

        assert next_position > position

        self._assert_writes(hosts)

    def _init_keyspace(self, replication_factor: int) -> None:
        assert self.hosts and self.password

        for attempt in Retrying(
            wait=wait_fixed(10), stop=stop_after_delay(self.timeout), reraise=True
        ):
            with attempt:
                with connect_cql(
                    hosts=self.hosts, password=self.password, timeout=self.timeout
                ) as session:
                    session.execute(
                        f"CREATE KEYSPACE {self.keyspace_name} WITH replication = "
                        f"{{'class': 'SimpleStrategy','replication_factor': {replication_factor}}}"
                    )
                    session.set_keyspace(self.keyspace_name)
                    session.execute("CREATE TABLE test_table(position INT PRIMARY KEY)")

    def _clear_keyspace(self) -> None:
        self._cql_exec(f"DROP KEYSPACE IF EXISTS {self.keyspace_name}")

    def _get_max_position(self, hosts: list[str] | None = None) -> int:
        res = self._cql_exec("SELECT MAX(position) FROM test_table", hosts=hosts)
        assert isinstance(res, ResultSet)
        return res.all()[0][0]

    def _assert_writes(self, hosts: list[str] | None = None) -> None:
        for attempt in Retrying(
            wait=wait_fixed(10), stop=stop_after_delay(self.timeout), reraise=True
        ):
            with attempt:
                res = self._cql_exec(
                    "SELECT min(position), max(position), count(position) FROM test_table",
                    hosts=hosts,
                )
                assert isinstance(res, ResultSet)
                pmin, pmax, pcount = res.all()[0]
                logger.info(f"continuous writes min={pmin}, max={pmax}, pcount={pcount}")
                assert pcount == (1 + pmax - pmin)

    def _cql_exec(self, query: str, hosts: list[str] | None = None) -> ResultSet | None:
        assert self.hosts and self.password

        for attempt in Retrying(
            wait=wait_fixed(10), stop=stop_after_delay(self.timeout), reraise=True
        ):
            with attempt:
                with connect_cql(
                    hosts=hosts or self.hosts,
                    password=self.password,
                    keyspace=self.keyspace_name,
                    timeout=self.timeout,
                ) as session:
                    return session.execute(query)

        return None

    @staticmethod
    def _continuous_writes(
        stop_event: synchronize.Event,
        write_event: synchronize.Event,
        hosts: list[str],
        password: str,
        keyspace_name: str,
        timeout: int,
    ) -> None:
        position: int = 1
        for attempt in Retrying(wait=wait_fixed(10), stop=stop_after_delay(timeout), reraise=True):
            with attempt:
                with connect_cql(
                    hosts=hosts,
                    password=password,
                    keyspace=keyspace_name,
                    timeout=timeout,
                ) as session:
                    while not stop_event.is_set():
                        session.execute(
                            "INSERT INTO test_table (position) VALUES (%s)",
                            (position,),
                            timeout=timeout,
                        )
                        write_event.set()
                        position += 1
                        stop_event.wait(1)
