#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from multiprocessing import Event, Process, synchronize

import jubilant
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
        self.juju: jubilant.Juju | None = None
        self.app_name: str | None = None

    def start(self, juju: jubilant.Juju, app_name: str, replication_factor: int = 1) -> None:
        assert not self.juju

        self.juju = juju
        self.app_name = app_name
        self._init_keyspace(replication_factor)
        self.process = Process(
            target=self._continuous_writes,
            args=(
                self.stop_event,
                self.write_event,
                juju,
                app_name,
                self.keyspace_name,
                self.timeout,
            ),
        )
        self.process.start()
        self.write_event.wait(self.force_timeout)

    def stop_and_assert_writes(self) -> None:
        assert self.juju and self.app_name and self.process and self.process.is_alive()

        self.write_event.clear()
        self.write_event.wait(self.force_timeout)

        self.stop_event.set()
        self.process.join(self.force_timeout)
        if self.process.is_alive():
            logger.error("continuous writes process was killed forcefully")
            self.process.terminate()

        self._assert_writes()

        self._clear_keyspace()
        self.stop_event.clear()
        self.write_event.clear()
        self.juju = None
        self.app_name = None

    def _init_keyspace(self, replication_factor: int) -> None:
        assert self.juju and self.app_name
        with connect_cql(juju=self.juju, app_name=self.app_name, timeout=self.timeout) as session:
            session.execute(
                f"CREATE KEYSPACE {self.keyspace_name} WITH replication = "
                f"{{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}"
            )
            session.set_keyspace(self.keyspace_name)
            session.execute("CREATE TABLE test_table(position INT PRIMARY KEY)")

    def _clear_keyspace(self) -> None:
        assert self.juju and self.app_name
        with connect_cql(juju=self.juju, app_name=self.app_name, timeout=self.timeout) as session:
            session.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace_name}")

    def _assert_writes(self) -> None:
        assert self.juju and self.app_name
        with connect_cql(
            juju=self.juju,
            app_name=self.app_name,
            keyspace=self.keyspace_name,
            timeout=self.timeout,
        ) as session:
            res = session.execute("SELECT * FROM test_table")
            assert isinstance(res, ResultSet)
            positions = [row.position for row in res.all()]
            assert positions
            for position in range(min(positions), max(positions)):
                assert position in positions

    @staticmethod
    def _continuous_writes(
        stop_event: synchronize.Event,
        write_event: synchronize.Event,
        juju: jubilant.Juju,
        app_name: str,
        keyspace_name: str,
        timeout: int,
    ) -> None:
        position: int = 1
        for attempt in Retrying(wait=wait_fixed(10), stop=stop_after_delay(timeout), reraise=True):
            with attempt:
                with connect_cql(
                    juju=juju, app_name=app_name, keyspace=keyspace_name, timeout=timeout
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
