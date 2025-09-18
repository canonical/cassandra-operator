#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Lock manager."""

import logging
from enum import StrEnum

from ops import CharmBase, EventBase, Object
from ops.model import Application, Relation, Unit

logger = logging.getLogger(__name__)


class LockState(StrEnum):
    """Possible states for our Distributed lock.

    Note that there are two states set on the unit, and two on the application.

    """

    ACQUIRE = "acquire"
    RELEASE = "release"
    GRANTED = "granted"
    IDLE = "idle"


class Lock:
    def __init__(self, relation: Relation, app: Application, unit: Unit):
        self.relation = relation
        self.app = app
        self.unit = unit

    @property
    def _state(self) -> LockState:
        """Return an appropriate state.

        Note that the state exists in the unit's relation data, and the application
        relation data, so we have to be careful about what our states mean.

        Unit state can only be in "acquire", "release", "None" (None means unset)
        Application state can only be in "granted" or "None" (None means unset or released)

        """
        unit_state = LockState(self.relation.data[self.unit].get("state", LockState.IDLE.value))
        app_state = LockState(
            self.relation.data[self.app].get(str(self.unit), LockState.IDLE.value)
        )

        if app_state == LockState.GRANTED and unit_state == LockState.RELEASE:
            # Active release request.
            return LockState.RELEASE

        if app_state == LockState.IDLE and unit_state == LockState.ACQUIRE:
            # Active acquire request.
            return LockState.ACQUIRE

        logger.debug("Lock state: %s %s", unit_state, app_state)
        return app_state  # Granted or unset/released

    @_state.setter
    def _state(self, state: LockState):
        """Set the given state.

        Since we update the relation data, this may fire off a RelationChanged event.
        """
        if state == LockState.ACQUIRE:
            self.relation.data[self.unit].update({"state": state.value})

        if state == LockState.RELEASE:
            self.relation.data[self.unit].update({"state": state.value})

        if state == LockState.GRANTED:
            self.relation.data[self.app].update({str(self.unit): state.value})

        if state is LockState.IDLE:
            self.relation.data[self.app].update({str(self.unit): state.value})

        logger.debug("state: %s", state.value)

    def acquire(self):
        """Request that a lock be acquired."""
        self._state = LockState.ACQUIRE
        logger.debug("Lock acquired.")

    def release(self):
        """Request that a lock be released."""
        self._state = LockState.RELEASE
        logger.debug("Lock released.")

    def clear(self):
        """Unset a lock."""
        self._state = LockState.IDLE
        logger.debug("Lock cleared.")

    def grant(self):
        """Grant a lock to a unit."""
        self._state = LockState.GRANTED
        logger.debug("Lock granted.")

    def is_held(self):
        """This unit holds the lock."""
        return self._state == LockState.GRANTED

    def release_requested(self):
        """A unit has reported that they are finished with the lock."""
        return self._state == LockState.RELEASE

    def is_pending(self):
        """Is this unit waiting for a lock?"""
        return self._state == LockState.ACQUIRE


class Locks:
    """Generator that returns a list of locks."""

    def __init__(self, relation: Relation, app: Application, unit: Unit):
        # Gather all the units.
        self.relation = relation
        self.app = app
        self.units = [*relation.units, unit]

    def __iter__(self):
        """Yields a lock for each unit we can find on the relation."""
        for unit in self.units:
            yield Lock(relation=self.relation, app=self.app, unit=unit)


class LockManager(Object):
    def __init__(self, charm: CharmBase, relation: str) -> None:
        super().__init__(charm, key=f"{relation}_lock_manager")
        self.app = charm.app
        self.unit = charm.unit
        self.relation = charm.model.get_relation(relation)
        self.framework.observe(charm.on[relation].relation_changed, self._process_locks)
        self.framework.observe(charm.on[relation].relation_changed, self._process_locks)
        self.framework.observe(charm.on[relation].relation_broken, self._process_locks)

    def _process_locks(self, event: EventBase | None = None) -> None:
        if not self.unit.is_leader():
            return

        pending = []

        for lock in self._locks:
            if lock.is_held():
                # One of our units has the lock -- return without further processing.
                return

            if lock.release_requested():
                lock.clear()  # Updates relation data

            if lock.is_pending():
                if lock.unit == self.unit:
                    # Always run on the leader last.
                    pending.insert(0, lock)
                else:
                    pending.append(lock)

        # If we reach this point, and we have pending units, we want to grant a lock to
        # one of them.
        if pending:
            lock = pending[-1]
            lock.grant()

    @property
    def _locks(self) -> Locks:
        assert self.relation
        return Locks(self.relation, self.app, self.unit)

    @property
    def _lock(self) -> Lock:
        assert self.relation
        return Lock(self.relation, self.app, self.unit)

    def try_lock(self) -> bool:
        if self._lock.is_pending():
            return False
        if self._lock.is_held():
            return True
        self._lock.acquire()
        self._process_locks()
        return self._lock.is_held()

    @property
    def is_pending(self) -> bool:
        return self._lock.is_pending() or self._lock.is_held() or self._lock.release_requested()

    def release(self) -> None:
        self._lock.release()
        self._process_locks()
