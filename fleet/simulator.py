"""Pluggable clock and step execution simulation.

No real external services: an attempt just sleeps for its declared duration
through the injected Clock, then reports the declared behavior. A "timeout"
step simulates a hung process by sleeping until its cancel event is fired
(which the engine does at the declared timeout deadline).
"""
from __future__ import annotations

import threading
import time

from .models import AttemptOutcome, StepSpec

# Guard against accidentally enormous virtual sleeps in tests.
_MAX_VIRTUAL_SLEEP_MS = 60 * 60 * 1000


class Clock:
    def now_ms(self) -> int:
        raise NotImplementedError

    def sleep_ms(self, ms: int, cancel_event: threading.Event | None = None) -> bool:
        """Block for ms; return True if elapsed fully, False if cancelled."""
        raise NotImplementedError

    def wait_for_waiters(self, count: int, timeout: float = 2.0) -> None:
        """Block until at least ``count`` threads are sleeping (virtual)."""
        return None

    def wake_all(self) -> None:
        """Wake parked sleepers without advancing time (virtual only)."""
        return None


class WallClock(Clock):
    def now_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000

    def sleep_ms(self, ms: int, cancel_event: threading.Event | None = None) -> bool:
        if ms <= 0:
            return not (cancel_event is not None and cancel_event.is_set())
        deadline = time.monotonic() + ms / 1000.0
        while True:
            if cancel_event is not None and cancel_event.is_set():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            cancel_event.wait(min(0.02, remaining))


class VirtualClock(Clock):
    """Deterministic, manually driven clock for tests."""

    def __init__(self, start_ms: int = 0) -> None:
        self.cond = threading.Condition()
        self.now = start_ms
        self._waiters = 0

    def now_ms(self) -> int:
        with self.cond:
            return self.now

    def wait_for_waiters(self, count: int, timeout: float = 2.0) -> None:
        """Block until at least ``count`` threads are sleeping.

        Makes virtual-clock tests deterministic: advance time only after
        every attempt (and its watchdog) has actually parked on the clock.
        """
        deadline = time.monotonic() + timeout
        with self.cond:
            while self._waiters < count:
                if not self.cond.wait(timeout=max(0.001, deadline - time.monotonic())):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"expected {count} clock waiters, "
                            f"have {self._waiters}")

    def sleep_ms(self, ms: int, cancel_event: threading.Event | None = None) -> bool:
        with self.cond:
            deadline = self.now + min(ms, _MAX_VIRTUAL_SLEEP_MS)
            must_wait = (self.now < deadline and not (
                cancel_event is not None and cancel_event.is_set()))
            if must_wait:
                self._waiters += 1
                self.cond.notify_all()
            try:
                while self.now < deadline:
                    if cancel_event is not None and cancel_event.is_set():
                        return False
                    # Bounded poll so external cancellation (timeout
                    # watchdog, task cancel, unit offline) is noticed even
                    # though it does not notify this condition variable.
                    self.cond.wait(timeout=0.02)
                return not (cancel_event is not None
                            and cancel_event.is_set())
            finally:
                if must_wait:
                    self._waiters -= 1
                    self.cond.notify_all()

    def wake_all(self) -> None:
        """Notify parked sleepers without advancing time.

        Lets an already-cancelled sleep re-check its cancel event promptly
        instead of waiting for the polling interval.
        """
        with self.cond:
            self.cond.notify_all()

    def advance(self, ms: int) -> None:
        if ms < 0:
            raise ValueError("cannot advance clock backwards")
        with self.cond:
            self.now += ms
            self.cond.notify_all()


class Simulator:
    """Executes one attempt of a step according to its declared behavior."""

    def __init__(self) -> None:
        # Per step-key attempt counter, used for "flaky" simulation.
        self._attempt_seen: dict[str, int] = {}

    def run_attempt(self, spec: StepSpec, cancel_event: threading.Event,
                    clock: Clock) -> tuple[AttemptOutcome, str]:
        if spec.behavior == "timeout":
            # Simulated hung unit: block until the engine cancels at deadline.
            clock.sleep_ms(_MAX_VIRTUAL_SLEEP_MS, cancel_event)
            return AttemptOutcome.INTERRUPTED, "timed out, attempt killed"
        completed = clock.sleep_ms(spec.run_ms, cancel_event)
        if not completed:
            return AttemptOutcome.INTERRUPTED, "attempt interrupted"
        if spec.behavior == "succeed":
            return AttemptOutcome.SUCCEEDED, f"ran for {spec.run_ms}ms"
        if spec.behavior in ("fail", "fail_nonretryable"):
            return AttemptOutcome.FAILED, spec.fail_message
        if spec.behavior == "flaky":
            seen = self._attempt_seen.get(spec.key, 0) + 1
            self._attempt_seen[spec.key] = seen
            if seen <= spec.flaky_fail_times:
                return (AttemptOutcome.FAILED,
                        f"{spec.fail_message} (flaky attempt {seen})")
            return AttemptOutcome.SUCCEEDED, (
                f"succeeded on attempt {seen} after {spec.flaky_fail_times} "
                f"failure(s)")
        return AttemptOutcome.FAILED, f"unknown behavior {spec.behavior!r}"
