"""In-memory circular buffer of recent log records.

Wraps collections.deque with a maxlen so the oldest record is evicted in O(1)
when capacity is reached. This keeps a bounded view of "what just happened"
for incident triage without growing memory unbounded over the process lifetime.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from typing import Any


class LogRecord:
    """A single log entry stored in the ring buffer.

    Fields are intentionally minimal: a timestamp, a level, and the message
    payload. Keeping this flat avoids hidden coupling to any particular
    logging framework; callers map whatever they have onto these fields.
    """

    __slots__ = ("timestamp", "level", "message")

    def __init__(self, timestamp: float, level: str, message: Any) -> None:
        self.timestamp = timestamp
        self.level = level
        self.message = message

    def __repr__(self) -> str:
        return (
            f"LogRecord(timestamp={self.timestamp!r}, "
            f"level={self.level!r}, message={self.message!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LogRecord):
            return NotImplemented
        return (
            self.timestamp == other.timestamp
            and self.level == other.level
            and self.message == other.message
        )

    def __hash__(self) -> int:
        # message may be an unhashable type (dict, list), so hash on the
        # stable identity fields only. This keeps LogRecord usable as a dict
        # key when callers want deduplication by time+level.
        return hash((self.timestamp, self.level))


class RingBuffer:
    """A fixed-capacity ring buffer of LogRecords.

    Records are stored in insertion order. When the buffer is full, the oldest
    record is evicted automatically by deque(maxlen=...).

    A ``clock`` callable is injected rather than reading time.time() internally
    so that tests can supply deterministic timestamps. Production callers pass
    ``time.monotonic`` or ``time.time`` as appropriate; the buffer treats the
    value opaquely and only compares it.
    """

    def __init__(
        self,
        capacity: int,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._clock = clock
        self._records: deque[LogRecord] = deque(maxlen=capacity)

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Iterator[LogRecord]:
        # Yield a stable snapshot view: iterate over the deque directly. deque
        # iteration is safe against concurrent append on CPython, but callers
        # who need a true snapshot should call records().
        return iter(self._records)

    def append(self, level: str, message: Any, timestamp: float | None = None) -> LogRecord:
        """Append a record and return it.

        If ``timestamp`` is None the injected clock is consulted, or zero is
        used when no clock was provided. Callers may pass an explicit timestamp
        to batch-insert historical records.
        """
        if timestamp is None:
            timestamp = self._clock() if self._clock is not None else 0.0
        record = LogRecord(timestamp, level, message)
        self._records.append(record)
        return record

    def records(self) -> list[LogRecord]:
        """Return a list copy of the current contents in insertion order.

        The list is a snapshot: mutation of the buffer afterwards does not
        affect the returned list, and vice versa.
        """
        return list(self._records)

    def filter(
        self,
        *,
        level: str | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> list[LogRecord]:
        """Return records matching the given predicates, in insertion order.

        - ``level``: exact case-sensitive match against the record's level.
        - ``since``: inclusive lower bound on timestamp.
        - ``until``: inclusive upper bound on timestamp.

        All bounds are inclusive on both ends. Passing ``since`` greater than
        ``until`` simply yields no records rather than raising; this matches
        the intuition of an empty time window.
        """
        out: list[LogRecord] = []
        for r in self._records:
            if level is not None and r.level != level:
                continue
            if since is not None and r.timestamp < since:
                continue
            if until is not None and r.timestamp > until:
                continue
            out.append(r)
        return out

    def clear(self) -> None:
        """Remove all records without changing the capacity."""
        self._records.clear()
