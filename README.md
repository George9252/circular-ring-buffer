# circular_ring_buffer

A fixed-capacity in-memory ring buffer for the last N log records, queryable by level and time range. Standard library only.

```python
from circular_ring_buffer import RingBuffer

buf = RingBuffer(1000)
buf.append("INFO", "request started", timestamp=1.0)
buf.append("ERROR", "disk full", timestamp=2.0)
buf.append("WARN", "retry scheduled", timestamp=3.0)

recent_errors = buf.filter(level="ERROR", since=1.5)
for rec in recent_errors:
    print(rec.timestamp, rec.level, rec.message)
```

## Why

When something goes wrong in a long-running process you usually want "the last few hundred log lines", not the whole history. Keeping every log in memory grows without bound; writing everything to disk adds latency you may not want on the hot path. This buffer holds the most recent N records in a `collections.deque(maxlen=N)`, so eviction is O(1) and memory is bounded by the capacity you choose. You query the window you care about after the fact.

The trade-off: records that aged out are gone for good. This is a recent-incident lens, not an audit log.

## Edge cases

- `capacity` must be a positive integer; smaller values raise `ValueError`.
- Time bounds in `filter(since=..., until=...)` are inclusive on both ends. A window with `since > until` returns an empty list rather than raising.
- `append` accepts an explicit `timestamp` to back-fill historical records. If omitted and no clock was injected, the timestamp defaults to `0.0`. Pass a clock callable (e.g. `time.monotonic`) to the constructor if you want automatic timestamps in production.
- Level matching is exact and case-sensitive.

## Exported names

- `RingBuffer(capacity, clock=None)` — the buffer. Methods: `append(level, message, timestamp=None)`, `records()`, `filter(level=None, since=None, until=None)`, `clear()`. Properties: `capacity`. Supports `len()` and iteration.
- `LogRecord(timestamp, level, message)` — the record type returned by `append`, `records()`, `filter()`, and iteration.

## Run the tests

```
PYTHONPATH=src python -m unittest discover -s tests
```

## Performance

The window keeps a bounded buffer, so `push` is constant time and memory does not
grow with the length of the stream. `peak` and `trough` are linear in the window
size, which is the trade that keeps `push` cheap.

