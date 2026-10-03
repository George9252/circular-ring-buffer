import unittest

from circular_ring_buffer import RingBuffer, LogRecord


class FakeClock:
    """Deterministic clock: returns a value that only changes when advanced."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, by: float) -> None:
        self.now += by


class TestConstruction(unittest.TestCase):
    def test_default_capacity_and_empty(self):
        buf = RingBuffer(5)
        self.assertEqual(buf.capacity, 5)
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.records(), [])

    def test_zero_capacity_rejected(self):
        with self.assertRaises(ValueError):
            RingBuffer(0)

    def test_negative_capacity_rejected(self):
        with self.assertRaises(ValueError):
            RingBuffer(-3)


class TestAppend(unittest.TestCase):
    def test_append_uses_clock_when_no_timestamp(self):
        clock = FakeClock(100.0)
        buf = RingBuffer(3, clock=clock)
        rec = buf.append("INFO", "hello")
        self.assertEqual(rec.timestamp, 100.0)
        self.assertEqual(rec.level, "INFO")
        self.assertEqual(rec.message, "hello")

    def test_append_without_clock_uses_zero(self):
        buf = RingBuffer(3)
        rec = buf.append("WARN", "no clock")
        self.assertEqual(rec.timestamp, 0.0)

    def test_append_with_explicit_timestamp_overrides_clock(self):
        clock = FakeClock(100.0)
        buf = RingBuffer(3, clock=clock)
        rec = buf.append("ERROR", "historical", timestamp=42.0)
        self.assertEqual(rec.timestamp, 42.0)

    def test_eviction_drops_oldest_when_full(self):
        buf = RingBuffer(2)
        a = buf.append("INFO", "a", timestamp=1.0)
        b = buf.append("INFO", "b", timestamp=2.0)
        c = buf.append("INFO", "c", timestamp=3.0)
        self.assertEqual(len(buf), 2)
        self.assertEqual(buf.records(), [b, c])
        self.assertNotIn(a, buf.records())

    def test_message_can_be_arbitrary_object(self):
        buf = RingBuffer(3)
        payload = {"key": "value", "n": 1}
        rec = buf.append("DEBUG", payload, timestamp=1.0)
        self.assertIs(rec.message, payload)


class TestFilter(unittest.TestCase):
    def setUp(self):
        self.buf = RingBuffer(10)
        # Insertion order: timestamps ascend but levels interleave, which is
        # what a real debug log looks like.
        self.buf.append("INFO", "m1", timestamp=1.0)
        self.buf.append("WARN", "m2", timestamp=2.0)
        self.buf.append("ERROR", "m3", timestamp=3.0)
        self.buf.append("INFO", "m4", timestamp=4.0)
        self.buf.append("WARN", "m5", timestamp=5.0)

    def test_filter_by_level(self):
        result = self.buf.filter(level="WARN")
        self.assertEqual([r.message for r in result], ["m2", "m5"])

    def test_filter_by_level_no_match(self):
        result = self.buf.filter(level="FATAL")
        self.assertEqual(result, [])

    def test_filter_by_time_range_inclusive(self):
        result = self.buf.filter(since=2.0, until=4.0)
        self.assertEqual([r.message for r in result], ["m2", "m3", "m4"])

    def test_filter_since_only_inclusive(self):
        result = self.buf.filter(since=3.0)
        self.assertEqual([r.message for r in result], ["m3", "m4", "m5"])

    def test_filter_until_only_inclusive(self):
        result = self.buf.filter(until=3.0)
        self.assertEqual([r.message for r in result], ["m1", "m2", "m3"])

    def test_filter_level_and_time(self):
        result = self.buf.filter(level="INFO", since=3.0)
        self.assertEqual([r.message for r in result], ["m4"])

    def test_filter_inverted_range_yields_empty(self):
        # since > until is treated as an empty window, not an error.
        result = self.buf.filter(since=5.0, until=1.0)
        self.assertEqual(result, [])

    def test_filter_boundary_exact_match(self):
        result = self.buf.filter(since=3.0, until=3.0)
        self.assertEqual([r.message for r in result], ["m3"])


class TestClear(unittest.TestCase):
    def test_clear_empties_but_keeps_capacity(self):
        buf = RingBuffer(3)
        buf.append("INFO", "a", timestamp=1.0)
        buf.append("INFO", "b", timestamp=2.0)
        buf.clear()
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.records(), [])
        self.assertEqual(buf.capacity, 3)
        # Buffer must still accept new records after clear.
        rec = buf.append("INFO", "after", timestamp=3.0)
        self.assertEqual(buf.records(), [rec])


class TestIteration(unittest.TestCase):
    def test_iter_yields_insertion_order(self):
        buf = RingBuffer(5)
        buf.append("INFO", "a", timestamp=1.0)
        buf.append("INFO", "b", timestamp=2.0)
        msgs = [r.message for r in buf]
        self.assertEqual(msgs, ["a", "b"])


class TestLogRecord(unittest.TestCase):
    def test_equality_compares_all_fields(self):
        a = LogRecord(1.0, "INFO", "x")
        b = LogRecord(1.0, "INFO", "x")
        c = LogRecord(1.0, "INFO", "y")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_equality_rejects_other_types(self):
        a = LogRecord(1.0, "INFO", "x")
        self.assertNotEqual(a, "not a record")
        self.assertNotEqual(a, 42)

    def test_repr_contains_fields(self):
        a = LogRecord(1.0, "INFO", "x")
        r = repr(a)
        self.assertIn("LogRecord", r)
        self.assertIn("INFO", r)

    def test_hash_stable_for_same_timestamp_and_level(self):
        a = LogRecord(1.0, "INFO", ["mutable"])
        b = LogRecord(1.0, "INFO", ["different"])
        # Hash only depends on timestamp and level so that unhashable messages
        # do not break use as a dict key.
        self.assertEqual(hash(a), hash(b))


if __name__ == "__main__":
    unittest.main()
