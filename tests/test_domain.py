import json
import tempfile
import unittest
from pathlib import Path

from kafka_lab.domain import NetworkState, WindowCounter, event_partitioner, validate


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name)

    def test_network_persists_and_rejects_stale_updates(self):
        state = NetworkState(self.output / "state.json")
        record = {"device_id": "router-01", "device_type": "router", "status": "Online",
                  "timestamp": "2026-10-16T10:00:00+00:00"}
        state.handle(record)
        state.handle(dict(record, status="Offline", timestamp="2026-10-16T10:01:00+00:00"))
        state.handle(record)
        restored = NetworkState(state.path)
        self.assertEqual(restored.states["router-01"]["status"], "Offline")
        self.assertEqual(json.loads(state.path.read_text()), restored.states)

    def test_partition_mapping_and_missing_partition(self):
        for event, expected in (("click", 0), ("view", 1), ("purchase", 2)):
            self.assertEqual(event_partitioner({"event_type": event}, (0, 1, 2)), expected)
        with self.assertRaises(ValueError):
            event_partitioner({"event_type": "purchase"}, (0, 1))

    def test_reports_on_time_and_when_idle(self):
        now = [100.0]
        counter = WindowCounter("logs", self.output, 10, clock=lambda: now[0])
        record = {"log_level": "ERROR", "service_name": "web", "message": "test",
                  "timestamp": "2026-10-16T10:00:00+00:00"}
        counter.handle(record)
        self.assertTrue((self.output / "error.log").exists())
        now[0] = 109.99
        counter.tick()
        self.assertFalse((self.output / "log_counts.jsonl").exists())
        now[0] = 110
        counter.tick()
        now[0] = 120
        counter.tick()
        rows = [json.loads(line) for line in (self.output / "log_counts.jsonl").read_text().splitlines()]
        self.assertEqual(rows[0]["counts"], {"INFO": 0, "ERROR": 1, "DEBUG": 0})
        self.assertEqual(rows[1]["counts"], {"INFO": 0, "ERROR": 0, "DEBUG": 0})
        self.assertFalse(rows[0]["partial"])

    def test_event_window_default_duration_and_partial_flush(self):
        now = [0.0]
        counter = WindowCounter("events", self.output, 60, clock=lambda: now[0])
        record = {"user_id": "u1", "item_id": "i1", "event_type": "purchase",
                  "timestamp": "2026-10-16T10:00:00+00:00"}
        with self.assertRaises(ValueError):
            counter.handle(record, partition=0)
        counter.handle(record, partition=2)
        now[0] = 59.9
        counter.tick()
        self.assertFalse((self.output / "event_counts.log").exists())
        now[0] = 60
        counter.tick()
        counter.handle(record, partition=2)
        counter.close()
        rows = [json.loads(line) for line in (self.output / "event_counts.log").read_text().splitlines()]
        self.assertEqual([row["partial"] for row in rows], [False, True])
        self.assertEqual([row["counts"]["purchase"] for row in rows], [1, 1])

    def test_invalid_schema(self):
        for record in (None, [], {}, {"device_id": 12}):
            with self.subTest(record=record), self.assertRaises(ValueError):
                validate("network", record)
        base = {"device_id": "d1", "device_type": "server", "status": "Online",
                "timestamp": "2026-10-16T10:00:00+00:00"}
        for change in ({"status": "Unknown"}, {"timestamp": "not-a-date"}, {"timestamp": "2026-10-16T10:00:00"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate("network", dict(base, **change))


if __name__ == "__main__":
    unittest.main()
