"""Xử lý dữ liệu độc lập với kết nối Kafka để có thể kiểm thử."""

import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

EVENT_PARTITIONS = {"click": 0, "view": 1, "purchase": 2}
LEVELS = ("INFO", "ERROR", "DEBUG")
TOPICS = {"network": "network-status", "logs": "app-logs", "events": "user-events"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def event_partitioner(event, available_partitions):
    """Custom partitioner Python; kết quả truyền vào produce(partition=...)."""
    partition = EVENT_PARTITIONS[event["event_type"]]
    if partition not in available_partitions:
        raise ValueError(f"Topic thiếu partition {partition}")
    return partition


def validate(kind, record):
    if not isinstance(record, dict):
        raise ValueError("Thông điệp phải là JSON object")
    fields = {
        "network": ("device_id", "device_type", "status", "timestamp"),
        "logs": ("log_level", "service_name", "message", "timestamp"),
        "events": ("user_id", "event_type", "item_id", "timestamp"),
    }[kind]
    for field in fields:
        if not isinstance(record.get(field), str) or not record[field]:
            raise ValueError(f"Thiếu hoặc sai kiểu trường {field}")
    stamp = datetime.fromisoformat(record["timestamp"])
    if stamp.tzinfo is None:
        raise ValueError("timestamp phải có múi giờ")
    if kind == "network" and record["status"] not in ("Online", "Offline"):
        raise ValueError("status phải là Online hoặc Offline")
    if kind == "logs" and record["log_level"] not in LEVELS:
        raise ValueError("log_level không hợp lệ")
    if kind == "events" and record["event_type"] not in EVENT_PARTITIONS:
        raise ValueError("event_type không hợp lệ")
    return record


def append_json(path, record):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


class NetworkState:
    def __init__(self, path):
        self.path = Path(path)
        self.states = json.loads(self.path.read_text()) if self.path.exists() else {}

    def handle(self, record):
        validate("network", record)
        device = record["device_id"]
        previous = self.states.get(device)
        if previous and datetime.fromisoformat(record["timestamp"]) < datetime.fromisoformat(previous["timestamp"]):
            print(f"Bỏ qua trạng thái cũ của {device}", flush=True)
            return
        self.states[device] = record
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(self.states, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(self.path)
        old = previous["status"] if previous else "Unknown"
        print(f"{device} ({record['device_type']}): {old} -> {record['status']}", flush=True)


class WindowCounter:
    """Cửa sổ processing time; vẫn xuất thống kê khi Kafka không có dữ liệu."""

    def __init__(self, kind, output, interval, clock=time.monotonic):
        self.kind = kind
        self.output = Path(output)
        self.interval = interval
        self.clock = clock
        self.start = clock()
        self.next_report = self.start + interval
        self.counts = Counter({key: 0 for key in (LEVELS if kind == "logs" else EVENT_PARTITIONS)})

    def handle(self, record, partition=None):
        validate(self.kind, record)
        if self.kind == "events":
            expected = event_partitioner(record, (0, 1, 2))
            if partition is not None and partition != expected:
                raise ValueError(f"Sai partition: {partition}, cần {expected}")
        if self.kind == "logs" and record["log_level"] == "ERROR":
            # Sink ưu tiên được ghi ngay, trước thống kê và các log thường trong batch.
            append_json(self.output / "error.log", record)
            print(f"[ƯU TIÊN ERROR] {record['service_name']}: {record['message']}", flush=True)
        self.counts[record["log_level" if self.kind == "logs" else "event_type"]] += 1

    def report(self, partial=False):
        summary = {
            "timestamp": utc_now(), "kind": self.kind,
            "window_seconds": self.interval,
            "elapsed_seconds": round(self.clock() - self.start, 3),
            "partial": partial, "counts": dict(self.counts),
        }
        append_json(self.output / ("log_counts.jsonl" if self.kind == "logs" else "event_counts.log"), summary)
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        self.counts = Counter({key: 0 for key in self.counts})

    def tick(self):
        now = self.clock()
        while now >= self.next_report:
            self.report()
            self.start = self.next_report
            self.next_report += self.interval

    def close(self):
        self.tick()
        if sum(self.counts.values()):
            self.report(partial=True)
