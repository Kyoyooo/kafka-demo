"""Kiểm thử với broker THẬT; chạy từ thư mục gốc bằng .venv/bin/python.

Giữ nguyên cửa sổ 10s và 60s của đề. Mỗi lần chạy dùng topic/group riêng.
"""

import json
import platform
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition

from kafka_lab.domain import EVENT_PARTITIONS, TOPICS
from kafka_lab.topics import create_topics


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def inspect_records(topic, expected, kind):
    client = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": f"inspect-{uuid.uuid4().hex}",
                       "enable.auto.commit": False})
    records = []
    try:
        client.assign([TopicPartition(topic, p, confluent_kafka.OFFSET_BEGINNING) for p in range(3)])
        deadline = time.monotonic() + 15
        while len(records) < expected and time.monotonic() < deadline:
            msg = client.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            record = json.loads(msg.value())
            if kind == "events":
                assert msg.partition() == EVENT_PARTITIONS[record["event_type"]]
            else:
                assert dict(msg.headers())["log_level"].decode() == record["log_level"]
            records.append(record)
        assert len(records) == expected, (topic, len(records))
    finally:
        client.close()


def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    output = ROOT / "results" / run_id
    output.mkdir(parents=True)
    suffix = "-test-" + run_id.lower()
    create_topics(suffix=suffix)
    processes = []
    handles = []
    try:
        for kind, duration in (("network", 12), ("logs", 23), ("events", 65)):
            handle = (output / f"{kind}-consumer.txt").open("w")
            handles.append(handle)
            process = subprocess.Popen([
                sys.executable, "-m", "kafka_lab.consumer", kind,
                "--topic", TOPICS[kind] + suffix, "--group", f"{kind}-{run_id}",
                "--output-dir", str(output / kind), "--duration", str(duration),
            ], cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT)
            processes.append(process)
        for kind, count in (("network", 6), ("logs", 9), ("events", 9)):
            with (output / f"{kind}-producer.txt").open("w") as handle:
                subprocess.run([
                    sys.executable, "-m", "kafka_lab.producer", kind,
                    "--topic", TOPICS[kind] + suffix, "--count", str(count), "--interval", "0.05",
                ], cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT, check=True, timeout=30)
        # Xem trực tiếp dữ liệu Kafka, xác minh partition và metadata header.
        inspect_records(TOPICS["events"] + suffix, 9, "events")
        inspect_records(TOPICS["logs"] + suffix, 9, "logs")
        # Consumer phải cách ly dữ liệu lỗi và tiếp tục xử lý.
        producer = Producer({"bootstrap.servers": "127.0.0.1:9092"})
        producer.produce(TOPICS["logs"] + suffix, value=b"not-json")
        producer.produce(TOPICS["logs"] + suffix, value=json.dumps({
            "log_level": "INFO", "service_name": "test", "message": "bad header",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }), headers=[("log_level", b"ERROR")])
        assert producer.flush(10) == 0
        print(f"Đã gửi và kiểm tra partition/headers. Chờ hết cửa sổ 60 giây; kết quả: {output}", flush=True)
        for process in processes:
            code = process.wait(timeout=75)
            if code:
                raise RuntimeError(f"Consumer trả về mã lỗi {code}; xem {output}")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for handle in handles:
            handle.close()
    states = json.loads((output / "network/network_state.json").read_text())
    assert len(states) == 3
    assert all(state["status"] == "Offline" for state in states.values())
    errors = read_jsonl(output / "logs/error.log")
    assert len(errors) == 3 and all(row["log_level"] == "ERROR" for row in errors)
    invalid = read_jsonl(output / "logs/invalid_messages.jsonl")
    assert len(invalid) == 2
    logs = read_jsonl(output / "logs/log_counts.jsonl")
    assert len(logs) >= 2 and all(row["window_seconds"] == 10 for row in logs)
    assert logs[0]["counts"] == {"INFO": 3, "ERROR": 3, "DEBUG": 3}
    assert logs[1]["counts"] == {"INFO": 0, "ERROR": 0, "DEBUG": 0}
    events = read_jsonl(output / "events/event_counts.log")
    assert events[0]["window_seconds"] == 60 and not events[0]["partial"]
    assert events[0]["counts"] == {"click": 3, "view": 3, "purchase": 3}
    summary = {
        "status": "PASS", "run_id": run_id, "python": platform.python_version(),
        "confluent_kafka": confluent_kafka.__version__, "kafka": "3.9.1", "topics_suffix": suffix,
        "network": {"messages": 6, "devices": 3, "final_status": "Offline"},
        "logs": {"valid_messages": 9, "errors": 3, "invalid_messages": 2,
                 "first_window": logs[0]["counts"], "idle_window": logs[1]["counts"]},
        "events": {"messages": 9, "window_seconds": 60, "counts": events[0]["counts"],
                   "partition_mapping_verified": True},
        "log_headers_verified": True,
    }
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
