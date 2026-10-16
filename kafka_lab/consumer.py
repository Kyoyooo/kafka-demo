"""python -m kafka_lab.consumer network|logs|events"""

import argparse
import json
import signal
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition

from .domain import NetworkState, TOPICS, WindowCounter, append_json, utc_now, validate


def main():
    parser = argparse.ArgumentParser(description="Consumer xử lý và lưu kết quả bài thực hành")
    parser.add_argument("kind", choices=TOPICS)
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--topic")
    parser.add_argument("--group", help="Cùng group chia sẻ partition; group mới đọc từ đầu")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--duration", type=float, default=0, help="0 = chạy liên tục")
    parser.add_argument("--window-seconds", type=float, help="Mặc định logs=10, events=60")
    args = parser.parse_args()
    if args.duration < 0 or (args.window_seconds is not None and args.window_seconds <= 0):
        parser.error("duration phải >= 0 và window-seconds phải > 0")
    processor = (NetworkState(args.output_dir / "network_state.json") if args.kind == "network" else
                 WindowCounter(args.kind, args.output_dir, args.window_seconds or (10 if args.kind == "logs" else 60)))
    consumer = Consumer({"bootstrap.servers": args.bootstrap,
                         "group.id": args.group or f"{args.kind}-group",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         "allow.auto.create.topics": False})
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    started = time.monotonic()
    consumer.subscribe([args.topic or TOPICS[args.kind]])
    try:
        while running and (not args.duration or time.monotonic() - started < args.duration):
            if isinstance(processor, WindowCounter):
                processor.tick()
            batch = consumer.consume(num_messages=100, timeout=0.5)
            records = []
            for message in batch:
                if message.error():
                    raise KafkaException(message.error())
                try:
                    record = json.loads(message.value())
                    validate(args.kind, record)
                    if args.kind == "logs":
                        headers = dict(message.headers() or [])
                        level = headers.get("log_level")
                        if level is not None and level.decode() != record["log_level"]:
                            raise ValueError("Header log_level khác payload")
                    records.append((message, record))
                except (ValueError, TypeError, UnicodeError) as error:
                    append_json(args.output_dir / "invalid_messages.jsonl", {
                        "timestamp": utc_now(), "topic": message.topic(), "partition": message.partition(),
                        "offset": message.offset(), "error": str(error),
                        "payload": (message.value() or b"").decode(errors="replace"),
                    })
                    print(f"Bỏ qua thông điệp không hợp lệ: {error}", flush=True)
            if args.kind == "logs":
                records.sort(key=lambda pair: pair[1]["log_level"] != "ERROR")
            for message, record in records:
                if isinstance(processor, WindowCounter):
                    processor.tick()
                    try:
                        processor.handle(record, message.partition())
                    except ValueError as error:
                        append_json(args.output_dir / "invalid_messages.jsonl", {
                            "timestamp": utc_now(), "topic": message.topic(), "partition": message.partition(),
                            "offset": message.offset(), "error": str(error), "payload": record,
                        })
                else:
                    processor.handle(record)
            # Commit sau khi TOÀN BỘ batch thành công, kể cả batch đã sắp xếp ưu tiên.
            offsets = {}
            for message in batch:
                key = (message.topic(), message.partition())
                offsets[key] = max(offsets.get(key, 0), message.offset() + 1)
            if offsets:
                result = consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False)
                for partition in result:
                    if partition.error:
                        raise KafkaException(partition.error)
    finally:
        try:
            if isinstance(processor, WindowCounter):
                processor.close()
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
