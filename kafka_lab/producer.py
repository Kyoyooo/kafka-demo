"""python -m kafka_lab.producer network|logs|events"""

import argparse
import json
import random
import time

from confluent_kafka import Producer

from .domain import EVENT_PARTITIONS, LEVELS, TOPICS, event_partitioner, utc_now


def make_record(kind, index, rng):
    stamp = utc_now()
    if kind == "network":
        devices = (("server-01", "server"), ("router-01", "router"), ("switch-01", "switch"))
        device, device_type = devices[index % len(devices)]
        return {"device_id": device, "device_type": device_type,
                "status": "Offline" if (index // 3) % 2 else "Online", "timestamp": stamp}
    if kind == "logs":
        level = LEVELS[index % len(LEVELS)]
        return {"log_level": level, "service_name": rng.choice(("web", "auth", "payment")),
                "message": {"INFO": "Request completed", "ERROR": "Service request failed", "DEBUG": "Inspect request context"}[level],
                "timestamp": stamp}
    return {"user_id": f"user-{rng.randrange(1, 11):02}",
            "event_type": tuple(EVENT_PARTITIONS)[index % 3],
            "item_id": f"item-{rng.randrange(1, 21):02}", "timestamp": stamp}


def main():
    parser = argparse.ArgumentParser(description="Giả lập thiết bị, log ứng dụng hoặc sự kiện người dùng")
    parser.add_argument("kind", choices=TOPICS)
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--topic")
    parser.add_argument("--count", type=int, default=0, help="0 = liên tục")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.count < 0 or args.interval < 0:
        parser.error("count và interval phải >= 0")
    topic = args.topic or TOPICS[args.kind]
    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": f"{args.kind}-producer",
                         "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 15000})
    metadata = producer.list_topics(topic, timeout=10)
    topic_metadata = metadata.topics.get(topic)
    if not topic_metadata or topic_metadata.error:
        raise RuntimeError(f"Topic chưa sẵn sàng: {topic}")
    partitions = tuple(topic_metadata.partitions)
    if args.kind == "events" and set(partitions) != {0, 1, 2}:
        raise RuntimeError("Topic sự kiện cần đúng 3 partition: 0, 1, 2")
    failures = []
    delivered = 0

    def on_delivery(error, message):
        nonlocal delivered
        if error:
            failures.append(str(error))
        else:
            delivered += 1
            print(f"ACK {message.topic()} partition={message.partition()} offset={message.offset()}", flush=True)

    rng = random.Random(args.seed)
    index = 0
    try:
        while args.count == 0 or index < args.count:
            record = make_record(args.kind, index, rng)
            key = record[{"network": "device_id", "logs": "service_name", "events": "user_id"}[args.kind]]
            extra = {}
            if args.kind == "events":
                extra["partition"] = event_partitioner(record, partitions)
            if args.kind == "logs":
                extra["headers"] = [("log_level", record["log_level"].encode())]
            while True:
                try:
                    producer.produce(topic, key=key, value=json.dumps(record, ensure_ascii=False),
                                     on_delivery=on_delivery, **extra)
                    break
                except BufferError:
                    producer.poll(0.5)
            print(json.dumps(record, ensure_ascii=False), flush=True)
            producer.poll(0)
            if failures:
                raise RuntimeError("; ".join(failures))
            index += 1
            # poll cũng phục vụ callback trong thời gian chờ giữa 2 thông điệp.
            deadline = time.monotonic() + args.interval
            while time.monotonic() < deadline:
                producer.poll(min(0.1, max(0, deadline - time.monotonic())))
    except KeyboardInterrupt:
        print("Dừng producer, chờ gửi hết hàng đợi...", flush=True)
    finally:
        remaining = producer.flush(20)
    if remaining or failures:
        raise RuntimeError(f"Gửi thất bại: remaining={remaining}, errors={failures}")
    print(f"Đã gửi thành công {delivered} thông điệp", flush=True)


if __name__ == "__main__":
    main()
