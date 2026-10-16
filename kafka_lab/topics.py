"""Tạo topic và xác nhận cấu hình dùng AdminClient."""

import argparse
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from .domain import TOPICS


def create_topics(bootstrap="127.0.0.1:9092", suffix=""):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    specs = {TOPICS[kind] + suffix: "compact" if kind == "network" else "delete" for kind in TOPICS}
    futures = admin.create_topics([
        NewTopic(name, num_partitions=3, replication_factor=1, config={"cleanup.policy": policy})
        for name, policy in specs.items()
    ], request_timeout=20)
    for name, future in futures.items():
        try:
            future.result(25)
            print(f"Đã tạo {name}")
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    metadata = admin.list_topics(timeout=15)
    configs = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, name) for name in specs])
    for resource, future in configs.items():
        topic = metadata.topics[resource.name]
        if topic.error or set(topic.partitions) != {0, 1, 2}:
            raise RuntimeError(f"{resource.name}: cần đúng 3 partition")
        policy = future.result(15)["cleanup.policy"].value
        if policy != specs[resource.name]:
            raise RuntimeError(f"{resource.name}: cleanup.policy phải là {specs[resource.name]}, hiện là {policy}")
        print(f"{resource.name}: partitions={sorted(topic.partitions)}, cleanup.policy={policy}")
    return specs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    args = parser.parse_args()
    create_topics(args.bootstrap)
