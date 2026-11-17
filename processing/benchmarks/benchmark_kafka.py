from __future__ import annotations

import argparse
import json
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure local Kafka produce/consume throughput in a disposable topic."
    )
    parser.add_argument("--records", type=int, default=10_000)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.records < 1 or args.partitions < 1 or args.timeout_seconds < 1:
        raise ValueError("records, partitions, and timeout-seconds must be positive")

    topic = f"climate-benchmark-{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    topic_created = False
    report = None
    try:
        admin.create_topics(
            [NewTopic(topic, num_partitions=args.partitions, replication_factor=1)],
            request_timeout=args.timeout_seconds,
        )[topic].result(args.timeout_seconds)
        topic_created = True

        delivery_errors = []
        acknowledged = 0
        bytes_sent = 0

        def on_delivery(error, _message):
            nonlocal acknowledged
            if error is not None:
                delivery_errors.append(str(error))
            else:
                acknowledged += 1

        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "acks": "all",
                "compression.type": "lz4",
                "batch.size": 32768,
            }
        )
        produce_started = time.perf_counter()
        produce_deadline = time.monotonic() + args.timeout_seconds
        for index in range(args.records):
            payload = f"synthetic-kafka-benchmark-v1:{index:09d}".encode()
            bytes_sent += len(payload)
            while True:
                remaining = produce_deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Kafka producer remained backpressured past its deadline")
                try:
                    producer.produce(topic, value=payload, key=str(index).encode(), callback=on_delivery)
                    producer.poll(0)
                    break
                except BufferError:
                    producer.poll(min(0.1, remaining))
        producer.flush(args.timeout_seconds)
        produce_seconds = time.perf_counter() - produce_started
        if delivery_errors or acknowledged != args.records:
            raise RuntimeError(
                f"Kafka publish incomplete: acknowledged={acknowledged}/{args.records}; "
                f"errors={delivery_errors[:3]}"
            )

        metadata = admin.list_topics(topic=topic, timeout=args.timeout_seconds)
        topic_metadata = metadata.topics[topic]
        assignments = []
        watermark_consumer = Consumer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "group.id": f"{topic}-watermarks",
                "enable.auto.commit": False,
            }
        )
        try:
            for partition_id in sorted(topic_metadata.partitions):
                low, _ = watermark_consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id),
                    timeout=args.timeout_seconds,
                    cached=False,
                )
                assignments.append(TopicPartition(topic, partition_id, low))
        finally:
            watermark_consumer.close()

        consumer = Consumer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "group.id": f"{topic}-consumer",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
            }
        )
        consumer.assign(assignments)
        consumed = 0
        consume_started = time.perf_counter()
        deadline = time.monotonic() + args.timeout_seconds
        try:
            while consumed < args.records and time.monotonic() < deadline:
                message = consumer.poll(0.5)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consume failed: {error}")
                consumed += 1
        finally:
            consumer.close()
        consume_seconds = time.perf_counter() - consume_started
        if consumed != args.records:
            raise TimeoutError(f"Kafka consumer read {consumed} of {args.records} records")

        report = {
            "topic": topic,
            "topic_deleted_after_test": True,
            "records": args.records,
            "partitions": args.partitions,
            "payload_bytes": bytes_sent,
            "produce_seconds": round(produce_seconds, 3),
            "produce_records_per_second": round(args.records / produce_seconds, 2),
            "consume_seconds": round(consume_seconds, 3),
            "consume_records_per_second": round(args.records / consume_seconds, 2),
            "produce_and_consume_seconds": round(produce_seconds + consume_seconds, 3),
        }
    finally:
        if topic_created:
            admin.delete_topics([topic], operation_timeout=args.timeout_seconds)[topic].result(
                args.timeout_seconds
            )

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
