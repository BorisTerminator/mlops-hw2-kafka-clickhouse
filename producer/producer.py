"""Читает CSV с транзакциями и построчно отправляет его в топик Kafka в формате JSON."""

import argparse
import csv
import json
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

INT_FIELDS = {"post_code", "population_city", "target"}
FLOAT_FIELDS = {"amount", "lat", "lon", "merchant_lat", "merchant_lon"}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=os.getenv("CSV_PATH", "data/train.csv"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--limit", type=int, default=None, help="отправить только первые N строк")
    return parser.parse_args()


def ensure_topic(bootstrap, topic):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=30).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    futures[topic].result()
    print(f"Создан топик {topic}")


def convert(row):
    for key in INT_FIELDS:
        row[key] = int(row[key])
    for key in FLOAT_FIELDS:
        row[key] = float(row[key])
    return row


def main():
    args = parse_args()
    ensure_topic(args.bootstrap, args.topic)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
    })

    started = time.time()
    sent = 0
    with open(args.csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if args.limit is not None and sent >= args.limit:
                break
            payload = json.dumps(convert(row), ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce(args.topic, value=payload)
                    break
                except BufferError:
                    # локальная очередь продюсера заполнена - ждём, пока брокер её разберёт
                    producer.poll(1)
            sent += 1
            if sent % 100_000 == 0:
                producer.poll(0)
                print(f"Отправлено {sent} строк")

    producer.flush()
    print(f"Готово: {sent} строк в топик {args.topic} за {time.time() - started:.1f} с")


if __name__ == "__main__":
    main()
