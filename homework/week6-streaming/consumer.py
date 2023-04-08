import sys

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from settings import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_FHV_TOPIC,
                      KAFKA_GREEN_TOPIC)

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
    "group.id": "tripdata-consumer",
}
consumer = Consumer(conf)

running = True

topics = [KAFKA_FHV_TOPIC, KAFKA_GREEN_TOPIC]
# topics = ["rides_csv"]


def consume_loop(consumer: Consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        f"End of partition reached {msg.topic()} {msg.partition()}n"
                    )
                else:
                    raise KafkaException(msg.error())
            else:
                process(msg)
    finally:
        consumer.close()


def process(msg: Message):
    msg_byte: bytes = msg.value()
    msg_str: str = msg_byte.decode("utf-8")
    print(msg_str)


def shutdown():
    global running
    running = False


def main():
    try:
        consume_loop(consumer, topics)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
