import subprocess
import uuid
import time
from multiprocessing import Process, Queue

import kafka

from yelp_kafka.config import ClusterConfig, KafkaConsumerConfig
from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.consumer_group import KafkaConsumerGroup


ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def create_topic(topic_name, replication_factor, partitions):
    cmd = ['/usr/bin/kafka-topics', '--create',
           '--zookeeper', ZOOKEEPER_URL,
           '--replication-factor', str(replication_factor),
           '--partitions', str(partitions),
           '--topic', topic_name]
    subprocess.check_call(cmd)

    # It may take a little moment for the topic to be ready for writing.
    time.sleep(1)


def create_random_topic(replication_factor, partitions):
    topic_name = str(uuid.uuid1())
    create_topic(topic_name, replication_factor, partitions)
    return topic_name


def test_simple_consumer():
    topic = create_random_topic(1, 1)

    messages = [str(i) for i in range(100)]

    producer = kafka.SimpleProducer(kafka.KafkaClient(KAFKA_URL))
    producer.send_messages(topic, *messages)

    cluster_config = ClusterConfig(None, None, [KAFKA_URL], ZOOKEEPER_URL)
    config = KafkaConsumerConfig('test', cluster_config,
                                 auto_offset_reset='smallest',
                                 auto_commit=False,
                                 consumer_timeout_ms=1000)
    consumer = KafkaSimpleConsumer(topic, config)

    with consumer:
        for expected_offset in xrange(100):
            message = consumer.get_message()
            assert message.offset == expected_offset
            assert message.partition == 0
            assert message.value == str(expected_offset)


def test_kafka_consumer_group_one_consumer_one_partition():
    run_kafka_consumer_group_test(1, 1)


def test_kafka_consumer_group_one_consumer_two_partitions():
    run_kafka_consumer_group_test(1, 2)


def test_kafka_consumer_group_two_consumers_one_partition():
    run_kafka_consumer_group_test(2, 1)


def test_kafka_consumer_group_two_consumers_two_partitions():
    run_kafka_consumer_group_test(2, 2)


def run_kafka_consumer_group_test(num_consumers, num_partitions):
    topic = create_random_topic(1, num_partitions)
    cluster_config = ClusterConfig(None, None, [KAFKA_URL], ZOOKEEPER_URL)
    config = KafkaConsumerConfig('test', cluster_config,
                                 auto_offset_reset='smallest')

    queue = Queue()

    def create_consumer():
        def consume():
            consumer = KafkaConsumerGroup([topic], config)
            with consumer:
                while True:
                    queue.put(consumer.next())

        p = Process(target=consume)
        p.daemon = True
        return p

    consumer_processes = [create_consumer() for _ in xrange(num_consumers)]

    for consumer_process in consumer_processes:
        consumer_process.start()

    producer = kafka.producer.base.Producer(kafka.KafkaClient(KAFKA_URL))

    for i in xrange(100):
        producer.send_messages(topic, i % num_partitions, str(i))

    # wait until all 100 messages have been consumed
    while queue.qsize() < 100:
        pass

    received_messages = []
    while not queue.empty():
        message = queue.get()
        received_messages.append(int(message.value))

    assert range(100) == sorted(received_messages)
