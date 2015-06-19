import subprocess
import uuid

import kafka

from yelp_kafka.config import ClusterConfig, KafkaConsumerConfig
from yelp_kafka.consumer import KafkaSimpleConsumer


ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def create_topic(topic_name, replication_factor, partitions):
    cmd = ['/usr/bin/kafka-topics', '--create',
           '--zookeeper', ZOOKEEPER_URL,
           '--replication-factor', str(replication_factor),
           '--partitions', str(partitions),
           '--topic', topic_name]
    subprocess.check_call(cmd)


def create_random_topic(replication_factor, partitions):
    topic_name = str(uuid.uuid1())
    create_topic(topic_name, replication_factor, partitions)
    return topic_name


def test_simple_consumer():
    topic = create_random_topic(1, 1)

    messages = [str(i) for i in range(100)]

    producer = kafka.SimpleProducer(kafka.KafkaClient(KAFKA_URL))
    producer.send_messages(topic, *messages)

    cluster_config = ClusterConfig(None, [KAFKA_URL], ZOOKEEPER_URL)
    config = KafkaConsumerConfig('test', cluster_config,
                                 auto_offset_reset='smallest',
                                 auto_commit=False)
    consumer = KafkaSimpleConsumer(topic, config)

    with consumer:
        # If we don't get any exceptions here, we're good.
        for _ in xrange(100):
            consumer.get_message()
