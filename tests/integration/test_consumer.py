import os
import kafka
import uuid

from yelp_kafka.config import ClusterConfig, KafkaConsumerConfig
from yelp_kafka.consumer import KafkaSimpleConsumer


ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def create_topic(topic_name, replication_factor, partitions):
    cmd = ' '.join(['/usr/bin/kafka-topics', '--create',
                    '--zookeeper', ZOOKEEPER_URL,
                    '--replication-factor', str(replication_factor),
                    '--partitions', str(partitions),
                    '--topic', topic_name])
    os.system(cmd)


def create_random_topic(replication_factor, partitions):
    topic_name = str(uuid.uuid1())
    create_topic(topic_name, replication_factor, partitions)
    return topic_name


def test_simple_consumer():
    topic = create_random_topic(1, 1)

    sent_messages = [str(i) for i in range(100)]

    producer = kafka.SimpleProducer(kafka.KafkaClient(KAFKA_URL))
    producer.send_messages(topic, *sent_messages)

    cluster_config = ClusterConfig(None, [KAFKA_URL], ZOOKEEPER_URL)
    config = KafkaConsumerConfig('test', cluster_config,
                                 auto_offset_reset='smallest')
    consumer = KafkaSimpleConsumer(topic, config)
    consumer.connect()

    received_messages = [consumer.get_message().value for _ in range(100)]

    assert received_messages == sent_messages
