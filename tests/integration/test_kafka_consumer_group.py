import os
import kafka
import uuid


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


# This test does not test any yelp_kafka code. It only tests if the integration
# test setup is done properly.
def test_itest_works():
    topic = create_random_topic(1, 1)

    producer = kafka.SimpleProducer(kafka.KafkaClient('kafka:9092'))
    producer.send_messages(topic, 'foobar')

    consumer = kafka.KafkaConsumer(topic, group_id='test',
                                   metadata_broker_list='kafka:9092',
                                   auto_offset_reset='smallest')
    messages = [message.value for message in consumer.fetch_messages()]

    assert messages == ['foobar']
