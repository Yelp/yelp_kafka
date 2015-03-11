import logging

from kafka.client import KafkaClient
from kafka.common import KafkaUnavailableError


log = logging.getLogger(__name__)


def get_kafka_topics(brokers_list):
    kafkaclient = KafkaClient(brokers_list)
    try:
        kafkaclient.load_metadata_for_topics()
    except KafkaUnavailableError:
        # Sometimes the kafka server closes the connection for inactivity
        # in this case the second call should succeed otherwise the kafka
        # server is down and we should fail
        log.warning("First call to kafka for loading metadata failed."
                    " Trying again.")
        kafkaclient.load_metadata_for_topics()
    finally:
        kafkaclient.close()
    return kafkaclient.topic_partitions


def make_scribe_topic(stream, datacenter):
    return "scribe.{0}.{1}".format(datacenter, stream)
