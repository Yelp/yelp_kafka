import logging

from kafka.common import KafkaUnavailableError


log = logging.getLogger(__name__)


def get_kafka_topics(kafkaclient):
    """Connect to kafka and fetch all the topics/partitions."""
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
    """Get a scribe topic name

    :param stream: scribe stream name
    :param datacenter: datacenter name
    :returns: topic name
    """
    return "scribe.{0}.{1}".format(datacenter, stream)


def split_scribe_topic(topic_name):
    """Get the datacentre and stream from a kafka topic name

    :param topic_name: Kafka topic name
    "returns: datacentre and stream name
    """
    try:
        (_, datacentre, stream) = topic_name.split(".")
    except ValueError:
        log.warning("Encountered wrongly formatted topic %s" % topic_name)
        raise
    else:
        return (datacentre, stream)
