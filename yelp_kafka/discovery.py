import logging
import re

from kafka import KafkaClient

from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.config import TopologyConfiguration
from yelp_kafka.error import ConfigurationError
from yelp_kafka.error import DiscoveryError
from yelp_kafka.utils import get_kafka_topics
from yelp_kafka.utils import make_scribe_topic


ECOSYSTEM_PATH = '/nail/etc/ecosystem'
DEFAULT_KAFKA_SCRIBE = 'scribe'


log = logging.getLogger(__name__)


def make_scribe_regex(stream):
    return '^scribe\.[\w-]+\.{0}$'.format(re.escape(stream))


def get_local_cluster(cluster_type):
    """Get the local kafka cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :returns: py:class:`yelp_kafka.config.ClusterConfig`
    """
    topology = TopologyConfiguration(cluster_type=cluster_type)
    return topology.get_local_cluster()


def get_all_clusters(cluster_type):
    """Get a list of (cluster_name, cluster_config)
    for the available kafka clusters in the ecosystem.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :returns: list of py:class:`yelp_kafka.config.ClusterConfig`
    """
    topology = TopologyConfiguration(cluster_type=cluster_type)
    return topology.get_all_clusters()


def get_cluster_by_name(cluster_type, cluster_name):
    """Get a :py:class:`yelp_kafka.config.ClusterConfig` from an ecosystem with
    a particular name.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param cluster_name: name of the cluster
        (ex.'uswest1-devc').
    :type cluster_type: string
    :returns: :py:class:`yelp_kafka.config.ClusterConfig`
    """
    topology = TopologyConfiguration(cluster_type=cluster_type)
    return topology.get_cluster_by_name(cluster_name)


def get_consumer_config(cluster_type, group_id, **extra):
    """Get a :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    for the local kafka cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param group_id: consumer group id
    :type group_id: string
    :param extra: extra arguments to use for creating the configuration
    :returns: :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    """
    cluster = get_local_cluster(cluster_type)
    return KafkaConsumerConfig(group_id=group_id, cluster=cluster, **extra)


def get_all_consumer_config(cluster_type, group_id, ecosystem=None, **extra):
    """Get a list of :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    for the kafka clusters in ecosystem.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param group_id: consumer group id
    :type group_id: string
    :param extra: extra arguments to use for creating the configuration
    :returns: list :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    """
    clusters = get_all_clusters(cluster_type)
    return [KafkaConsumerConfig(group_id=group_id, cluster=cluster, **extra)
            for cluster in clusters]


def get_kafka_connection(cluster_type, client_id='yelp-kafka', **kwargs):
    """Get a kafka connection for the local kafka cluster.

    :param cluster_type: kafka cluster type (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: client_id to be used to connect to kafka.
    :type client_id: string
    :param kwargs: parameters to pass along when creating the KafkaClient instance.
    :returns: KafkaClient
    :raises DiscoveryError: :py:class:`yelp_kafka.error.DiscoveryError` upon failure connecting to a cluster.
    """
    cluster = get_local_cluster(cluster_type)
    try:
        return KafkaClient(cluster.broker_list, client_id=client_id, **kwargs)
    except:
        log.exception("Connection to kafka cluster %s using broker"
                      " list %s failed", cluster.name,
                      cluster.broker_list)
        raise DiscoveryError("Failed to connect to cluster {0}".format(
            cluster.name))


def get_all_kafka_connections(cluster_type, client_id='yelp-kafka', **kwargs):
    """Get a kafka connection for each available kafka cluster.

    :param cluster_type: kafka cluster type (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: client_id to be used to connect to kafka.
    :type client_id: string
    :param kwargs: parameters to pass along when creating the KafkaClient instance.
    :returns: list (cluster_name, KafkaClient)
    :raises DiscoveryError: :py:class:`yelp_kafka.error.DiscoveryError` upon failure connecting to a cluster.

    .. note:: This function creates a KafkaClient for each cluster in a region and tries to connect to it. If a cluster is not available it fails and closes all the previous connections.
    """

    clusters = get_all_clusters(cluster_type)
    connected_clusters = []
    for cluster in clusters:
        try:
            client = KafkaClient(cluster.broker_list, client_id=client_id, **kwargs)
            connected_clusters.append((cluster.name, client))
        except:
            log.exception("Connection to kafka cluster %s using broker"
                          " list %s failed", cluster.name,
                          cluster.broker_list)
            for _, client in connected_clusters:
                client.close()
            raise DiscoveryError("Failed to connect to cluster {0}".format(
                cluster.name))
    return connected_clusters


def discover_topics(cluster):
    """Get all the topics in a cluster

    :param cluster: config of the cluster to get topics from
    :type cluster: ClusterConfig
    :returns: a dict <topic>: <[partitions]>
    :raises DiscoveryError: upon failure to request topics from kafka
    """
    client = KafkaClient(cluster.broker_list)
    try:
        return get_kafka_topics(client)
    except:
        log.exception("Topics discovery failed for %s",
                      cluster.broker_list)
        raise DiscoveryError("Failed to get topics information from "
                             "{cluster}".format(cluster=cluster))


def search_topic(topic, clusters=None):
    """Find the topic in the list of clusters or the local cluster

    :param topic: topic name
    :param clusters: list of cluster config
    :returns: [(topic, cluster)].
    """
    matches = []
    for cluster in clusters:
        topics = discover_topics(cluster)
        if topic in topics.keys():
            matches.append((topic, cluster))
    return matches


def local_topic_exists(cluster_type, topic):
    """Search for a topic in the local kafka cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param topic: topic name
    :type topic: string
    :returns: True is the topic exists or False
    """
    cluster = get_local_cluster(cluster_type)
    result = search_topic(topic, [cluster])
    return len(result) > 0


def search_topic_in_all_clusters(cluster_type, topic):
    """Search for a topic in the all available clusters.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param topic: topic name
    :type topic: string
    :returns: list (topic, cluster_config).
    """
    clusters = get_all_clusters(cluster_type)
    results = search_topic(topic, clusters)
    if not results:
        raise DiscoveryError("Topic {topic} does not exist".format(
            topic=topic
        ))
    return results


def search_topics_by_regex(pattern, clusters=None):
    """Find the topics matching pattern in the list of clusters.

    :param pattern: regex to match topics
    :param clusters: list of cluster config
    :returns: [([topics], cluster)].
    :rtype: list
    """
    matches = []
    for cluster in clusters:
        topics = discover_topics(cluster)
        valid_topics = [topic for topic in topics.iterkeys()
                        if re.match(pattern, topic)]
        if valid_topics:
            matches.append((valid_topics, cluster))
    return matches


def search_local_topics_by_regex(cluster_type, pattern):
    """Search for all the topics matching pattern in the local cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param pattern: regex to match topics
    :returns: ([topics], cluster).
    :raises DiscoveryError: if the topic does not exist
    """
    cluster = get_local_cluster(cluster_type)
    result = search_topics_by_regex(pattern, [cluster])
    if not result:
        raise DiscoveryError(
            "No Kafka topics for pattern {pattern}".format(
                pattern=pattern
            )
        )
    return result[0]


def search_local_scribe_topics_by_regex(pattern):
    """Search for all local scribe topics matching pattern.

    :param pattern: regex to match topics
    :returns: ([topics], cluster)
    :raises DiscoveryError: if no matching topics exist
    """
    topology = TopologyConfiguration(cluster_type=DEFAULT_KAFKA_SCRIBE)
    prefix = re.escape(topology.get_scribe_local_prefix())
    return search_local_topics_by_regex('scribe', prefix + pattern)


def search_topics_by_regex_in_all_clusters(cluster_type, pattern):
    """Search for all the topics matching pattern.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param pattern: regex to match topics
    :returns: a list of tuples ([topics], cluster).
    :raises DiscoveryError: if the topic does not exist
    """
    clusters = get_all_clusters(cluster_type)
    results = search_topics_by_regex(pattern, clusters)
    if not results:
        raise DiscoveryError(
            "No Kafka topics for pattern {pattern}".format(
                pattern=pattern
            )
        )
    return results


def local_scribe_topic_exists(stream):
    """Check if a scribe topic exists in the local cluster.
    :param stream: scribe stream name
    :type stream: string
    :returns: True if the topic exists in the local cluster
    :raises ConfigurationError: if the local prefix is not in the config file
    """
    topology = TopologyConfiguration(cluster_type=DEFAULT_KAFKA_SCRIBE)
    cluster = topology.get_local_cluster()
    prefix = topology.get_scribe_local_prefix()
    if not prefix:
        raise ConfigurationError("Scribe cluster config must contain a valid "
                                 "prefix. Invalid topology configuration "
                                 "{topology}".format(topology=topology))
    topic = '{prefix}{stream}'.format(
        prefix=topology.get_scribe_local_prefix(),
        stream=stream
    )
    result = search_topic(topic, [cluster])
    return len(result) > 0


def get_local_scribe_topic(stream):
    """Search for the local topic matching the given scribe stream.

    :param stream: scribe stream name
    :type stream: string
    :returns: (topic, cluster)
    :raises DiscoveryError: if the topic does not exist
    """
    topology = TopologyConfiguration(cluster_type=DEFAULT_KAFKA_SCRIBE)
    cluster = topology.get_local_cluster()
    prefix = topology.get_scribe_local_prefix()
    if not prefix:
        raise DiscoveryError("Scribe cluster config must contain a valid "
                             "prefix. Invalid topology configuration "
                             "{topology}".format(topology=topology))
    topic = '{prefix}{stream}'.format(
        prefix=topology.get_scribe_local_prefix(),
        stream=stream
    )
    result = search_topic(topic, [cluster])
    if not result:
        raise DiscoveryError(
            "No Kafka topic {topic} for stream {stream}".format(
                topic=topic,
                stream=stream
            )
        )
    # Return the topic name if it exists
    return result[0]


def get_all_local_scribe_topics(stream):
    """Search for all the topics for a scribe stream in the local kafka cluster.

    :param stream: scribe stream name
    :type stream: string
    :returns: ([topics], cluster)
    :raises DiscoveryError: if the topic does not exist
    """
    pattern = make_scribe_regex(stream)
    return search_local_topics_by_regex(DEFAULT_KAFKA_SCRIBE, pattern)


def get_scribe_topics(stream, clusters=None):
    """Search for all the topics for a scribe stream in
    all available clusters, or in the given list of clusters.

    :param stream: scribe stream name
    :type stream: string
    :param clusters: list of cluster config
    :type cluster: ClusterConfig
    :returns: [([topics], cluster)]
    :raises DiscoveryError: if the topic does not exist
    """
    pattern = make_scribe_regex(stream)
    if not clusters:
        results = search_topics_by_regex_in_all_clusters(DEFAULT_KAFKA_SCRIBE, pattern)
    else:
        results = search_topics_by_regex(pattern, clusters)

    if not results:
        raise DiscoveryError(
            "No Kafka topics for stream {stream}".format(
                stream=stream
            )
        )
    return results


def scribe_topic_exists_in_datacenter(stream, datacenter):
    """Check if a scribe topic exists in a certain datacenter.
    :param stream: scribe stream name
    :type stream: string
    :param datacenter: datacenter name
    :type datacenter: string
    :returns: True if the topic exists, False otherwise.
    """
    result = search_topic_in_all_clusters(DEFAULT_KAFKA_SCRIBE,
                                          make_scribe_topic(stream, datacenter))
    return len(result) > 0


def get_scribe_topic_in_datacenter(stream, datacenter):
    """Search for the scribe topic for a scribe stream in the specified
    datacenter.

    :param stream: scribe stream name
    :type stream: string
    :param datacenter: datacenter name
    :type datacenter: string
    :returns: (topic, cluster)
    :raises DiscoveryError: if the topic does not exist
    """
    result = search_topic_in_all_clusters(DEFAULT_KAFKA_SCRIBE,
                                          make_scribe_topic(stream, datacenter))
    if not result:
        raise DiscoveryError(
            "No Kafka topic for stream {stream} in {datacenter}".format(
                stream=stream, datacenter=datacenter
            )
        )
    # Return both topic and cluster
    return result[0]
