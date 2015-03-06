import logging
import os
import re

from kafka import KafkaClient

from yelp_kafka.config import YelpKafkaConfig
from yelp_kafka.config import TopologyConfiguration
from yelp_kafka.error import DiscoveryError
from yelp_kafka.utils import get_kafka_topics
from yelp_kafka.utils import make_scribe_topic


ECOSYSTEM_PATH = '/nail/etc/ecosystem'
DEFAULT_KAFKA_SCRIBE = 'scribe'


log = logging.getLogger(__name__)


def get_local_ecosystem():
    if os.path.exists(ECOSYSTEM_PATH):
        with open(ECOSYSTEM_PATH) as fd:
            return fd.read().strip()
    else:
        raise DiscoveryError("Something is seriously wrong."
                             "Can't find an ecosystem file on this box")


def make_scribe_regex(stream):
    return '^scribe\.\w*\.{0}$'.format(re.escape(stream))


def get_local_cluster(cluster_type):
    """Get a list of tuples (cluster_name, :py:class:`yelp_kafka.config.ClusterConfig`)
    for the kafka clusters in region.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param region: the name of the region where to look for the cluster.
        Default: local region.
    :type region: string
    :returns: list :py:class:`yelp_kafka.config.ClusterConfig`
    """
    topology = TopologyConfiguration(kafka_id=cluster_type)
    return topology.get_local_cluster()


def get_clusters_in_ecosystem(cluster_type, ecosystem=None):
    """Get a list of tuples (cluster_name, :py:class:`yelp_kafka.config.ClusterConfig`)
    for the kafka clusters in ecosystem.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param ecosystem: the name of the ecosystem where to look for the cluster.
        Default: local ecosystem.
    :type ecosystem: string
    :returns: list :py:class:`yelp_kafka.config.ClusterConfig`
    """
    topology = TopologyConfiguration(kafka_id=cluster_type)
    if not ecosystem:
        ecosystem = get_local_ecosystem()
    return topology.get_clusters_for_ecosystem(ecosystem)


def get_yelp_kafka_config(cluster_type, group_id, **extra):
    """Get a list of tuples (cluster_name, :py:class:`yelp_kafka.config.YelpKafkaConfig`)
    for the kafka clusters in region.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param group_id: consumer group id
    :type group_id: string
    :param region: the name of the region where to look for the cluster.
        Default: local region.
    :type region: string
    :param extra: extra arguments to use for creating the configuration
    :returns: list (name, :py:class:`yelp_kafka.config.YelpKafkaConfig`)
    """
    cluster = get_local_cluster(cluster_type)
    return YelpKafkaConfig(group_id=group_id, cluster=cluster, **extra)


def get_yelp_kafka_config_in_ecosystem(cluster_type, group_id, ecosystem=None, **extra):
    """Get a list of tuples (cluster_name, :py:class:`yelp_kafka.config.YelpKafkaConfig`)
    for the kafka clusters in ecosystem.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param group_id: consumer group id
    :type group_id: string
    :param ecosystem: the name of the ecosystem where to look for the cluster.
        Default: local ecosystem.
    :type ecosystem: string
    :param extra: extra arguments to use for creating the configuration
    :returns: list (name, :py:class:`yelp_kafka.config.YelpKafkaConfig`)
    """
    clusters = get_clusters_in_ecosystem(cluster_type, ecosystem=ecosystem)
    return [YelpKafkaConfig(group_id=group_id, cluster=cluster, **extra)
            for cluster in clusters]


def get_kafka_connection(cluster_type, client_id='yelp-kafka'):
    """Get a kafka connection for each cluster in region.

    :param cluster_type: the id of the kafka cluster (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: client_id to be used to connect to kafka.
    :type client_id: string
    :param region: the name of the region where to look for the cluster. Default: local region.
    :type region: string
    :returns: list (cluster, KafkaClient)
    :raises DiscoveryError: :py:class:`yelp_kafka.error.DiscoveryError` upon failure connecting connecting to a cluster.

    .. note: This function create a KafkaClient for each cluster in a region
       and tries to connect to it. If a cluster is not available it fails and
       closes all the previous connections.
    .. warning: Connecting to a cluster fails if the cluster is not in
       the current runtime env.
    """

    cluster = get_local_cluster(cluster_type)
    return KafkaClient(cluster.broker_list, client_id=client_id)


def get_kafka_connections_in_ecosystem(cluster_type, client_id='yelp-kafka'):
    """Get a kafka connection for each cluster in region.

    :param cluster_type: the id of the kafka cluster (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: client_id to be used to connect to kafka.
    :type client_id: string
    :param region: the name of the region where to look for the cluster. Default: local region.
    :type region: string
    :returns: list (cluster, KafkaClient)
    :raises DiscoveryError: :py:class:`yelp_kafka.error.DiscoveryError` upon failure connecting connecting to a cluster.

    .. note: This function create a KafkaClient for each cluster in a region
       and tries to connect to it. If a cluster is not available it fails and
       closes all the previous connections.
    .. warning: Connecting to a cluster fails if the cluster is not in
       the current runtime env.
    """

    clusters = get_clusters_in_ecosystem(cluster_type)
    connected_clusters = []
    for cluster in clusters:
        try:
            client = KafkaClient(cluster.broker_list, client_id=client_id)
            connected_clusters.append((cluster.name, client))
        except:
            log.exception("Connection to kafka cluster %s using broker"
                          " list %s failed", cluster.name,
                          cluster.broker_list)
            for name, client in connected_clusters:
                client.close()
            raise DiscoveryError("Failed to connect to cluster {0}".format(
                cluster.name))
    return connected_clusters


def discover_topics(cluster_config):
    """Get all the topics in a cluster
    :param cluster_config: config of the cluster to get topics from
    :type cluster_config: :py:class:`yelp_kafka.config.ClusterConfig`
    :returns: a dict <topic>: <[partitions]>
    """
    try:
        return get_kafka_topics(cluster_config['broker_list'])
    except:
        log.exception("Topics discovery failed for %s",
                      cluster_config.broker_list)
        raise DiscoveryError("Failed to get topics information from "
                             "{0}".format(cluster_config.broker_list))


def search_topic(topic, clusters):
    """Find the topic in the list of clustes.

    :param topic: topic name
    :param clusters: list of :py:class:`yelp_kafka.config.ClusterConfig`
    :returns: (topic, cluster_config).
    """
    matches = []
    for cluster in clusters:
        topics = discover_topics(cluster)
        if topic in topics.keys():
            matches.append((topic, cluster))
    return matches


def search_local_topic(cluster_type, topic):
    """Search for a topic in a specific cluster.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param topic: topic name
    :type topic: string
    :returns: list (topic, cluster_config).

    .. note: you can only search for topics in the current runtime env.
    """
    cluster = get_local_cluster(cluster_type)
    return search_topic(topic, [cluster])


def search_topic_in_ecosystem(cluster_type, topic):
    """Search for a topic in a specific cluster.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param topic: topic name
    :type topic: string
    :returns: list (topic, cluster_config).
    """
    clusters = get_clusters_in_ecosystem(cluster_type)
    return search_topic(topic, clusters)


def search_topics_by_regex(pattern, clusters):
    """Find the topics matching pattern in the list of clustes.

    :param pattern: regex to match topics
    :param clusters: list of :py:class:`yelp_kafka.config.ClusterConfig`
    :returns: list (topics, cluster_config).
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
    """Search for all the topics matching pattern.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param pattern: regex to match topics
    :param region: the name of the region where to look for the cluster.
        Default: local region.
    :type region: string
    :returns: list ([topics], cluster_config).

    .. note: you can only search for topics in the current runtime env.
    """
    cluster = get_local_cluster(cluster_type)
    return search_topics_by_regex(pattern, [cluster])


def search_topics_by_regex_in_ecosystem(cluster_type, pattern):
    """Search for all the topics matching pattern.

    :param cluster_type: the id of the kafka cluster
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param pattern: regex to match topics
    :returns: a list of tuples ([topics], cluster_name, cluster_config).

    .. note: you can only search for topics in the current runtime env.
    """
    clusters = get_clusters_in_ecosystem(cluster_type)
    return search_topics_by_regex(pattern, clusters)


def get_local_scribe_topic(stream):
    topology = TopologyConfiguration(kafka_id=DEFAULT_KAFKA_SCRIBE)
    cluster = topology.get_local_cluster()
    prefix = topology.get_scribe_local_prefix()
    if not prefix:
        raise DiscoveryError("Scribe cluster config must contain a valid "
                             "prefix. Invalid topology configuration "
                             "{topology}".format(topology=topology))
    topic = '{prefix}.{stream}'.format(
        prefix=topology.get_scribe_local_prefix(),
        stream=stream
    )
    search_topic(topic, [cluster])


def get_scribe_topic_in_datacenter(stream, datacenter):
    """Search for the scribe topic for a scribe stream in the specified
    datacenter.

    :param stream: scribe stream name
    :type stream: string
    :param datacenter: datacenter name
    :type datacenter: string
    :param cluster_type: the id of the kafka cluster. Default: scribe.
    :type cluster_type: string
    :param region: the name of the region where to look for the cluster.
        Default: local region.
    :type region: string
    :returns: [(topic, cluster_name, cluster_config)]

    .. note: you can only search for topics in the current runtime env.
    """
    return search_local_topic(DEFAULT_KAFKA_SCRIBE,
                              make_scribe_topic(stream, datacenter))


def get_scribe_topics(stream):
    """Search for all the topics for a scribe stream.

    :param stream: scribe stream name
    :type stream: string
    :param cluster_type: the id of the kafka cluster. Default: scribe.
    :type cluster_type: string
    :param region: the name of the region where to look for the cluster.
        Default: local region.
    :returns: [([topics], cluster_name, cluster_config)]

    .. note: you can only search for topics in the current runtime env.
    """
    pattern = make_scribe_regex(stream)
    return search_local_topics_by_regex(DEFAULT_KAFKA_SCRIBE, pattern)


def get_scribe_topics_in_local_ecosystem(stream):
    """Search for all the topics for a scribe stream.

    :param stream: scribe stream name
    :type stream: string
    :param cluster_type: the id of the kafka cluster. Default: scribe.
    :type cluster_type: string
    :returns: [([topics], cluster_name, cluster_config)]

    .. note: you can only search for topics in the current runtime env.
    """
    pattern = make_scribe_regex(stream)
    return search_topics_by_regex_in_ecosystem(DEFAULT_KAFKA_SCRIBE, pattern)
