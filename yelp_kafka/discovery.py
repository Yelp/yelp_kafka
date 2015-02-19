import logging
import os
import re

from kafka import KafkaClient

from yelp_kafka.config import YelpKafkaConfig
from yelp_kafka.config import TopologyConfiguration
from yelp_kafka.error import DiscoveryError
from yelp_kafka.utils import get_kafka_topics
from yelp_kafka.utils import make_scribe_topic


REGION_PATH = '/nail/etc/region'
DEFAULT_KAFKA_SCRIBE = 'scribe'


log = logging.getLogger(__name__)


def get_local_region():
    if os.path.exists(REGION_PATH):
        with open(REGION_PATH) as fd:
            return fd.read()
    else:
        raise DiscoveryError("Something is seriously wrong."
                             "Can't find a region file in this box")


def get_clusters_config(kafka_cluster_id, region=None):
    """Returns a list of tuples (cluster_name, ClusterConfig)
    for the kafka clusters in region. If no region is specified
    returns a list of clusters in the local region.
    """
    topology = TopologyConfiguration(kafka_id=kafka_cluster_id)
    if not region:
        region = get_local_region()
    return topology.get_clusters_for_region(region=region)


def get_clusters_broker_list(kafka_cluster_id, region=None):
    """Return a list (cluster_name, [broker_list]) for the local
    clusters or the clusters in region if specified.
    """
    clusters = get_clusters_config(kafka_cluster_id, region)
    return [(name, config.broker_list) for name, config in clusters]


def get_all_clusters_config(kafka_cluster_id):
    """Return a list of tuple (region, [(cluster_name, ClusterConfig)])
    .. note: the name cluster will likely be in many regions.
    """
    topology = TopologyConfiguration(kafka_id=kafka_cluster_id)
    return topology.get_all_clusters()


def get_kafka_connection(kafka_cluster_id, region=None):
    """Return a list of tuples (cluster_name, KafkaClient).

    .. note: This function create a KafkaClient for each cluster in a region
       and tries to connect to it. If a cluster is not available it fails and
       closes all the previous connections.
    .. warning: Connecting to a cluster fails if it is not in the current
       runtime env.
    """

    clusters = get_clusters_config(kafka_cluster_id, region)
    connected_clusters = []
    for cluster_name, cluster_config in clusters:
        try:
            client = KafkaClient(cluster_config.broker_list)
            connected_clusters.append((cluster_name, client))
        except:
            log.exception("Connection to kafka cluster %s using broker"
                          " list %s failed", cluster_name,
                          cluster_config.broker_list)
            for cluster, client in connected_clusters:
                client.close()
            raise DiscoveryError("Failed to connect to cluster {0}".format(
                cluster_name))
    return connected_clusters


def discover_topics(cluster_config):
    """Return all the topics in a cluster"""
    try:
        return get_kafka_topics(cluster_config.broker_list)
    except:
        log.exception("Topics discovery failed for %s",
                      cluster_config.broker_list)
        raise DiscoveryError("Failed to get topics information from "
                             "{0}".format(cluster_config.broker_list))


def search_topic(kafka_cluster_id, topic, region=None):
    """Search for a certain topic. It returns a list
    of tuples
    (topic, cluster_name, cluster_config).
    .. note: you can only search for topics in the current runtime env.
    """
    matches = []
    clusters = get_clusters_config(kafka_cluster_id, region)
    for name, config in clusters:
        topics = discover_topics(config)
        if topic in topics.keys():
            matches.append((topic, name, config))
    return matches


def search_topic_by_regex(kafka_cluster_id, pattern, region=None):
    """Search for all the topics that match pattern.
    It returns a list of tuples
    ([topics], cluster_name, cluster_config).
    .. note: you can only search for topics in the current runtime env.
    """
    matches = []
    clusters = get_clusters_config(kafka_cluster_id, region)
    for name, config in clusters:
        topics = discover_topics(config)
        valid_topics = [topic for topic in topics.iterkeys()
                        if re.match(pattern, topic)]
        if valid_topics:
            matches.append((valid_topics, name, config))
    return matches


def get_scribe_topic_in_datacenter(
    stream, datacenter,
    kafka_cluster_id=DEFAULT_KAFKA_SCRIBE, region=None
):
    """Search for the scribe topic for a scribe stream in the specified
    datacenter.
    Return (topic, cluster_name, cluster_config)
    .. note: you can only search for topics in the current runtime env.
    """
    return search_topic(kafka_cluster_id,
                        make_scribe_topic(stream, datacenter),
                        region)


def get_scribe_topics(
    stream, kafka_cluster_id=DEFAULT_KAFKA_SCRIBE, region=None
):
    """Search for all the scribe topics for a scribe stream.
    .. note: you can only search for topics in the current runtime env.
    """
    pattern = '^scribe\.\w*\.{0}$'.format(stream)
    return search_topic_by_regex(kafka_cluster_id,
                                 pattern,
                                 region)
