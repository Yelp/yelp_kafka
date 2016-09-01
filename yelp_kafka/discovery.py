# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import re
from collections import defaultdict

import six
from bravado.exception import HTTPError
from kafka import KafkaClient

from yelp_kafka.config import ClusterConfig
from yelp_kafka.config import get_kafka_discovery_client
from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.error import DiscoveryError
from yelp_kafka.error import InvalidClusterType
from yelp_kafka.error import InvalidClusterTypeOrNameError
from yelp_kafka.error import InvalidClusterTypeOrRegionError
from yelp_kafka.error import InvalidClusterTypeOrSuperregionError
from yelp_kafka.error import InvalidLogOrRegionError
from yelp_kafka.error import InvalidLogOrSuperregionError
from yelp_kafka.utils import get_kafka_topics

DEFAULT_KAFKA_SCRIBE = 'scribe'
DEFAULT_CLIENT_ID = 'yelp_kafka.default'
REGION_FILE_PATH = '/nail/etc/region'
SUPERREGION_FILE_PATH = '/nail/etc/superregion'


log = logging.getLogger(__name__)


def _get_local_region():
    """Get name of the region where yelp_kafka instance is running (caller)
    from region-file path."""
    try:
        with open(REGION_FILE_PATH, 'r') as region_file:
            return region_file.read().rstrip()
    except IOError:
        err_msg = "Could not retrieve region information at {file}".format(
            file=REGION_FILE_PATH,
        )
        log.exception(err_msg)
        raise


def _get_local_superregion():
    """Get name of the superregion where yelp_kafka instance is running (caller)
    from superregion-file path."""
    try:
        with open(SUPERREGION_FILE_PATH, 'r') as superregion_file:
            return superregion_file.read().rstrip()
    except IOError:
        err_msg = "Could not retrieve superregion information at {file}".format(
            file=SUPERREGION_FILE_PATH,
        )
        log.exception(err_msg)
        raise


def parse_as_scribe_topics(logs_result):
    """Parse response topic configuration from kafka-discovery to desired type.

    :returns: [([topics], cluster)]
    """
    cluster_to_topics_info = defaultdict(list)
    for log_result in logs_result:
        for topic_info in log_result.topics:
            cluster_config = parse_as_cluster_config(topic_info.cluster)
            cluster_to_topics_info[cluster_config].append(topic_info.topic)
    return [(topics, cluster) for cluster, topics in six.iteritems(cluster_to_topics_info)]


def parse_as_cluster_config(config_obj):
    """Parse response config to Cluster-config type."""
    return ClusterConfig(
        name=config_obj.name,
        type=config_obj.type,
        broker_list=config_obj.broker_list,
        zookeeper=config_obj.zookeeper,
    )


def get_region_cluster(cluster_type, client_id, region=None):
    """Get the kafka cluster for given region. If no region is given, we default
    to local region.

    :param cluster_type: kafka cluster type (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: name of the client making the discovery request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param region: region name for which kafka cluster is desired.
    :type region: string
    :returns: py:class:`yelp_kafka.config.ClusterConfig`
    """
    if not region:
        region = _get_local_region()

    client = get_kafka_discovery_client(client_id)
    try:
        result = client.v1.getClustersWithRegion(
            type=cluster_type,
            region=region,
        ).result()
        return parse_as_cluster_config(result)
    except HTTPError as e:
        log.exception(
            "Failure while fetching kafka cluster for cluster-type:{type}, region"
            ":{region}.".format(type=cluster_type, region=region),
        )
        raise InvalidClusterTypeOrRegionError(e.response.text)


def get_superregion_cluster(cluster_type, client_id, superregion=None):
    """Get the kafka cluster for given superregion. If no region is specified,
    we default to local superregion.

    :param cluster_type: kafka cluster type (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: name of the client making the discovery request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param superregion: region name for which kafka cluster is desired.
    :type superregion: string
    :returns: py:class:`yelp_kafka.config.ClusterConfig`
    """
    if not superregion:
        superregion = _get_local_superregion()
    client = get_kafka_discovery_client(client_id)

    try:
        result = client.v1.getClustersWithSuperregion(
            type=cluster_type,
            superregion=superregion,
        ).result()
        return parse_as_cluster_config(result)
    except HTTPError as e:
        log.exception(
            "Failure while fetching kafka cluster for cluster-type:{type}, "
            "superregion :{superregion}.".format(type=cluster_type, superregion=superregion),
        )
        raise InvalidClusterTypeOrSuperregionError(e.response.text)


def get_kafka_cluster(cluster_type, client_id, cluster_name):
    """Get a :py:class:`yelp_kafka.config.ClusterConfig` for a given
    cluster-type and name.

    :param cluster_type: kafka cluster type (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: name of the client making the discovery request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param cluster_name: name of the cluster (ex.'uswest1-devc').
    :type cluster_name: string
    :returns: :py:class:`yelp_kafka.config.ClusterConfig`
    """
    client = get_kafka_discovery_client(client_id)
    try:
        result = client.v1.getClustersWithName(
            type=cluster_type,
            kafka_cluster_name=cluster_name,
        ).result()
        return parse_as_cluster_config(result)
    except HTTPError as e:
        log.exception(
            "Failure while fetching kafka cluster for cluster-type:{type}, cluster-name"
            ":{cluster_name}.".format(type=cluster_type, cluster_name=cluster_name),
        )
        raise InvalidClusterTypeOrNameError(e.response.text)


def stream_to_log_regex(stream):
    return '{stream}$'.format(stream=re.escape(stream))


def get_region_logs_stream(client_id, stream, region=None):
    """Get the kafka cluster logs for given region and stream.
    If no region is given, we default to local region.

    :param client_id: name of the client making the logs request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param region: region name defaulting to caller's local region.
    :type region: string
    :param stream: Log/stream name regex
    :type stream: string
    :returns: [([topics], cluster)]
    :raises InvalidLogOrRegionError: :py:class:`yelp_kafka.error.InvalidLogOrRegionError`
    upon failure fetching logs from region.
    """
    regex = stream_to_log_regex(stream)
    return get_region_logs_regex(client_id, regex, region)


def get_region_logs_regex(client_id, regex, region=None):
    """Get the kafka cluster logs for given region and log-regex.
    If no region is given, we default to local region.

    :param client_id: name of the client making the logs request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param region: region name defaulting to caller's local region.
    :type region: string
    :param regex: Log/stream name regex
    :type regex: string
    :returns: [([topics], cluster)]
    :raises InvalidLogOrRegionError: :py:class:`yelp_kafka.error.InvalidLogOrRegionError`
    upon failure fetching logs from region.
    """
    if not region:
        region = _get_local_region()

    client = get_kafka_discovery_client(client_id)
    try:
        result = client.v1.getLogsForRegionWithRegex(region=region, regex=regex).result()
        return parse_as_scribe_topics(result)
    except HTTPError as e:
        log.exception(
            "Failure while fetching logs for region:{region}".format(region=region),
        )
        raise InvalidLogOrRegionError(e.response.text)


def get_superregion_logs_stream(client_id, stream, superregion=None):
    """Get the kafka cluster logs for given superregion and stream.
    If no superregion is given, we default to local superregion.

    :param client_id: name of the client making the logs request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param superregion: superregion name defaulting to caller's local superregion.
    :type superregion: string
    :param stream: Log/stream name regex
    :type stream: string
    :returns: [([topics], cluster)]
    :raises InvalidLogOrSuperegionError: :py:class:`yelp_kafka.error.InvalidLogOrRegionError`
    upon failure fetching logs from superregion.
    """
    regex = stream_to_log_regex(stream)
    return get_superregion_logs_regex(client_id, regex, superregion)


def get_superregion_logs_regex(client_id, regex, superregion=None):
    """Get the kafka cluster logs for given superregion and topic-regex.
    If no superregion is given, we default to local superregion.

    :param client_id: name of the client making the logs request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :param superregion: superregion name defaulting to caller's local superregion.
    :type superregion: string
    :param regex: Log/stream name regex
    :type regex: string
    :returns: [([topics], cluster)]
    :raises InvalidLogOrSuperegionError: :py:class:`yelp_kafka.error.InvalidLogOrRegionError`
    upon failure fetching logs from superregion.
    """
    if not superregion:
        superregion = _get_local_superregion()

    client = get_kafka_discovery_client(client_id)
    try:
        result = client.v1.getLogsForSuperregionWithRegex(
            superregion=superregion,
            regex=regex,
        ).result()
        return parse_as_scribe_topics(result)
    except HTTPError as e:
        log.exception(
            "Failure while fetching logs for superregion:{superregion}"
            .format(superregion=superregion),
        )
        raise InvalidLogOrSuperregionError(e.response.text)


def get_all_clusters(cluster_type, client_id):
    """Get a list of (cluster_name, cluster_config)
    for the available kafka clusters in the ecosystem.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: name of the client making the request. Usually
        the same client id used to create the Kafka connection.
    :type client_id: string
    :returns: list of py:class:`yelp_kafka.config.ClusterConfig`
    """
    client = get_kafka_discovery_client(client_id)
    try:
        cluster_names = client.v1.getClustersAll(cluster_type).result()
    except HTTPError as e:
        log.exception(
            "Failure while fetching clusters for cluster type:{clustertype}"
            .format(clustertype=cluster_type),
        )
        raise InvalidClusterType(e.response.text)
    return [
        get_kafka_cluster(cluster_type, client_id, cluster_name)
        for cluster_name in cluster_names
    ]


def get_consumer_config(cluster_type, group_id, **extra):
    """Get a :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    for the local region kafka cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param group_id: consumer group id
    :type group_id: string
    :param extra: extra arguments to use for creating the configuration
    :returns: :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    """
    cluster = get_region_cluster(cluster_type, group_id)
    return KafkaConsumerConfig(group_id=group_id, cluster=cluster, **extra)


def get_kafka_connection(cluster_type, client_id, **kwargs):
    """Get a kafka connection for the local region kafka cluster.

        :param cluster_type: kafka cluster type (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param client_id: client_id to be used to connect to kafka.
    :type client_id: string
    :param kwargs: parameters to pass along when creating the KafkaClient instance.
    :returns: KafkaClient
    :raises DiscoveryError: :py:class:`yelp_kafka.error.DiscoveryError` upon failure connecting to a cluster.
    """
    cluster = get_region_cluster(cluster_type, client_id)
    try:
        return KafkaClient(cluster.broker_list, client_id=client_id, **kwargs)
    except:
        log.exception(
            "Connection to kafka cluster %s using broker list %s failed",
            cluster.name,
            cluster.broker_list
        )
        raise DiscoveryError("Failed to connect to cluster {0}".format(
            cluster.name))


def get_all_kafka_connections(cluster_type, client_id, **kwargs):
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

    clusters = get_all_clusters(cluster_type, client_id)
    connected_clusters = []
    for cluster in clusters:
        try:
            client = KafkaClient(cluster.broker_list, client_id=client_id, **kwargs)
            connected_clusters.append((cluster.name, client))
        except:
            log.exception(
                "Connection to kafka cluster %s using broker list %s failed",
                cluster.name,
                cluster.broker_list
            )
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
        topics = get_kafka_topics(client)
        return dict([(topic.decode(), partitions) for topic, partitions in six.iteritems(topics)])
    except:
        log.exception(
            "Topics discovery failed for %s",
            cluster.broker_list
        )
        raise DiscoveryError("Failed to get topics information from "
                             "{cluster}".format(cluster=cluster))


def search_topic(topic, clusters=None):
    """Find the topic in the list of clusters or the local region cluster

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
    """Search for a topic in the local region kafka cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param topic: topic name
    :type topic: string
    :returns: True is the topic exists or False
    """
    cluster = get_region_cluster(cluster_type, DEFAULT_CLIENT_ID)
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
    clusters = get_all_clusters(cluster_type, DEFAULT_CLIENT_ID)
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
        valid_topics = [topic for topic in six.iterkeys(topics)
                        if re.match(pattern, topic)]
        if valid_topics:
            matches.append((valid_topics, cluster))
    return matches


def search_topics_by_regex_in_all_clusters(cluster_type, pattern):
    """Search for all the topics matching pattern.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param pattern: regex to match topics
    :returns: a list of tuples ([topics], cluster).
    :raises DiscoveryError: if the topic does not exist
    """
    clusters = get_all_clusters(cluster_type, DEFAULT_CLIENT_ID)
    results = search_topics_by_regex(pattern, clusters)
    if not results:
        raise DiscoveryError(
            "No Kafka topics for pattern {pattern}".format(
                pattern=pattern
            )
        )
    return results
