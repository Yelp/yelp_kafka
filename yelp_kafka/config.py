# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from collections import namedtuple

import six
import yaml
from bravado.client import SwaggerClient
from bravado.fido_client import FidoClient
from bravado_decorators.retry import SmartStackClient
from bravado_decorators.retry import UserFacingRetryConfig
from kafka.consumer.base import FETCH_MIN_BYTES
from kafka.consumer.kafka import DEFAULT_CONSUMER_CONFIG
from kafka.util import kafka_bytestring
from yelp_lib.decorators import memoized

from yelp_kafka.error import ConfigurationError


DEFAULT_KAFKA_TOPOLOGY_BASE_PATH = '/nail/etc/kafka_discovery'

# This is fixed to 2MiB, which is twice as much as the max message size
# configured by default in our Kafka clusters.
MAX_MESSAGE_SIZE_BYTES = 2 * 1024 * 1024

ZOOKEEPER_BASE_PATH = '/yelp-kafka'
PARTITIONER_COOLDOWN = 30
MAX_TERMINATION_TIMEOUT_SECS = 10
MAX_ITERATOR_TIMEOUT_SECS = 0.1
DEFAULT_OFFSET_RESET = 'largest'
DEFAULT_OFFSET_STORAGE = 'zookeeper'
DEFAULT_CLIENT_ID = 'yelp-kafka'

# The default has been changed from 100 to None.
# https://github.com/Yelp/kafka-python/blob/master/kafka/consumer/base.py#L181
AUTO_COMMIT_MSG_COUNT = None
AUTO_COMMIT_INTERVAL_SECS = 1

DEFAULT_SIGNALFX_METRICS_INTERVAL = 60  # seconds
DEFAULT_KAFKA_DISCOVERY_SERVICE_PATH = '/nail/etc/services/services.yaml'

RESPONSE_TIMEOUT = 2.0  # Response timeout (2 sec) for kafka cluster-endpoints


@memoized
def get_kafka_discovery_client(client_name):
    """Create smartstack-client for kafka_discovery service."""
    # Default retry is 1 on response timeout
    retry_config = UserFacingRetryConfig(timeout=RESPONSE_TIMEOUT)
    swagger_url = get_swagger_url()
    swagger_client = SwaggerClient.from_url(
        swagger_url,
        FidoClient(),
    )
    return SmartStackClient(
        swagger_client,
        retry_config,
        client_name=client_name,
        service_name='kafka_discovery',
    )


class ClusterConfig(
    namedtuple(
        'ClusterConfig',
        ['type', 'name', 'broker_list', 'zookeeper'],
    ),
):
    """Cluster configuration.
    :param name: cluster name
    :param broker_list: list of kafka brokers
    :param zookeeper: zookeeper connection string
    """

    def __ne__(self, other):
        return self.__hash__() != other.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __hash__(self):
        if isinstance(self.broker_list, list):
            broker_list = self.broker_list
        else:
            broker_list = self.broker_list.split(',')
        zk_list = self.zookeeper.split(',')
        return hash((
            self.type,
            self.name,
            ",".join(sorted(filter(None, broker_list))),
            ",".join(sorted(filter(None, zk_list)))
        ))


def load_yaml_config(config_path):
    with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)


def get_swagger_url(service_path=DEFAULT_KAFKA_DISCOVERY_SERVICE_PATH):
    service_conf = load_yaml_config(service_path)
    host = service_conf['kafka_discovery.main']['host']
    port = service_conf['kafka_discovery.main']['port']
    return 'http://{0}:{1}/swagger.json'.format(host, port)


class TopologyConfiguration(object):
    """Topology configuration for a kafka cluster.
    A topology configuration represents a kafka cluster
    in all the available regions at Yelp.

    :param cluster_type: kafka cluster type. Ex. standard, scribe, etc.
    :type cluster_type: string
    :param kafka_topology_path: path of the directory containing
        the kafka topology.yaml config
    :type kafka_topology_path: string
    """

    def __init__(
        self,
        cluster_type,
        kafka_topology_path=DEFAULT_KAFKA_TOPOLOGY_BASE_PATH
    ):
        self.kafka_topology_path = kafka_topology_path
        self.cluster_type = cluster_type
        self.log = logging.getLogger(self.__class__.__name__)
        self.clusters = None
        self.local_config = None
        self.load_topology_config()

    def __eq__(self, other):
        if all([
            self.cluster_type == other.cluster_type,
            self.clusters == other.clusters,
            self.local_config == other.local_config,
        ]):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def load_topology_config(self):
        """Load the topology configuration"""
        config_path = os.path.join(
            self.kafka_topology_path,
            '{id}.yaml'.format(id=self.cluster_type)
        )
        self.log.debug("Loading configuration from %s", config_path)
        if os.path.isfile(config_path):
            topology_config = load_yaml_config(config_path)
        else:
            raise ConfigurationError(
                "Topology configuration {0} for cluster {1} "
                "does not exist".format(
                    config_path, self.cluster_type
                )
            )
        self.log.debug("Topology configuration %s", topology_config)
        try:
            self.clusters = topology_config['clusters']
            self.local_config = topology_config['local_config']
        except KeyError:
            self.log.exception("Invalid topology file")
            raise ConfigurationError("Invalid topology file {0}".format(
                config_path))

    def get_all_clusters(self):
        return [
            ClusterConfig(
                type=self.cluster_type,
                name=name,
                broker_list=cluster['broker_list'],
                zookeeper=cluster['zookeeper'],
            )
            for name, cluster in six.iteritems(self.clusters)
        ]

    def get_cluster_by_name(self, name):
        if name in self.clusters:
            cluster = self.clusters[name]
            return ClusterConfig(
                type=self.cluster_type,
                name=name,
                broker_list=cluster['broker_list'],
                zookeeper=cluster['zookeeper'],
            )
        raise ConfigurationError("No cluster with name: {0}".format(name))

    def get_local_cluster(self):
        try:
            if self.local_config:
                local_cluster = self.clusters[self.local_config['cluster']]
                return ClusterConfig(
                    type=self.cluster_type,
                    name=self.local_config['cluster'],
                    broker_list=local_cluster['broker_list'],
                    zookeeper=local_cluster['zookeeper'])
        except KeyError:
            self.log.exception("Invalid topology file")
            raise ConfigurationError("Invalid topology file.")

    def get_scribe_local_prefix(self):
        """We use prefix only in the scribe cluster."""
        return self.local_config.get('prefix')

    def __repr__(self):
        return ("TopologyConfig: cluster_type {0}, clusters: {1},"
                "local_config {2}".format(
                    self.cluster_type,
                    self.clusters,
                    self.local_config
                ))


class KafkaConsumerConfig(object):
    """Config class for KafkaConsumerGroup, ConsumerGroup,
    MultiprocessingConsumerGroup, KafkaSimpleConsumer and KakfaConsumerBase.

    :param group_id: group of the kafka consumer
    :param cluster: cluster config from :py:mod:`yelp_kafka.discovery`
    :param config: keyword arguments, configuration arguments from kafka-python
        SimpleConsumer are accepted.
        See valid keyword arguments in:
        http://kafka-python.readthedocs.org/en/latest/apidoc/kafka.consumer.html

        Yelp_kafka specific configuration arguments are:

        * **auto_offset_reset**: Used for offset validation.
          if 'largest' reset the offset to the latest available
          message (tail). If 'smallest' uses consumes from the
          earliest (head). Default: 'largest'.
        * **client_id**: client id to use on connection. Default: 'yelp-kafka'.
        * **partitioner_cooldown**: Waiting time for the consumer
          to acquire the partitions. Default: 30 seconds.
        * **use_group_sha**: Used by partitioner to establish group membership.
          When True the partitioner will use the topic list to represent group itself.
          Basically groups with the same name but subscribed to different topic
          lists will not coordinate with each other. If False, groups with the same
          name but a different topic list will coordinate with each other for
          consumption. NOTE: in this case some topics may not be assigned until all
          consumers of the group converge to the same topics list. Default: True.
        * **max_termination_timeout_secs**: Used by MultiprocessinConsumerGroup
          time to wait for a consumer to terminate. Default 10 secs.
        * **metrics_reporter**: Used by
          :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup` to send
          metrics data from kafka-python to SignalFx.
          Valid options are ``yelp_meteorite`` (which uses meteorite) and
          ``signalfx`` (deprecated).
        * **signalfx_dimensions**: Additional dimensions to send to SignalFx.
          Both 'signalfx' and 'yelp_meteorite' use this.
        * **signalfx_send_metrics_interval**: How often to send metrics to
          SignalFx. Only used if metrics_reporter is 'signalfx'.
        * **signalfx_token**: Authentication token to send to SignalFx. Only
          used if metrics_reporter is 'signalfx'.
        * **pre_rebalance_callback**: Optional callback which is passed a
          dict of topics/partitions which will be discarded in a repartition.
          This is called directly prior to the actual discarding of the topics.
          It's important to note this may be called multiple times in a single
          repartition, so any actions taken as a result must be idempotent. You
          are guaranteed that no messages will be consumed between this
          callback and the post_rebalance_callback. Currently this only
          applies to consumer groups.
        * **post_rebalance_callback**: Optional callback which is passed a
          dict of topics/partitions which were acquired in a repartition. You
          are guaranteed that no messages will be consumed between the
          pre_rebalance_callback and this callback. Currently this only
          applies to consumer groups.
        * **offset_storage**: Specifies the storage that will be used for the
          consumer offset. Valid values are 'zookeeper', 'kafka', and 'dual'.
          Kafka based storage (enabled with 'kafka' and 'dual') is only
          available from Kafka 0.9.

    Yelp_kafka overrides some kafka-python default settings:

    * **consumer_timeout_ms** is 0.1 seconds by default in yelp_kafka, while it
      is -1 (infinite) in kafka-python.
    * **fetch_message_max_bytes** is 2MB by default in yelp_kafka.
    * **auto_commit_interval_messages** is 100 for both
      :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup` and
      :py:class:`yelp_kafka.consumer_group.ConsumerGroup`.This means commit will
      happen only once every minute irrespective of number of messages in that second.
    * **auto_commit_interval_ms** is 1 seconds by default.
    """

    NOT_CONVERTIBLE = object()

    def identity(value):
        return value

    def seconds_to_ms(value):
        return value * 1000

    def ms_to_seconds(value):
        return value / 1000

    SIMPLE_FROM_KAFKA = {
        'auto_commit': ('auto_commit_enable', identity),
        'auto_commit_every_n': ('auto_commit_interval_messages', identity),
        'auto_commit_every_t': ('auto_commit_interval_ms', identity),
        'fetch_size_bytes': ('fetch_min_bytes', identity),
        'buffer_size': NOT_CONVERTIBLE,
        'max_buffer_size': ('fetch_message_max_bytes', identity),
        'iter_timeout': ('consumer_timeout_ms', ms_to_seconds),
        'auto_offset_reset': ('auto_offset_reset', identity),
        'offset_storage': ('offset_storage', identity),
    }

    KAFKA_FROM_SIMPLE = {
        'client_id': NOT_CONVERTIBLE,
        'fetch_message_max_bytes': ('max_buffer_size', identity),
        'fetch_min_bytes': ('fetch_size_bytes', identity),
        'fetch_wait_max_ms': NOT_CONVERTIBLE,
        'refresh_leader_backoff_ms': NOT_CONVERTIBLE,
        'socket_timeout_ms': NOT_CONVERTIBLE,
        'auto_offset_reset': ('auto_offset_reset', identity),
        'deserializer_class': NOT_CONVERTIBLE,
        'auto_commit_enable': ('auto_commit', identity),
        'auto_commit_interval_ms': ('auto_commit_every_t', identity),
        'auto_commit_interval_messages': ('auto_commit_every_n', identity),
        'consumer_timeout_ms': ('iter_timeout', seconds_to_ms),
        'offset_storage': ('offset_storage', identity),
    }

    # Do not modify SIMPLE_CONSUMER_DEFAULT_CONFIG without also changing
    # KAKFA_CONSUMER_DEFAULT_CONFIG
    SIMPLE_CONSUMER_DEFAULT_CONFIG = {
        'buffer_size': MAX_MESSAGE_SIZE_BYTES,
        'auto_commit_every_n': AUTO_COMMIT_MSG_COUNT,
        'auto_commit_every_t': AUTO_COMMIT_INTERVAL_SECS * 1000,
        'auto_commit': True,
        'fetch_size_bytes': FETCH_MIN_BYTES,
        'max_buffer_size': None,
        'iter_timeout': MAX_ITERATOR_TIMEOUT_SECS,
        'auto_offset_reset': DEFAULT_OFFSET_RESET,
        'offset_storage': DEFAULT_OFFSET_STORAGE,
    }
    """Default SimpleConsumer configuration"""

    KAFKA_CONSUMER_DEFAULT_CONFIG = {
        'auto_commit_interval_messages': AUTO_COMMIT_MSG_COUNT,
        'auto_commit_interval_ms': AUTO_COMMIT_INTERVAL_SECS * 1000,
        'auto_commit_enable': True,
        'fetch_min_bytes': FETCH_MIN_BYTES,
        'consumer_timeout_ms': seconds_to_ms(MAX_ITERATOR_TIMEOUT_SECS),
        'auto_offset_reset': DEFAULT_OFFSET_RESET,
        'fetch_message_max_bytes': MAX_MESSAGE_SIZE_BYTES,
        'offset_storage': DEFAULT_OFFSET_STORAGE,
    }
    """SIMPLE_CONSUMER_DEFAULT_CONFIG converted into a KafkaConsumer config"""

    def __init__(self, group_id, cluster, **config):
        self.log = logging.getLogger(self.__class__.__name__)
        self._config = config
        self.cluster = cluster
        self.group_id = kafka_bytestring(group_id)

    def __eq__(self, other):
        return all([
            self._config == other._config,
            self.cluster == other.cluster,
            self.group_id == other.group_id,
        ])

    def __ne__(self, other):
        return not self == other

    def get_simple_consumer_args(self):
        """Get the configuration args for kafka-python SimpleConsumer.
        Values used in the generated config are evaluated in the following order:

            1. User provided value for a valid SimpleConsumer config specified
               as keyword argument in KafkaConsumerConfig
            2. User provided value for a KafkaConsumer config specified
               as keyword argument in KafkaConsumerConfig.
            3. Default value specified in yelp-kafka
        """
        args = {}
        for key, default in six.iteritems(self.SIMPLE_CONSUMER_DEFAULT_CONFIG):
            if key in self._config:
                args[key] = self._config[key]
            else:
                conversion = self.SIMPLE_FROM_KAFKA[key]
                try:
                    (kafka_key, convert_fn) = conversion
                    args[key] = convert_fn(self._config[kafka_key])
                except (TypeError, KeyError):
                    # either the conversion or kafka_key doesn't exist
                    args[key] = default

        args['group'] = self.group_id
        return args

    def get_kafka_consumer_config(self):
        """Get the configuration for kafka-python KafkaConsumer.
        The generated config values come from user provided values and
        default values and are evaluated in the following order:

            1. User provided value for a valid KafkaConsumer config specified
               as keyword argument in KafkaConsumerConfig
            2. User provided value for a valid SimpleConsumer config specified
               as keyword argument in KafkaConsumerConfig.
            3. Default value specified in yelp-kafka for KafkaConsumer
            4. Default value specified in kafka-python

        .. note:: SimpleConsumer is considered deprecated and not all of its
                  options can be converted in KafkaConsumer.
        """
        config = {}
        for key, default in six.iteritems(DEFAULT_CONSUMER_CONFIG):
            if key in self._config:
                config[key] = self._config[key]
            else:
                try:
                    conversion = self.KAFKA_FROM_SIMPLE[key]
                    (simple_key, convert_fn) = conversion
                except (KeyError, TypeError):
                    # no conversion can be made
                    config[key] = default
                    continue

                if simple_key in self._config:
                    # the user has provided a key we can convert from
                    self.log.warning("%s is deprecated use %s instead.", simple_key, key)
                    config[key] = convert_fn(self._config[simple_key])
                elif key in self.KAFKA_CONSUMER_DEFAULT_CONFIG:
                    # use yelp-kafka's default
                    config[key] = self.KAFKA_CONSUMER_DEFAULT_CONFIG[key]
                else:
                    # use kafka-python's default
                    config[key] = default

        config['group_id'] = self.group_id
        config['bootstrap_servers'] = self.cluster.broker_list
        return config

    @property
    def broker_list(self):
        return self.cluster.broker_list

    @property
    def zookeeper(self):
        return self.cluster.zookeeper

    @property
    def use_group_sha(self):
        return self._config.get('use_group_sha', True)

    @property
    def group_path(self):
        return '{zk_base}/{group_id}'.format(
            zk_base=ZOOKEEPER_BASE_PATH,
            group_id=self.group_id.decode(),
        )

    @property
    def partitioner_cooldown(self):
        return self._config.get('partitioner_cooldown', PARTITIONER_COOLDOWN)

    @property
    def max_termination_timeout_secs(self):
        return self._config.get(
            'max_termination_timeout_secs',
            MAX_ITERATOR_TIMEOUT_SECS
        )

    @property
    def client_id(self):
        return self._config.get('client_id', DEFAULT_CLIENT_ID)

    @property
    def metrics_reporter(self):
        return self._config.get('metrics_reporter', None)

    @property
    def signalfx_dimensions(self):
        dimensions = self._config.get('signalfx_dimensions', {})
        dimensions.update({
            'group_id': self.group_id,
            'cluster_name': self.cluster.name,
            'cluster_type': self.cluster.type,
        })
        return dimensions

    @property
    def signalfx_send_metrics_interval(self):
        return self._config.get(
            'signalfx_send_metrics_interval',
            DEFAULT_SIGNALFX_METRICS_INTERVAL
        )

    @property
    def signalfx_token(self):
        return self._config.get('signalfx_token', None)

    @property
    def pre_rebalance_callback(self):
        return self._config.get('pre_rebalance_callback', None)

    @property
    def post_rebalance_callback(self):
        return self._config.get('post_rebalance_callback', None)

    @property
    def offset_storage(self):
        return self._config.get('offset_storage', DEFAULT_OFFSET_STORAGE)

    def __repr__(self):
        return (
            "KafkaConsumerConfig(group_id={group_id!r}, cluster={cluster!r}, "
            "{config})".format(
                group_id=self.group_id,
                cluster=self.cluster,
                config=", ".join(
                    [
                        "{key}={value!r}".format(key=key, value=value)
                        for key, value in six.iteritems(self._config)
                    ],
                ),
            )
        )
