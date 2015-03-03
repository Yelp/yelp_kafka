import logging
import os
import yaml

from kafka.consumer.base import AUTO_COMMIT_MSG_COUNT
from kafka.consumer.base import AUTO_COMMIT_INTERVAL
from kafka.consumer.base import FETCH_MIN_BYTES
from kafka.consumer.kafka import DEFAULT_CONSUMER_CONFIG

from yelp_kafka.error import ConfigurationError


DEFAULT_KAFKA_TOPOLOGY_BASE_PATH = '/nail/etc/kafka_discovery'
DEFAULT_ZK_TOPOLOGY_BASE_PATH = '/nail/etc/zookeeper_discovery/generic'


cluster_configuration = {}


def load_yaml_config(config_path):
    with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)


class TopologyConfiguration(object):
    """Topology configuration for a kafka cluster.
    A topology configuration represents a kafka cluster
    in all the available regions at Yelp.

    :param kafka_id: kafka cluster id. Ex. standard or scribe
    :type kafka_id: string
    :param kafka_topology_path: path of the directory containing
        the kafka topology.yaml config
    :type kafka_topology_path: string
    :param zk_topology_path: path of the directory containing
        the zookeeper topology.yaml
    :type zk_topology_path: string
    """

    def __init__(self, kafka_id,
                 kafka_topology_path=DEFAULT_KAFKA_TOPOLOGY_BASE_PATH,
                 zk_topology_path=DEFAULT_ZK_TOPOLOGY_BASE_PATH):
        self.kafka_topology_path = kafka_topology_path
        self.zk_topology_path = zk_topology_path
        self.kafka_id = kafka_id
        self.log = logging.getLogger(self.__class__.__name__)
        self.clusters = None
        self.region_to_cluster = None
        self.load_topology_config()

    def load_topology_config(self):
        """Load the topology configuration"""
        config_path = os.path.join(self.kafka_topology_path,
                                   '{id}.yaml'.format(id=self.kafka_id))
        self.log.debug("Loading configuration from %s", config_path)
        if os.path.isfile(config_path):
            topology_config = load_yaml_config(config_path)
        else:
            raise ConfigurationError(
                "Topology configuration {0} for cluster {1} "
                "does not exist".format(
                    config_path, self.kafka_id
                )
            )
        self.log.debug("Topology configuration %s", topology_config)
        try:
            self.clusters = dict([(name, ClusterConfig(**config)) for name, config
                                  in topology_config['clusters'].iteritems()])
            for name, config in topology_config['clusters'].iteritems():
                self.clusters[name] = ClusterConfig(
                    name,
                    zookeeper_topology_path=self.zk_topology_path,
                    **config
                )
            self.region_to_cluster = topology_config['region_to_cluster']
        except KeyError:
            self.log.exception("Invalid topology file")
            raise ConfigurationError("Invalid topology file {0}".format(
                config_path))

    def get_clusters_for_region(self, region):
        clusters = []
        if region not in self.region_to_cluster:
            raise ConfigurationError("Region {0} not in config".format(region))
        for cluster in self.region_to_cluster[region]:
            if cluster not in self.clusters:
                raise ConfigurationError(
                    "Mismatching configuration, {0} in region {1} is "
                    "not a valid cluster".format(cluster, region)
                )
            clusters.append(self.clusters[cluster])
        return clusters

    def get_all_clusters(self):
        return [(region, self.get_clusters_for_region(region))
                for region in self.region_to_cluster.iterkeys()]

    def get_regions_for_cluster(self, cluster):
        """Reverse lookup from cluster to regions"""
        if cluster not in self.clusters:
            raise ConfigurationError("Cluster {0} not in configuration".format(cluster))
        return [region for region, clusters in self.region_to_cluster.iteritems()
                if cluster in clusters]

    def get_clusters_for_ecosystem(self, ecosystem):
        regions = [region for region in self.region_to_cluster
                   if region.endswith(ecosystem)]
        return set([cluster for region in regions
                    for cluster in self.get_clusters_for_region(region)])

    def __repr__(self):
        return ("TopologyConfig: kafka_id {0}, clusters: {1},"
                "regions_to_cluster {2}".format(
                    self.kafka_id,
                    self.clusters,
                    self.region_to_cluster
                ))


class ClusterConfig(object):

    def __init__(self, name,
                 broker_list,
                 zookeeper_cluster,
                 zookeeper_topology_path=DEFAULT_ZK_TOPOLOGY_BASE_PATH,
                 ):
        self.zookeeper_cluster = zookeeper_cluster
        self.name = name
        self.broker_list = broker_list
        self.zookeeper_topology_path = zookeeper_topology_path
        self.log = logging.getLogger(self.__class__.__name__)
        self._zookeeper_hosts = None

    @property
    def zookeeper_hosts(self):
        if self._zookeeper_hosts is None:
            # Lazy loading, most of the consumers don't need to access
            # zookeeeper directly
            self._zookeeper_hosts = self._load_zookeeper_topology()
        return self._zookeeper_hosts

    def _load_zookeeper_topology(self):
        topology_file = os.path.join(self.zookeeper_topology_path,
                                     "{zk}.yaml".format(zk=self.zookeeper_cluster))
        if os.path.isfile(topology_file):
            zk_config = load_yaml_config(topology_file)
            # zk_config is a list of lists we create a list of tuples(host,
            # port)
            return ["{host}:{port}".format(host=host_port[0], port=host_port[1])
                    for host_port in zk_config]
        else:
            raise ConfigurationError("Topology config {0} for zookeeper cluster {1} "
                                     "does not exist".format(
                                         topology_file, self.zookeeper_cluster
                                     ))

    def __repr__(self):
        return ("ClusterConfig {0}, zookeeper_cluster {1}"
                "zookeeper_topology_path {2}".format(
                    self.broker_list, self.zookeeper_cluster,
                    self.zookeeper_topology_path
                ))


class YelpKafkaConfig(object):
    """Config class for ConsumerGroup, MultiprocessingConsumerGroup,
    KafkaSimpleConsumer and KakfaConsumerBase"""

    # This is fixed to 1 MB for making a fetch call more efficient when dealing
    # with ranger messages, that can be more than 100KB in size
    KAFKA_BUFFER_SIZE = 1024 * 1024  # 1MB

    ZOOKEEPER_BASE_PATH = '/python-kafka'
    PARTITIONER_COOLDOWN = 30
    MAX_TERMINATION_TIMEOUT_SECS = 10
    MAX_ITERATOR_TIMEOUT_SECS = 0.1
    DEFAULT_OFFSET_RESET = 'largest'
    DEFAULT_CLIENT_ID = 'yelp-kafka'

    SIMPLE_CONSUMER_DEFAULT_CONFIG = {
        'buffer_size': KAFKA_BUFFER_SIZE,
        'auto_commit_every_n': AUTO_COMMIT_MSG_COUNT,
        'auto_commit_every_t': AUTO_COMMIT_INTERVAL,
        'auto_commit': True,
        'fetch_size_bytes': FETCH_MIN_BYTES,
        'max_buffer_size': None,
        'iter_timeout': MAX_ITERATOR_TIMEOUT_SECS,
    }
    """Default SimpleConsumer configuration"""

    def __init__(self, group_id, cluster, **config):
        self._config = config
        self.cluster = cluster
        self.group_id = group_id

    def get_simple_consumer_args(self):
        args = {}
        for key in self.SIMPLE_CONSUMER_DEFAULT_CONFIG.iterkeys():
            args[key] = self._config.get(
                key, self.SIMPLE_CONSUMER_DEFAULT_CONFIG[key]
            )
        args['group'] = self.group_id
        return args

    def get_kafka_consumer_config(self):
        config = {}
        for key in DEFAULT_CONSUMER_CONFIG.iterkeys():
            config[key] = self._config.get(
                key, DEFAULT_CONSUMER_CONFIG[key]
            )
        config['group_id'] = self.group_id
        config['metadata_broker_list'] = self.cluster.broker_list
        return config

    @property
    def zookeeper_base(self):
        return self._config.get('zookeeper_base', self.ZOOKEEPER_BASE_PATH)

    @property
    def partitioner_cooldown(self):
        return self._config.get('partitioner_cooldown', self.PARTITIONER_COOLDOWN)

    @property
    def max_termination_timeout_secs(self):
        return self._config.get('max_termination_timeout_secs',
                                self.MAX_ITERATOR_TIMEOUT_SECS)

    @property
    def auto_offset_reset(self):
        return self._config.get('auto_offset_reset', self.DEFAULT_OFFSET_RESET)

    @property
    def client_id(self):
        return self._config.get('client_id', self.DEFAULT_CLIENT_ID)

    def __repr__(self):
        return ("YelpKafkaConfig: cluster {cluster}, group {group} "
                "config: {config}".format(cluster=self.cluster,
                                          group=self.group_id,
                                          config=self._config))
