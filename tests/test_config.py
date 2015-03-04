import mock
import pytest

from tests.mock_config import TEST_BASE_ZK
from tests.mock_config import TEST_BASE_KAFKA

from yelp_kafka.config import TopologyConfiguration
from yelp_kafka.config import ClusterConfig
from yelp_kafka.error import ConfigurationError


class TestTopologyConfig(object):

    def clusters_equal(self, expected, actual):
        return (
            expected.name == actual.name and
            expected.broker_list == actual.broker_list and
            expected.zookeeper_cluster == actual.zookeeper_cluster and
            expected.zookeeper_topology_path == actual.zookeeper_topology_path
        )

    def test_missing_cluster(self, mock_files):
        with pytest.raises(ConfigurationError):
            TopologyConfiguration(
                kafka_id="wrong_cluster",
                kafka_topology_path=TEST_BASE_KAFKA,
                zk_topology_path=TEST_BASE_ZK
            )

    def test_get_cluster_for_region(self, mock_files):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
            zk_topology_path=TEST_BASE_ZK
        )
        actual_clusters = topology.get_clusters_for_region('sfo12-prod')
        expected_clusters = [
            ClusterConfig(
                name='cluster1',
                broker_list=['mybrokerhost1:9092'],
                zookeeper_cluster='myzookeepercluster1',
                zookeeper_topology_path=TEST_BASE_ZK
            ),
            ClusterConfig(
                name='cluster3',
                broker_list=['mybrokerhost3:9092', 'mybrokerhost4:9092'],
                zookeeper_cluster='myzookeepercluster1',
                zookeeper_topology_path=TEST_BASE_ZK
            )
        ]
        assert all(map(self.clusters_equal, expected_clusters, actual_clusters))

    @mock.patch("yelp_kafka.config.os.path.isfile", lambda x: True)
    def test_get_cluster_for_region_error(self):
        # Should raise ConfigurationError if a cluster is in region but not in
        # the cluster list
        with mock.patch("yelp_kafka.config.load_yaml_config",
                        autospec=True) as mock_config:
            mock_config.return_value = {
                'clusters': {
                    'cluster1': {
                        'broker_list': ['mybroker'],
                        'zookeeper_cluster': 'zk_cluster'
                    },
                },
                'region_to_cluster': {
                    'region1': ['cluster2']
                }
            }
            topology = TopologyConfiguration(
                kafka_id='mykafka',
                kafka_topology_path=TEST_BASE_KAFKA,
                zk_topology_path=TEST_BASE_ZK
            )
            # Raise ConfigurationError because cluster 2 does not exist
            with pytest.raises(ConfigurationError):
                topology.get_clusters_for_region("region1")

    def test_get_regions_for_cluster(self, mock_files):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
            zk_topology_path=TEST_BASE_ZK
        )
        actual = topology.get_regions_for_cluster('cluster1')
        expected = ['dc6-prod', 'sfo12-prod']
        assert sorted(actual) == sorted(expected)

    def test_get_regions_for_cluster_error(self, mock_files):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
            zk_topology_path=TEST_BASE_ZK
        )
        with pytest.raises(ConfigurationError):
            topology.get_regions_for_cluster('wrong_cluster')

    def test_get_cluster_for_ecosystem(self, mock_files):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
            zk_topology_path=TEST_BASE_ZK
        )
        actual_clusters = topology.get_clusters_for_ecosystem('devc')
        expected_clusters = [
            ClusterConfig(
                name='cluster2',
                broker_list=['mybrokerhost2:9092'],
                zookeeper_cluster='myzookeepercluster2',
                zookeeper_topology_path=TEST_BASE_ZK
            ),
            ClusterConfig(
                name='cluster4',
                broker_list=['mybrokerhost5:9092'],
                zookeeper_cluster='myzookeepercluster3',
                zookeeper_topology_path=TEST_BASE_ZK
            )
        ]
        assert all(map(self.clusters_equal, expected_clusters, actual_clusters))

    @mock.patch("yelp_kafka.config.os.path.isfile", lambda x: True)
    def test_get_all_clusters(self):
        with mock.patch("yelp_kafka.config.load_yaml_config",
                        autospec=True) as mock_config:
            mock_config.return_value = {
                'clusters': {
                    'cluster1': {
                        'broker_list': ['mybroker'],
                        'zookeeper_cluster': 'zk_cluster'
                    },
                    'cluster2': {
                        'broker_list': ['mybroker2'],
                        'zookeeper_cluster': 'zk_cluster2'
                    }
                },
                'region_to_cluster': {
                    'region1': ['cluster1'],
                    'region2': ['cluster2'],
                }
            }
            topology = TopologyConfiguration(
                kafka_id='mykafka',
                kafka_topology_path=TEST_BASE_KAFKA,
                zk_topology_path=TEST_BASE_ZK
            )
            actual_clusters = topology.get_all_clusters()
            expected_clusters = [
                ('region1', [
                    ClusterConfig(
                        name='cluster1',
                        broker_list=['mybroker'],
                        zookeeper_cluster='zk_cluster',
                        zookeeper_topology_path=TEST_BASE_ZK
                    )
                ]),
                ('region2', [
                    ClusterConfig(
                        name='cluster2',
                        broker_list=['mybroker2'],
                        zookeeper_cluster='zk_cluster2',
                        zookeeper_topology_path=TEST_BASE_ZK
                    )
                ])
            ]
            for actual, expected in zip(sorted(actual_clusters), sorted(expected_clusters)):
                assert actual[0] == expected[0]
                assert all(map(self.clusters_equal, sorted(expected[1]), sorted(actual[1])))


class TestClusterConfig(object):

    def test_zookeeper_hosts(self, mock_files):
        cluster = ClusterConfig('mycluster', ['mybroker:9092'], 'myzookeepercluster1',
                                TEST_BASE_ZK)
        assert cluster.zookeeper_hosts == ["0.1.2.3:2181", "0.2.3.4:2181"]

    def test_zookeeper_hosts_error(self, mock_files):
        cluster = ClusterConfig('mycluster', ['mybroker:9092'], 'wrong_cluster',
                                TEST_BASE_ZK)
        with pytest.raises(ConfigurationError):
            cluster.zookeeper_hosts
