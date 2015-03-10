import contextlib
import mock
import pytest
from StringIO import StringIO

from yelp_kafka.config import TopologyConfiguration
from yelp_kafka.config import load_yaml_config
from yelp_kafka.error import ConfigurationError

TEST_BASE_KAFKA = '/base/kafka_discovery'

MOCK_TOPOLOGY_CONFIG = """
---
  clusters:
    cluster1:
      broker_list:
        - "mybrokerhost1:9092"
      zookeeper: "0.1.2.3,0.2.3.4/kafka"
    cluster2:
      broker_list:
        - "mybrokerhost2:9092"
      zookeeper: "0.3.4.5,0.4.5.6/kafka"
  local_config:
    cluster: cluster1
    prefix: my.prefix.
"""

MOCK_SCRIBE_YAML = {
    'clusters': {
        'cluster1': {
            'broker_list': ["mybrokerhost1:9092"],
            'zookeeper': "0.1.2.3,0.2.3.4/kafka"
        },
        'cluster2': {
            'broker_list': ["mybrokerhost2:9092"],
            'zookeeper': "0.3.4.5,0.4.5.6/kafka"
        }
    },
    'local_config': {
        'cluster': 'cluster1',
        'prefix': 'my.prefix.'
    }
}

MOCK_NO_SCRIBE_YAML = {
    'clusters': {
        'cluster1': {
            'broker_list': ["mybrokerhost1:9092"],
            'zookeeper': "0.1.2.3,0.2.3.4/kafka"
        },
        'cluster2': {
            'broker_list': ["mybrokerhost2:9092"],
            'zookeeper': "0.3.4.5,0.4.5.6/kafka"
        }
    },
    'local_config': {
        'cluster': 'cluster1',
    }
}


@pytest.yield_fixture
def mock_yaml():
    with contextlib.nested(
        mock.patch('yelp_kafka.config.load_yaml_config',
                   return_value=MOCK_SCRIBE_YAML,
                   create=True),
        mock.patch('os.path.isfile', return_value=True)
    ) as (m, mock_isfile):
        yield m


def test_load_yaml():
    stio = StringIO()
    stio.write(MOCK_TOPOLOGY_CONFIG)
    stio.seek(0)
    with mock.patch('__builtin__.open',
                    return_value=contextlib.closing(stio)) as mock_open:
        actual = load_yaml_config('test')
        mock_open.assert_called_once_with("test", "r")
        assert actual == MOCK_SCRIBE_YAML


class TestTopologyConfig(object):

    def test_missing_cluster(self):
        with pytest.raises(ConfigurationError):
            with mock.patch("os.path.isfile", return_value=False):
                TopologyConfiguration(
                    kafka_id="wrong_cluster",
                    kafka_topology_path=TEST_BASE_KAFKA
                )

    def test_get_local_cluster(self, mock_yaml):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        mock_yaml.assert_called_once_with('/base/kafka_discovery/mykafka.yaml')
        actual_cluster = topology.get_local_cluster()
        expected_cluster = ('cluster1', {
            'broker_list': ['mybrokerhost1:9092'],
            'zookeeper': '0.1.2.3,0.2.3.4/kafka'
        })
        assert actual_cluster == expected_cluster

    def test_get_local_cluster_error(self, mock_yaml):
        # Should raise ConfigurationError if a cluster is in region but not in
        # the cluster list
            mock_yaml.return_value = {
                'clusters': {
                    'cluster1': {
                        'broker_list': ['mybroker'],
                        'zookeeper': '0.1.2.3,0.2.3.4/kafka'
                    },
                },
                'local_config': {
                    'cluster': 'cluster3'
                }
            }
            topology = TopologyConfiguration(
                kafka_id='mykafka',
                kafka_topology_path=TEST_BASE_KAFKA,
            )
            # Raise ConfigurationError because cluster 3 does not exist
            with pytest.raises(ConfigurationError):
                topology.get_local_cluster()

    def test_get_scribe_prefix(self, mock_yaml):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        assert 'my.prefix.' == topology.get_scribe_local_prefix()

    def test_get_scribe_prefix_None(self, mock_yaml):
        mock_yaml.return_value = MOCK_NO_SCRIBE_YAML
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        assert not topology.get_scribe_local_prefix()

    def test_get_all_clusters(self, mock_yaml):
        topology = TopologyConfiguration(
            kafka_id='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        actual_clusters = topology.get_all_clusters()
        expected_clusters = [
            ('cluster1', {
                'broker_list': ["mybrokerhost1:9092"],
                'zookeeper': "0.1.2.3,0.2.3.4/kafka"
            }),
            ('cluster2', {
                'broker_list': ["mybrokerhost2:9092"],
                'zookeeper': "0.3.4.5,0.4.5.6/kafka"
            })
        ]
        assert sorted(expected_clusters) == sorted(actual_clusters)
