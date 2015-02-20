import pytest
import mock

from yelp_kafka import discovery
from yelp_kafka.error import DiscoveryError
from yelp_kafka.config import ClusterConfig


def test_get_local_region(mock_files):
    assert discovery.get_local_region() == 'myregion'


@mock.patch("yelp_kafka.discovery.os.path.exists", return_value=False)
def test_get_local_region_error(_):
    with pytest.raises(DiscoveryError):
        discovery.get_local_region()


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_cluster_config(mock_topology, mock_files):
    get_cluster = mock_topology.return_value.get_clusters_for_region
    get_cluster.return_value = mock.sentinel.clusters
    clusters = discovery.get_clusters_config("mycluster")
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    get_cluster.assert_called_once_with(region='myregion')
    assert clusters == mock.sentinel.clusters


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_cluster_config_region(mock_topology, mock_files):
    get_cluster = mock_topology.return_value.get_clusters_for_region
    get_cluster.return_value = mock.sentinel.clusters
    clusters = discovery.get_clusters_config("mycluster", 'myregion2')
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    get_cluster.assert_called_once_with(region='myregion2')
    assert clusters == mock.sentinel.clusters


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_clusters_broker_list(mock_topology, mock_files):
    get_cluster = mock_topology.return_value.get_clusters_for_region
    get_cluster.return_value = [
        ('cluster1', mock.Mock(broker_list=['mybroker'])),
        ('cluster2', mock.Mock(broker_list=['mybroker2']))
    ]
    clusters = discovery.get_clusters_broker_list("mycluster")
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    get_cluster.assert_called_once_with(region='myregion')
    assert clusters == [('cluster1', ['mybroker']),
                        ('cluster2', ['mybroker2'])]


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_clusters_broker_list_region(mock_topology, mock_files):
    get_cluster = mock_topology.return_value.get_clusters_for_region
    get_cluster.return_value = [
        ('cluster1', mock.Mock(broker_list=['mybroker'])),
        ('cluster2', mock.Mock(broker_list=['mybroker2']))
    ]
    clusters = discovery.get_clusters_broker_list("mycluster", "myregion2")
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    get_cluster.assert_called_once_with(region='myregion2')
    assert clusters == [('cluster1', ['mybroker']),
                        ('cluster2', ['mybroker2'])]


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_all_clusters(mock_topology, mock_files):
    get_cluster = mock_topology.return_value.get_all_clusters
    get_cluster.return_value = mock.sentinel.clusters
    clusters = discovery.get_all_clusters_config("mycluster")
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    get_cluster.assert_called_once_with()
    assert clusters == mock.sentinel.clusters


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_yelp_kafka_config(mock_clusters):
    mock_cluster1 = mock.Mock(broker_list=['mybroker'])
    mock_cluster2 = mock.Mock(broker_list=['mybroker2'])
    mock_clusters.return_value = [
        ('cluster1', mock_cluster1),
        ('cluster2', mock_cluster2)
    ]
    with mock.patch("yelp_kafka.discovery.YelpKafkaConfig",
                    autospec=True) as mock_config:
        mock_config.return_value = mock.sentinel.kafka_config
        actual = discovery.get_yelp_kafka_config(
            "mycluster", group_id='mygroup', auto_offset_reset='largest')
        assert mock_config.call_args_list == [
            mock.call(cluster=mock_cluster1, group_id='mygroup',
                      auto_offset_reset='largest'),
            mock.call(cluster=mock_cluster2, group_id='mygroup',
                      auto_offset_reset='largest'),
        ]
        assert actual == [('cluster1', mock.sentinel.kafka_config),
                          ('cluster2', mock.sentinel.kafka_config)]


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_kafka_connection(mock_clusters):
    mock_clusters.return_value = [
        ('cluster1', mock.Mock(broker_list=['mybroker'])),
        ('cluster2', mock.Mock(broker_list=['mybroker2']))
    ]
    with mock.patch("yelp_kafka.discovery.KafkaClient",
                    autospec=True) as mock_kafka:
        mock_kafka.return_value = mock.sentinel.kafkaclient
        actual = discovery.get_kafka_connection("mycluster")
        assert mock_kafka.call_args_list == [
            mock.call(['mybroker'], client_id='yelp-kafka'),
            mock.call(['mybroker2'], client_id='yelp-kafka')
        ]
        assert actual == [('cluster1', mock.sentinel.kafkaclient),
                          ('cluster2', mock.sentinel.kafkaclient)]


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_kafka_connection_error(mock_clusters, mock_files):
    mock_clusters.return_value = [
        ('cluster1', mock.Mock(broker_list=['mybroker'])),
        ('cluster2', mock.Mock(broker_list=['mybroker2']))
    ]
    with mock.patch("yelp_kafka.discovery.KafkaClient",
                    autospec=True) as mock_kafka:
        client = mock.MagicMock()
        mock_kafka.side_effect = [client, Exception("Boom!")]
        with pytest.raises(DiscoveryError):
            discovery.get_kafka_connection("mycluster")
        client.close.assert_called_once_with()


@mock.patch("yelp_kafka.discovery.get_kafka_topics", autospec=True)
def test_discover_topics(mock_topics):
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0]
    }
    mock_topics.return_value = expected
    actual = discovery.discover_topics(
        ClusterConfig(broker_list=['mybroker'],
                      zookeeper_cluster='mycluster')
    )
    assert actual == expected


@mock.patch("yelp_kafka.discovery.get_kafka_topics", autospec=True)
def test_discover_topics_error(mock_topics):
    mock_topics.side_effect = Exception("Boom!")
    with pytest.raises(DiscoveryError):
        discovery.discover_topics(
            ClusterConfig(broker_list=['mybroker'],
                          zookeeper_cluster='mycluster')
        )


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_topic(mock_clusters):
    config1 = ClusterConfig(broker_list=['mybroker'],
                            zookeeper_cluster='zkcluster')
    mock_clusters.return_value = [
        ('cluster1', config1),
        ('cluster2', mock.Mock(broker_list=['mybroker2']))
    ]
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # topic1 is only in cluster1
        actual = discovery.search_topic('mycluster', 'topic1')
        expected = [('topic1', 'cluster1', config1)]
        assert expected == actual


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_topic_in_2_clusters(mock_clusters):
    config1 = ClusterConfig(broker_list=['mybroker'],
                            zookeeper_cluster='zkcluster')
    config2 = ClusterConfig(broker_list=['mybroker2'],
                            zookeeper_cluster='zkcluster2')
    mock_clusters.return_value = [
        ('cluster1', config1),
        ('cluster2', config2)
    ]
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # topic1 is only in cluster1
        actual = discovery.search_topic('mycluster', 'topic2')
        expected = [('topic2', 'cluster1', config1),
                    ('topic2', 'cluster2', config2)]
        assert expected == actual


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_no_topic(mock_clusters):
    config1 = ClusterConfig(broker_list=['mybroker'],
                            zookeeper_cluster='zkcluster')
    mock_clusters.return_value = [
        ('cluster1', config1),
        ('cluster2', mock.Mock(broker_list=['mybroker2']))
    ]
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        actual = discovery.search_topic('mycluster', 'topic3')
        # Since the topic does not exist we expect an empty list
        expected = []
        assert expected == actual


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_by_regex(mock_clusters):
    config1 = ClusterConfig(broker_list=['mybroker'],
                            zookeeper_cluster='zkcluster')
    config2 = ClusterConfig(broker_list=['mybroker2'],
                            zookeeper_cluster='zkcluster2')
    mock_clusters.return_value = [
        ('cluster1', config1),
        ('cluster2', config2)
    ]
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # search for all topics starting with top
        actual = discovery.search_topic_by_regex('mycluster', 'top.*')
        expected = [(['topic1', 'topic2'], 'cluster1', config1),
                    (['topic2'], 'cluster2', config2)]
        assert expected == actual


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_by_regex_no_topic(mock_clusters):
    config1 = ClusterConfig(broker_list=['mybroker'],
                            zookeeper_cluster='zkcluster')
    config2 = ClusterConfig(broker_list=['mybroker2'],
                            zookeeper_cluster='zkcluster2')
    mock_clusters.return_value = [
        ('cluster1', config1),
        ('cluster2', config2)
    ]
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # search for all topics starting with top
        actual = discovery.search_topic_by_regex('mycluster', 'notopic.*')
        assert [] == actual


@mock.patch("yelp_kafka.discovery.search_topic", autospec=True)
def test_get_scribe_topic_in_datacenter(mock_search):
    mock_search.return_value = mock.sentinel.topics
    actual = discovery.get_scribe_topic_in_datacenter(
        'ranger', 'sfo2', cluster_type='mycluster', region='myregion'
    )
    assert actual == mock.sentinel.topics
    mock_search.assert_called_once_with('mycluster',
                                        'scribe.sfo2.ranger',
                                        'myregion')


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_scribe_topic(mock_clusters):
    config1 = ClusterConfig(broker_list=['mybroker'],
                            zookeeper_cluster='zkcluster')
    mock_clusters.return_value = [('cluster1', config1)]
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.return_value = {
            'scribe.dc1.my_scribe_stream': [0, 1, 2],
            'scribe.dc2.my_scribe_stream': [0]
        }
        actual = discovery.get_scribe_topics('my_scribe_stream')
    expected = [(['scribe.dc2.my_scribe_stream', 'scribe.dc1.my_scribe_stream'],
                 'cluster1', config1)]
    assert actual == expected
