import pytest
import mock

from yelp_kafka import discovery
from yelp_kafka.config import ClusterConfig
from yelp_kafka.error import DiscoveryError


@pytest.fixture
def mock_clusters():
    cluster1 = mock.Mock(broker_list=['mybroker'])
    cluster1.name = 'cluster1'
    cluster2 = mock.Mock(broker_list=['mybroker2'])
    cluster2.name = 'cluster2'
    return [cluster1, cluster2]


def test_get_local_region(mock_files):
    assert discovery.get_local_region() == 'my-ecosystem'


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
    get_cluster.assert_called_once_with(region='my-ecosystem')
    assert clusters == mock.sentinel.clusters


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_cluster_config_region(mock_topology, mock_files):
    get_cluster = mock_topology.return_value.get_clusters_for_region
    get_cluster.return_value = mock.sentinel.clusters
    clusters = discovery.get_clusters_config("mycluster", 'my-ecosystem2')
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    get_cluster.assert_called_once_with(region='my-ecosystem2')
    assert clusters == mock.sentinel.clusters


@mock.patch("yelp_kafka.discovery.TopologyConfiguration", autospec=True)
def test_get_cluster_config_ecosystem(mock_topology, mock_files):
    get_cluster_eco = mock_topology.return_value.get_clusters_for_ecosystem
    get_cluster_eco.return_value = mock.sentinel.clusters
    clusters = discovery.get_clusters_config_in_ecosystem("mycluster")
    mock_topology.assert_called_once_with(kafka_id='mycluster')
    # 'ecosystem' is part of MOCK_REGION defined in mock_config
    get_cluster_eco.assert_called_once_with('ecosystem')
    assert clusters == mock.sentinel.clusters


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_clusters_broker_list(mock_get_clusters, mock_clusters):
    mock_get_clusters.return_value = mock_clusters
    clusters = discovery.get_clusters_broker_list("myclustertype")
    mock_get_clusters.assert_called_once_with('myclustertype', None)
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
def test_get_yelp_kafka_config(mock_get_clusters, mock_clusters):
    mock_get_clusters.return_value = mock_clusters
    with mock.patch("yelp_kafka.discovery.YelpKafkaConfig",
                    autospec=True) as mock_config:
        mock_config.return_value = mock.sentinel.kafka_config
        actual = discovery.get_yelp_kafka_config(
            "mycluster", group_id='mygroup', auto_offset_reset='largest')
        assert mock_config.call_args_list == [
            mock.call(cluster=mock_clusters[0], group_id='mygroup',
                      auto_offset_reset='largest'),
            mock.call(cluster=mock_clusters[1], group_id='mygroup',
                      auto_offset_reset='largest'),
        ]
        assert actual == [('cluster1', mock.sentinel.kafka_config),
                          ('cluster2', mock.sentinel.kafka_config)]


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_kafka_connection(mock_get_clusters, mock_clusters):
    mock_get_clusters.return_value = mock_clusters
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
def test_get_kafka_connection_error(mock_get_clusters, mock_clusters):
    mock_get_clusters.return_value = mock_clusters
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
        ClusterConfig(name='cluster1', broker_list=['mybroker'],
                      zookeeper_cluster='mycluster')
    )
    assert actual == expected


@mock.patch("yelp_kafka.discovery.get_kafka_topics", autospec=True)
def test_discover_topics_error(mock_topics):
    mock_topics.side_effect = Exception("Boom!")
    with pytest.raises(DiscoveryError):
        discovery.discover_topics(
            ClusterConfig(name='cluster1', broker_list=['mybroker'],
                          zookeeper_cluster='mycluster')
        )


def test_search_topic(mock_clusters):
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # topic1 is only in the first cluster
        actual = discovery.search_topic('topic1', mock_clusters)
        expected = [('topic1', mock_clusters[0])]
        assert expected == actual


def test_search_topic_in_2_clusters(mock_clusters):
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # topic1 is only in cluster1
        actual = discovery.search_topic('topic2', mock_clusters)
        expected = [('topic2', mock_clusters[0]),
                    ('topic2', mock_clusters[1])]
        assert expected == actual


def test_search_no_topic(mock_clusters):
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        actual = discovery.search_topic('topic3', mock_clusters)
        # Since the topic does not exist we expect an empty list
        assert [] == actual


@mock.patch("yelp_kafka.discovery.search_topic", autospec=True)
@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_topic_in_region(mock_get_clusters, mock_search):
    mock_get_clusters.return_value = mock.sentinel.clusters
    mock_search.return_value = mock.sentinel.topics
    actual = discovery.search_topic_in_region('mycluster', 'topic1', 'myregion')
    mock_search.assert_called_once_with('topic1', mock.sentinel.clusters)
    assert actual == mock.sentinel.topics


@mock.patch("yelp_kafka.discovery.search_topic", autospec=True)
@mock.patch("yelp_kafka.discovery.get_clusters_config_in_ecosystem",
            autospec=True)
def test_search_topic_in_ecosystem(mock_get_clusters, mock_search):
    mock_get_clusters.return_value = mock.sentinel.clusters
    mock_search.return_value = mock.sentinel.topics
    actual = discovery.search_topic_in_local_ecosystem(
        'mycluster', 'topic1'
    )
    mock_search.assert_called_once_with('topic1', mock.sentinel.clusters)
    mock_get_clusters.assert_called_once_with('mycluster')
    assert actual == mock.sentinel.topics


def test_search_by_regex(mock_clusters):
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # search for all topics starting with top
        actual = discovery.search_topics_by_regex('top.*', mock_clusters)
        expected = [(['topic1', 'topic2'], mock_clusters[0]),
                    (['topic2'], mock_clusters[1])]
        assert expected == actual


def test_search_by_regex_no_topic(mock_clusters):
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([
            {'topic1': [0, 1, 2], 'topic2': [0]},
            {'topic2': [0]}
        ])
        # search for all topics starting with top
        actual = discovery.search_topics_by_regex('notopic.*', mock_clusters)
        assert [] == actual


@mock.patch("yelp_kafka.discovery.search_topics_by_regex", autospec=True)
@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_search_topic_by_regex_in_region(mock_get_clusters, mock_search):
    mock_get_clusters.return_value = mock.sentinel.clusters
    mock_search.return_value = mock.sentinel.topics
    actual = discovery.search_topics_by_regex_in_region(
        'mycluster', 'topic1.*', 'myregion'
    )
    mock_search.assert_called_once_with('topic1.*', mock.sentinel.clusters)
    assert actual == mock.sentinel.topics


@mock.patch("yelp_kafka.discovery.search_topics_by_regex", autospec=True)
@mock.patch("yelp_kafka.discovery.get_clusters_config_in_ecosystem",
            autospec=True)
def test_search_topic_by_regex_in_ecosystem(mock_get_clusters, mock_search):
    mock_get_clusters.return_value = mock.sentinel.clusters
    mock_search.return_value = mock.sentinel.topics
    actual = discovery.search_topics_by_regex_in_local_ecosystem(
        'mycluster', 'topic1.*'
    )
    mock_search.assert_called_once_with('topic1.*', mock.sentinel.clusters)
    mock_get_clusters.assert_called_once_with('mycluster')
    assert actual == mock.sentinel.topics


@mock.patch("yelp_kafka.discovery.search_topic_in_region", autospec=True)
def test_get_scribe_topic_in_datacenter(mock_search):
    mock_search.return_value = mock.sentinel.topics
    actual = discovery.get_scribe_topic_in_datacenter(
        'ranger', 'sfo2', cluster_type='mycluster', region='my-ecosystem'
    )
    assert actual == mock.sentinel.topics
    mock_search.assert_called_once_with('mycluster',
                                        'scribe.sfo2.ranger',
                                        'my-ecosystem')


@mock.patch("yelp_kafka.discovery.get_clusters_config", autospec=True)
def test_get_scribe_topic(mock_get_clusters, mock_clusters):
    mock_get_clusters.return_value = mock_clusters
    with mock.patch("yelp_kafka.discovery.discover_topics",
                    autospec=True) as mock_discover:
        mock_discover.side_effect = iter([{
            'scribe.dc1.my_scribe_stream2': [0, 1, 2],
            'scribe.dc1.my_scribe_stream': [0, 1, 2],
            'scribe.dc2.my_scribe_stream': [0]
        }, {
            'scribe.dc1.non_my_scribe_stream': [0, 1]
        }])
        actual = discovery.get_scribe_topics('my_scribe_stream')
    expected = [(['scribe.dc2.my_scribe_stream', 'scribe.dc1.my_scribe_stream'],
                 mock_clusters[0])]
    assert actual == expected
