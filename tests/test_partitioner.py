import contextlib
import mock
import pytest

from kafka.common import KafkaUnavailableError
from kazoo.recipe.partitioner import SetPartitioner
from kazoo.recipe.partitioner import PartitionState
from kazoo.protocol.states import KazooState

from yelp_kafka.partitioner import Partitioner


@pytest.fixture
def config():
    return {
        'brokers': 'test_broker:9292',
        'group_id': 'test_group_id',
        'zookeeper_base': '/base_path',
        'zk_hosts': ['zookeeper_uri1:2181', 'zookeeper_uri2:2181'],
        'zk_partitioner_cooldown': 0.5
    }


def get_partitioner_state(status):
    return {'state': status}


class TestPartitioner(object):

    topics = ["topic1", "topic2"]

    @pytest.fixture
    @mock.patch('yelp_kafka.partitioner.KazooClient', autospec=True)
    def partitioner(self, kazoo, config):
        return Partitioner(config, self.topics, mock.Mock(), mock.Mock())

    def test_get_group_path(self, config):
        partitioner = Partitioner(config, self.topics, mock.Mock(), mock.Mock())
        assert partitioner.get_group_path() == '/base_path/test_group_id'

    def test_get_partitions_set(self, partitioner):
        with mock.patch('yelp_kafka.partitioner.KafkaClient',
                        autospec=True) as mock_client:
            mock_client.return_value.topic_partitions = {
                'topic1': [0, 1, 2, 3],
                'topic2': [0, 1, 2],
                'topic3': [0, 1, 2, 3],
            }
            actual = partitioner.get_partitions_set()
            assert actual == set([
                'topic1-0', 'topic1-1', 'topic1-2', 'topic1-3',
                'topic2-0', 'topic2-1', 'topic2-2'
            ])

    def test_get_partitions_set_kafka_away(self, partitioner):
        with mock.patch('yelp_kafka.partitioner.KafkaClient',
                        autospec=True) as mock_client:
            mock_obj = mock_client.return_value
            mock_obj.load_metadata_for_topics.side_effect = iter(
                [KafkaUnavailableError, None]
            )
            mock_client.return_value.topic_partitions = {
                'topic1': [0, 1, 2, 3],
                'topic2': [0, 1, 2],
                'topic3': [0, 1, 2, 3],
            }
            actual = partitioner.get_partitions_set()
            assert actual == set([
                'topic1-0', 'topic1-1', 'topic1-2', 'topic1-3',
                'topic2-0', 'topic2-1', 'topic2-2'
            ])

    def test_get_partitions_set_error(self, partitioner):
        with mock.patch('yelp_kafka.partitioner.KafkaClient',
                        autospec=True) as mock_client:
            mock_obj = mock_client.return_value
            mock_obj.load_metadata_for_topics.side_effect = KafkaUnavailableError
            mock_client.return_value.topic_partitions = {
                'topic1': [0, 1, 2, 3],
                'topic2': [0, 1, 2],
                'topic3': [0, 1, 2, 3],
            }
            with pytest.raises(KafkaUnavailableError):
                partitioner.get_partitions_set()

    def test_handle_release(self, partitioner):
        mock_kpartitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.RELEASE)
        )
        expected_partitions = {'topic1': [0, 1, 3]}
        partitioner.acquired_partitions = expected_partitions
        partitioner._handle_group(mock_kpartitioner)
        mock_kpartitioner.release_set.assert_called_once_with()
        partitioner.release.assert_called_once_with(expected_partitions)

    def test_handle_failed_and_release(self, partitioner):
        mock_kpartitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.FAILURE)
        )
        expected_partitions = {'topic1': [0, 1, 3]}
        partitioner.acquired_partitions = expected_partitions
        partitioner._handle_group(mock_kpartitioner)
        partitioner.release.assert_called_once_with(expected_partitions)

    def test_handle_acquired(self, partitioner):
        mock_kpartitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.ACQUIRED)
        )
        mock_kpartitioner.__iter__.return_value = ['topic1-0', 'topic1-2', 'topic2-1']
        partitioner._handle_group(mock_kpartitioner)
        expected_partitions = {'topic1': [0, 2], 'topic2': [1]}
        assert partitioner.acquired_partitions == expected_partitions
        partitioner.acquire.assert_called_once_with(expected_partitions)

    def test_handle_allocating(self, partitioner):
        mock_kpartitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.ALLOCATING)
        )
        partitioner._handle_group(mock_kpartitioner)
        mock_kpartitioner.wait_for_acquire.assert_called_once_with()

    def test__get_partitioner(self, partitioner, config):
        # We create a new partitioner, then we change the partitions
        # and we expect the partitioner to be destroyed.
        # Afterwards, the partitioner should be recreated for the new
        # partitions set.
        expected_partitions = set(['top-1', 'top1-2'])

        with contextlib.nested(
            mock.patch.object(Partitioner, '_create_partitioner',
                              side_effect=[mock.sentinel.partitioner1,
                                           mock.sentinel.partitioner2]),
            mock.patch.object(Partitioner, '_destroy_partitioner')
        ) as (mock_create, mock_destroy):
            actual = partitioner._get_partitioner(
                expected_partitions
            )
            assert actual == mock.sentinel.partitioner1
            assert partitioner.partitions_set == expected_partitions

            # Change the partitions and test the partitioner gets destroyed for
            # rebalancing
            new_expected_partitions = set(['top-1', 'top1-2', 'top1-3'])
            actual = partitioner._get_partitioner(
                new_expected_partitions
            )
            assert partitioner.partitions_set is new_expected_partitions
            assert mock_destroy.called
            assert actual == mock.sentinel.partitioner2
            assert mock_create.call_count == 2

            # Call the partitioner again with the same partitions set and be sure
            # it does not create a new one
            actual = partitioner._get_partitioner(
                new_expected_partitions
            )
            assert partitioner.partitions_set is new_expected_partitions
            assert actual == mock.sentinel.partitioner2
            assert mock_create.call_count == 2

    @mock.patch('yelp_kafka.partitioner.KazooClient')
    def test__destroy_partitioner(self, mock_kazoo, config):
        mock_kpartitioner = mock.MagicMock(spec=SetPartitioner)
        config['zk_partitioner_cooldown'] = 45
        partitioner = Partitioner(config, self.topics, mock.Mock(), mock.Mock())
        partitioner._destroy_partitioner(mock_kpartitioner)
        mock_kpartitioner.finish.assert_called_once()
        mock_kazoo.stop.assert_called_once()

    @mock.patch('yelp_kafka.partitioner.KazooClient')
    def test__create_partitioner(self, mock_kazoo, config):
        mock_kpartitioner = mock.MagicMock(spec=SetPartitioner)
        mock_kazoo.return_value.SetPartitioner.return_value = mock_kpartitioner
        mock_kazoo.return_value.state = KazooState.CONNECTED
        config['zk_partitioner_cooldown'] = 45
        partitioner = Partitioner(config, self.topics, mock.Mock(), mock.Mock())
        expected_partitions = set(['top-1', 'top1-2'])
        assert mock_kpartitioner == partitioner._create_partitioner(
            expected_partitions
        )
        mock_kazoo.return_value.SetPartitioner.assert_called_once_with(
            path='/base_path/test_group_id',
            set=expected_partitions,
            time_boundary=45
        )
        assert not mock_kazoo.return_value.start.called

    @mock.patch('yelp_kafka.partitioner.KazooClient')
    def test__create_partitioner_no_kazoo_connection(self, mock_kazoo, config):
        mock_kpartitioner = mock.MagicMock(spec=SetPartitioner)
        mock_kazoo.return_value.SetPartitioner.return_value = mock_kpartitioner
        mock_kazoo.return_value.state = KazooState.LOST
        config['zk_partitioner_cooldown'] = 45
        partitioner = Partitioner(config, self.topics, mock.Mock(), mock.Mock())
        expected_partitions = set(['top-1', 'top1-2'])
        assert mock_kpartitioner == partitioner._create_partitioner(
            expected_partitions
        )
        mock_kazoo.return_value.SetPartitioner.assert_called_once_with(
            path='/base_path/test_group_id',
            set=expected_partitions,
            time_boundary=45
        )
        mock_kazoo.return_value.start.assert_called_once()
