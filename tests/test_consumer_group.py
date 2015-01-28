import mock
from multiprocessing import Process
import os
import pytest

from yelp_kafka.consumer_group import ConsumerGroup
from yelp_kafka.consumer_group import MultiprocessingConsumerGroup
from kafka.common import KafkaUnavailableError
from kazoo.recipe.partitioner import SetPartitioner
from kazoo.recipe.partitioner import PartitionState
from kazoo.protocol.states import KazooState


@pytest.fixture
def config():
    return {
        'brokers': 'test_broker:9292',
        'group_id': 'test_group_id',
        'zookeeper_base': '/base_path',
    }


def get_partitioner_state(status):
    return {'state': status}


class TestConsumerGroup(object):

    topics = ['topic1', 'topic2']
    zookeeper_hosts = ['zookeeper_uri1:2181', 'zookeeper_uri2:2181']

    @pytest.fixture
    def group(self, config):
        with mock.patch('yelp_kafka.consumer_group.KazooClient', autospec=True):
            return ConsumerGroup(self.zookeeper_hosts, self.topics, config)

    def test_get_group_path(self, config):
        group = ConsumerGroup(self.zookeeper_hosts, self.topics, config)
        assert group.get_group_path() == '/base_path/test_group_id'

    def test_get_all_partitions(self, group):
        with mock.patch('yelp_kafka.consumer_group.KafkaClient',
                        autospec=True) as mock_client:
            mock_client.return_value.topic_partitions = {
                'topic1': [0, 1, 2, 3],
                'topic2': [0, 1, 2],
                'topic3': [0, 1, 2, 3],
            }
            actual = group.get_all_partitions()
            assert actual == set([
                'topic1-0', 'topic1-1', 'topic1-2', 'topic1-3',
                'topic2-0', 'topic2-1', 'topic2-2'
            ])

    def test_get_all_partitions_kafka_away(self, group):
        with mock.patch('yelp_kafka.consumer_group.KafkaClient',
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
            actual = group.get_all_partitions()
            assert actual == set([
                'topic1-0', 'topic1-1', 'topic1-2', 'topic1-3',
                'topic2-0', 'topic2-1', 'topic2-2'
            ])

    def test_get_all_partitions_error(self, group):
        with mock.patch('yelp_kafka.consumer_group.KafkaClient',
                        autospec=True) as mock_client:
            mock_obj = mock_client.return_value
            mock_obj.load_metadata_for_topics.side_effect = KafkaUnavailableError
            mock_client.return_value.topic_partitions = {
                'topic1': [0, 1, 2, 3],
                'topic2': [0, 1, 2],
                'topic3': [0, 1, 2, 3],
            }
            with pytest.raises(KafkaUnavailableError):
                group.get_all_partitions()

    def test_handle_release(self, group):
        group.release = mock.Mock()
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.RELEASE)
        )
        group.allocated_consumers = [mock.Mock(), mock.Mock]
        group._handle_group_status(mock_partitioner)
        mock_partitioner.release_set.assert_called_once_with()
        assert group.release.called
        assert group.get_consumers() is None

    def test_handle_failed_and_release(self, group):
        group.release = mock.Mock()
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.FAILURE)
        )
        group.allocated_consumers = [mock.Mock(), mock.Mock]
        group._acquired_partitions = {'topic1': [1, 2], 'topic2': [0]}
        group._handle_group_status(mock_partitioner)
        assert group.release.called
        assert group.get_consumers() is None

    def test_handle_acquired(self, group):
        mock_start = mock.Mock()
        consumers = [mock.Mock(), mock.Mock()]
        mock_start.return_value = consumers
        group.start = mock_start
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.ACQUIRED)
        )
        mock_partitioner.__iter__.return_value = ['topic1-0', 'topic1-2', 'topic2-1']
        group._handle_group_status(mock_partitioner)
        actual_partitions, = mock_start.call_args[0]
        assert actual_partitions == {'topic1': [0, 2], 'topic2': [1]}
        assert group.get_consumers() == consumers

    def test_handle_allocating(self, group):
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_state(PartitionState.ALLOCATING)
        )
        group._handle_group_status(mock_partitioner)
        mock_partitioner.wait_for_acquire.assert_called_once_with()

    def test_get_consumers(self, group):
        group.allocated_consumers = [mock.Mock(), mock.Mock]
        actual = group.get_consumers()
        # Test that get_consumers actually returns a copy
        assert actual is not group.allocated_consumers
        assert actual == group.allocated_consumers

    def test__monitor(self, group):
        group._acquired_partitions = [mock.Mock(), mock.Mock]
        mocked_monitor = mock.Mock()
        group.monitor = mocked_monitor
        group._monitor()
        mocked_monitor.assert_called_once_with()

    def test__monitor_no_partitions(self, group):
        mocked_monitor = mock.Mock()
        group.monitor = mocked_monitor
        group._monitor()
        assert not mocked_monitor.called

    @mock.patch.object(ConsumerGroup, 'get_all_partitions')
    def test__get_partitioner(self, mock_partitions, group, config):
        # We create a new partitioner, then we change the partitions
        # and we expect the partitioner to be destroyed.
        # Afterwards, the partitioner should be recreated for the new
        # partitions set.
        expected_partitions = set(['top-1', 'top1-2'])
        mock_partitions.return_value = expected_partitions

        with mock.patch.object(ConsumerGroup,
                               '_create_partitioner') as mock_create:
            mock_create.return_value = mock.sentinel.partitioner
            assert group._get_partitioner() == mock.sentinel.partitioner
            assert group.all_partitions == expected_partitions

        # Change the partitions and test the partitioner gets destroyed for
        # rebalancing
        new_expected_partitions = set(['top-1', 'top1-2', 'top1-3'])
        mock_partitions.return_value = new_expected_partitions
        with mock.patch.object(ConsumerGroup,
                               '_destroy_partitioner') as mock_destroy:
            assert group._get_partitioner() is None
            assert group.all_partitions is None
            assert mock_destroy.called

        # Next time we call get_partitioner a new one should be created
        with mock.patch.object(ConsumerGroup,
                               '_create_partitioner') as mock_create:
            mock_create.return_value = mock.sentinel.partitioner
            assert group._get_partitioner() == mock.sentinel.partitioner
            assert group.all_partitions == new_expected_partitions

    @mock.patch('yelp_kafka.consumer_group.KazooClient')
    def test__destroy_partitioner(self, mock_kazoo, config):
        mock_partitioner = mock.MagicMock(spec=SetPartitioner)
        config['zk_partitioner_cooldown'] = 45
        group = ConsumerGroup(self.zookeeper_hosts, self.topics, config)
        group._destroy_partitioner(mock_partitioner)
        mock_partitioner.finish.assert_called_once()
        mock_kazoo.stop.assert_called_once()

    @mock.patch('yelp_kafka.consumer_group.KazooClient')
    def test__create_partitioner(self, mock_kazoo, config):
        mock_partitioner = mock.MagicMock(spec=SetPartitioner)
        mock_kazoo.return_value.SetPartitioner.return_value = mock_partitioner
        mock_kazoo.return_value.state = KazooState.CONNECTED
        config['zk_partitioner_cooldown'] = 45
        group = ConsumerGroup(self.zookeeper_hosts, self.topics, config)
        expected_partitions = set(['top-1', 'top1-2'])
        assert mock_partitioner == group._create_partitioner(
            expected_partitions
        )
        mock_kazoo.return_value.SetPartitioner.assert_called_once_with(
            path='/base_path/test_group_id',
            set=expected_partitions,
            time_boundary=45
        )
        assert not mock_kazoo.return_value.start.called

    @mock.patch('yelp_kafka.consumer_group.KazooClient')
    def test__create_partitioner_no_kazoo_connection(self, mock_kazoo, config):
        mock_partitioner = mock.MagicMock(spec=SetPartitioner)
        mock_kazoo.return_value.SetPartitioner.return_value = mock_partitioner
        mock_kazoo.return_value.state = KazooState.LOST
        config['zk_partitioner_cooldown'] = 45
        group = ConsumerGroup(self.zookeeper_hosts, self.topics, config)
        expected_partitions = set(['top-1', 'top1-2'])
        assert mock_partitioner == group._create_partitioner(
            expected_partitions
        )
        mock_kazoo.return_value.SetPartitioner.assert_called_once_with(
            path='/base_path/test_group_id',
            set=expected_partitions,
            time_boundary=45
        )
        mock_kazoo.return_value.start.assert_called_once()


@mock.patch('yelp_kafka.consumer_group.KazooClient', autospec=True)
class TestMultiprocessingConsumerGroup(object):

    zookeeper_hosts = ['zookeeper_uri1:2181', 'zookeeper_uri2:2181']
    topics = ['topic1', 'topic2']

    def test_start(self, _, config):
        consumer_factory = mock.Mock()
        mock_consumer = mock.Mock()
        consumer_factory.return_value = mock_consumer
        group = MultiprocessingConsumerGroup(
            self.zookeeper_hosts, self.topics,
            config, consumer_factory
        )
        acquired_partitions = {
            'topic1': [0, 1, 2],
            'topic2': [3]
        }
        with mock.patch('yelp_kafka.consumer_group.Process',
                        autospec=True) as mock_process:
            actual_consumers = group.start(acquired_partitions)
            assert all(consumer is mock_consumer
                       for consumer in actual_consumers)
            assert consumer_factory.call_count == 4
            assert mock_process.call_count == 4
            assert mock_process.return_value.start.call_count == 4

    def test_release(self, _, config):
        group = MultiprocessingConsumerGroup(
            self.zookeeper_hosts, self.topics,
            config, mock.Mock()
        )
        consumer = mock.Mock()
        args = {'is_alive.return_value': False}
        group.consumer_procs = {
            mock.Mock(spec=Process, **args): consumer,
            mock.Mock(spec=Process, **args): consumer
        }
        with mock.patch.object(os, 'kill', autospec=True) as mock_kill:
            # Release takes acquired_partitions but in this case it is not used
            # so we pass None
            group.release(None)
        assert not mock_kill.called
        assert consumer.terminate.call_count == 2

    def test_release_and_kill_unresponsive_consumer(self, _, config):
        # Change default waiting time to not slow down the test
        config['max_termination_timeout_secs'] = 0.1
        group = MultiprocessingConsumerGroup(
            self.zookeeper_hosts, self.topics,
            config, mock.Mock()
        )
        consumer = mock.Mock()
        args = {'is_alive.return_value': True}
        group.consumer_procs = {
            mock.Mock(spec=Process, **args): consumer,
            mock.Mock(spec=Process, **args): consumer
        }
        with mock.patch.object(os, 'kill', autospec=True) as mock_kill:
            # Release takes acquired_partitions but in this case it is not used
            # so we pass None
            group.release(None)
        assert mock_kill.call_count == 2
        assert consumer.terminate.call_count == 2

    def test_monitor(self, _, config):
        group = MultiprocessingConsumerGroup(
            self.zookeeper_hosts, self.topics,
            config, mock.Mock()
        )
        consumer1 = mock.Mock()
        consumer2 = mock.Mock()
        args1 = {'is_alive.return_value': False}
        args2 = {'is_alive.return_value': True}
        group.consumer_procs = {
            mock.Mock(spec=Process, **args1): consumer1,
            mock.Mock(spec=Process, **args2): consumer2
        }
        with mock.patch.object(
            MultiprocessingConsumerGroup, '_start_consumer', autospec=True
        ) as mock_start:
            mock_start.return_value = mock.sentinel.proc
            group.monitor()
        assert mock.sentinel.proc in group.consumer_procs
        mock_start.assert_called_once_with(group, consumer1)
