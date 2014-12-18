import mock
from multiprocessing import Process
import os
import pytest

from yelp_kafka.consumer_group import ConsumerGroup
from yelp_kafka.consumer_group import MultiprocessingConsumerGroup
from kafka.common import KafkaUnavailableError
from kazoo.recipe.partitioner import SetPartitioner


@pytest.fixture
def config():
    return {
        'brokers': 'test_broker:9292',
        'group_id': 'test_group_id'
    }


def get_partitioner_status(status):
    return dict(
        [(k, True if k == status else False)
         for k in ('failed', 'release', 'acquired', 'allocating')]
    )


@mock.patch('yelp_kafka.consumer_group.KazooClient', autospec=True)
class TestConsumerGroup(object):

    topics = ['topic1', 'topic2']

    def test_get_group_path(self, _, config):
        config['zookeeper_base'] = '/base_path'
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        assert group.get_group_path() == '/base_path/test_group_id'

    def test_get_all_partitions(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
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

    def test_get_all_partitions_kafka_away(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
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

    def test_get_all_partitions_error(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
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

    def test_handle_release(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        group.release = mock.Mock()
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_status('release')
        )
        group.partitioner = mock_partitioner
        group.allocated_consumers = [mock.Mock(), mock.Mock]
        group._handle_partitions()
        mock_partitioner.release_set.assert_called_once_with()
        assert group.release.called
        assert group.get_consumers() is None

    def test_handle_failed(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_status('failed')
        )
        group.partitioner = mock_partitioner
        group._handle_partitions()
        assert group.partitioner is None

    def test_handle_failed_and_release(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        group.release = mock.Mock()
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_status('failed')
        )
        group.allocated_consumers = [mock.Mock(), mock.Mock]
        group._acquired_partitions = {'topic1': [1, 2], 'topic2': [0]}
        group.partitioner = mock_partitioner
        group._handle_partitions()
        assert group.partitioner is None
        assert group.release.called
        assert group.get_consumers() is None

    def test_handle_acquired(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        mock_start = mock.Mock()
        consumers = [mock.Mock(), mock.Mock()]
        mock_start.return_value = consumers
        group.start = mock_start
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_status('acquired')
        )
        mock_partitioner.__iter__.return_value = ['topic1-0', 'topic1-2', 'topic2-1']
        group.partitioner = mock_partitioner
        group._handle_partitions()
        actual_partitions, = mock_start.call_args[0]
        assert actual_partitions == {'topic1': [0, 2], 'topic2': [1]}
        assert group.get_consumers() == consumers

    def test_handle_allocating(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        mock_partitioner = mock.MagicMock(
            spec=SetPartitioner, **get_partitioner_status('allocating')
        )
        group.partitioner = mock_partitioner
        group._handle_partitions()
        mock_partitioner.wait_for_acquire.assert_called_once_with()

    def test_get_consumers(self, mock_kazoo, config):
        group = ConsumerGroup('zookeeper_uri:2181', self.topics, config)
        group.allocated_consumers = [mock.Mock(), mock.Mock]
        actual = group.get_consumers()
        # Test that get_consumers actually returns a copy
        assert actual is not group.allocated_consumers
        assert actual == group.allocated_consumers


@mock.patch('yelp_kafka.consumer_group.KazooClient', autospec=True)
class TestMultiprocessingConsumerGroup(object):

    topics = ['topic1', 'topic2']

    def test_start(self, _, config):
        consumer_factory = mock.Mock()
        mock_consumer = mock.Mock()
        consumer_factory.return_value = mock_consumer
        group = MultiprocessingConsumerGroup(
            'zookeeper_uri:2181', self.topics,
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
            'zookeeper_uri:2181', self.topics,
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
            'zookeeper_uri:2181', self.topics,
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
            'zookeeper_uri:2181', self.topics,
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
