import mock
from multiprocessing import Process
import os
import pytest

from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer_group import ConsumerGroup
from yelp_kafka.consumer_group import MultiprocessingConsumerGroup
from yelp_kafka.error import ProcessMessageError, PartitionerError, PartitionerZookeeperError


@mock.patch('yelp_kafka.consumer_group.Partitioner')
class TestConsumerGroup(object):

    topic = 'topic1'

    def test__consume(self, mock_partitioner, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        group.consumer = mock.MagicMock()
        group.consumer.__iter__.return_value = [
            mock.sentinel.message1,
            mock.sentinel.message2
        ]
        group._consume(refresh_timeout=1)
        assert group.process.call_args_list == [
            mock.call(mock.sentinel.message1),
            mock.call(mock.sentinel.message2)
        ]
        mock_partitioner.return_value.refresh.assert_called_once_with()

    def test__consume_partitioner_errors(self, mock_partitioner, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        group.consumer = mock.MagicMock()
        group.consumer.__iter__.return_value = [
            mock.sentinel.message1,
            mock.sentinel.message2
        ]
        mock_partitioner.return_value.refresh.side_effect = PartitionerError("Boom")
        group._consume(refresh_timeout=1)
        assert mock_partitioner.start.assert_called_once()

        mock_partitioner.return_value.refresh.side_effect = PartitionerZookeeperError("Boom")
        group._consume(refresh_timeout=1)
        assert mock_partitioner.start.assert_called_once()

    def test__consume_error(self, mock_partitioner, config):
        group = ConsumerGroup(self.topic, config, mock.Mock(side_effect=Exception("Boom!")))
        group.consumer = mock.MagicMock()
        group.consumer.__iter__.return_value = [
            mock.sentinel.message1,
            mock.sentinel.message2
        ]
        with pytest.raises(ProcessMessageError):
            group._consume(refresh_timeout=1)

    @mock.patch('yelp_kafka.consumer_group.KafkaSimpleConsumer', autospec=True)
    def test__acquire(self, mock_consumer, _, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        partitions = {self.topic: [0, 1]}
        group._acquire(partitions)
        args, _ = mock_consumer.call_args
        topic, _, partitions = args
        assert topic == self.topic
        assert partitions == [0, 1]
        mock_consumer.return_value.connect.assert_called_once_with()

    @mock.patch('yelp_kafka.consumer_group.KafkaSimpleConsumer', autospec=True)
    def test__release(self, mock_consumer, _, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        partitions = {self.topic: [0, 1]}
        group._acquire(partitions)
        group._release(partitions)
        mock_consumer.return_value.close.assert_called_once_with()


class TestMultiprocessingConsumerGroup(object):

    topics = ['topic1', 'topic2']

    @pytest.fixture
    @mock.patch('yelp_kafka.consumer_group.Partitioner', autospec=True)
    def group(self, _):
        config = KafkaConsumerConfig(
            cluster={'broker_list': ['test_broker:9292'],
                     'zookeeper': 'zookeeper_uri1:2181,zookeeper_uri2:2181'},
            group_id='test_group',
            client_id='test_client_id',
            max_termination_timeout_secs=0.1
        )
        return MultiprocessingConsumerGroup(
            self.topics,
            config, mock.Mock()
        )

    @mock.patch('yelp_kafka.consumer_group.Partitioner', autospec=True)
    def test_acquire(self, _, config):
        consumer_factory = mock.Mock()
        mock_consumer = mock.Mock()
        consumer_factory.return_value = mock_consumer
        group = MultiprocessingConsumerGroup(
            self.topics,
            config, consumer_factory
        )
        partitions = {
            'topic1': [0, 1, 2],
            'topic2': [3]
        }
        with mock.patch('yelp_kafka.consumer_group.Process',
                        autospec=True) as mock_process:
            group.acquire(partitions)
            assert all(consumer is mock_consumer
                       for consumer in group.get_consumers())
            assert consumer_factory.call_count == 4
            assert mock_process.call_count == 4
            assert mock_process.return_value.start.call_count == 4

    def test_release(self, group):
        consumer = mock.Mock()
        args = {'is_alive.return_value': False}
        group.consumers = [consumer, consumer]
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
        assert not group.get_consumers()

    def test_release_and_kill_unresponsive_consumer(self, group):
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

    def test_monitor(self, group):
        consumer1 = mock.Mock()
        consumer2 = mock.Mock()
        args1 = {'is_alive.return_value': False}
        args2 = {'is_alive.return_value': True}
        group.consumer_procs = {
            mock.Mock(spec=Process, **args1): consumer1,
            mock.Mock(spec=Process, **args2): consumer2
        }
        with mock.patch.object(
            MultiprocessingConsumerGroup, 'start_consumer', autospec=True
        ) as mock_start:
            mock_start.return_value = mock.sentinel.proc
            group.monitor()
        assert mock.sentinel.proc in group.consumer_procs
        mock_start.assert_called_once_with(group, consumer1)

    def test_get_consumers(self, group):
        group.consumers = [mock.Mock(), mock.Mock]
        actual = group.get_consumers()
        # Test that get_consumers actually returns a copy
        assert actual is not group.consumers
        assert actual == group.consumers
