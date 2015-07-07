import mock
from multiprocessing import Process
import os
import time
import pytest

from kafka.common import ConsumerTimeout

from yelp_kafka.config import ClusterConfig, KafkaConsumerConfig
from yelp_kafka.consumer_group import (
    ConsumerGroup,
    KafkaConsumerGroup,
    MultiprocessingConsumerGroup,
)
from yelp_kafka.error import (
    ProcessMessageError,
    PartitionerError,
    PartitionerZookeeperError,
    ConsumerGroupError,
)


@mock.patch('yelp_kafka.consumer_group.Partitioner', autospec=True)
class TestConsumerGroup(object):

    topic = 'topic1'

    def test__consume(self, mock_partitioner, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        group.consumer = mock.MagicMock()
        group.consumer.__iter__.return_value = [
            mock.sentinel.message1,
            mock.sentinel.message2
        ]
        group.consume(refresh_timeout=1)
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
        with pytest.raises(PartitionerError):
            group.consume(refresh_timeout=1)
        mock_partitioner.return_value.refresh.side_effect = PartitionerZookeeperError("Boom")
        with pytest.raises(PartitionerZookeeperError):
            group.consume(refresh_timeout=1)

    def test__consume_error(self, mock_partitioner, config):
        group = ConsumerGroup(self.topic, config, mock.Mock(side_effect=Exception("Boom!")))
        group.consumer = mock.MagicMock()
        group.consumer.__iter__.return_value = [
            mock.sentinel.message1,
            mock.sentinel.message2
        ]
        with pytest.raises(ProcessMessageError):
            group.consume(refresh_timeout=1)

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
    def test__acquire_no_partitions_assigned(self, mock_consumer, _, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        partitions = {}
        group._acquire(partitions)
        assert not mock_consumer.called

    @mock.patch('yelp_kafka.consumer_group.KafkaSimpleConsumer', autospec=True)
    def test__release(self, mock_consumer, _, config):
        group = ConsumerGroup(self.topic, config, mock.Mock())
        partitions = {self.topic: [0, 1]}
        group._acquire(partitions)
        group._release(partitions)
        mock_consumer.return_value.close.assert_called_once_with()


class TestKafkaConsumerGroup(object):

    topic = 'topic1'

    def test___init__string_topics(self):
        with pytest.raises(AssertionError):
            KafkaConsumerGroup('topic', None)

    def test__should_keep_trying_no_timeout(self):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     consumer_timeout_ms=-1)
        consumer = KafkaConsumerGroup([], config)

        long_time_ago = time.time() - 1000
        assert consumer._should_keep_trying(long_time_ago)

    @mock.patch('time.time')
    def test__should_keep_trying_not_timed_out(self, mock_time):
        mock_time.return_value = 0

        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     consumer_timeout_ms=1000)
        consumer = KafkaConsumerGroup([], config)

        almost_a_second_ago = time.time() - 0.8
        assert consumer._should_keep_trying(almost_a_second_ago)

    @mock.patch('time.time')
    def test__should_keep_trying_timed_out(self, mock_time):
        mock_time.return_value = 0

        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     consumer_timeout_ms=1000)
        consumer = KafkaConsumerGroup([], config)

        over_a_second_ago = time.time() - 1.2
        assert not consumer._should_keep_trying(over_a_second_ago)

    def test__auto_commit_enabled_is_enabled(self):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     auto_commit_enable=True)
        consumer = KafkaConsumerGroup([], config)
        assert consumer._auto_commit_enabled()

    def test__auto_commit_enabled_not_enabled(self):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     auto_commit_enable=False)
        consumer = KafkaConsumerGroup([], config)
        assert not consumer._auto_commit_enabled()

    @mock.patch('yelp_kafka.consumer_group.Partitioner')
    @mock.patch('yelp_kafka.consumer_group.KafkaConsumer')
    def test_next(self, mock_consumer, mock_partitioner):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     consumer_timeout_ms=500)
        consumer = KafkaConsumerGroup([], config)
        consumer.partitioner = mock_partitioner()
        consumer.consumer = mock_consumer()

        def fake_next():
            time.sleep(1)
            raise ConsumerTimeout()

        consumer.consumer.next.side_effect = fake_next

        # The mock KafkaConsumer.next (called fake_next above) takes longer than
        # consumer_timeout_ms, so we should get a ConsumerTimeout from
        # KafkaConsumerGroup
        with pytest.raises(ConsumerTimeout):
            consumer.next()

        consumer.consumer.next.assert_called_once_with()
        consumer.partitioner.refresh.assert_called_once_with()

    def test__acquire_has_consumer(self):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster)
        consumer = KafkaConsumerGroup([], config)

        consumer.consumer = mock.Mock()
        consumer._acquire({'a': 'b'})

        consumer.consumer.set_topic_partitions.assert_called_once_with({'a': 'b'})

    @mock.patch('yelp_kafka.consumer_group.KafkaConsumer')
    def test__acquire_has_no_consumer(self, mock_consumer):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster)
        consumer = KafkaConsumerGroup([], config)

        consumer._acquire({'a': 'b'})
        mock_consumer.assert_called_once_with({'a': 'b'}, **consumer.config)

    def test__release(self):
        cluster = ClusterConfig('my_cluster', [], 'zookeeper:2181')
        config = KafkaConsumerConfig('my_group', cluster,
                                     auto_commit_enable=True)
        consumer = KafkaConsumerGroup([], config)

        mock_consumer = mock.Mock()
        consumer.consumer = mock_consumer
        consumer._release({})

        mock_consumer.commit.assert_called_once_with()
        mock_consumer.set_topic_partitions.assert_called_once_with({})


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

    def test_start_consumer_fail(self, group):
        consumer = mock.Mock(topic='Test', partitions=[1, 2, 3])
        with mock.patch(
            'yelp_kafka.consumer_group.Process',
            autospec=True,
        ) as mock_process:
            mock_process.return_value.start.side_effect = Exception("Boom!")
            with pytest.raises(ConsumerGroupError):
                group.start_consumer(consumer)

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
