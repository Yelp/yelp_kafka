import contextlib
import mock
import pytest

from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.consumer import KafkaConsumerBase
from yelp_kafka.consumer import Message
from yelp_kafka.error import ProcessMessageError


@contextlib.contextmanager
def mock_kafka():
    with contextlib.nested(
        mock.patch('yelp_kafka.consumer.KafkaClient', autospec=True),
        mock.patch('yelp_kafka.consumer.SimpleConsumer', autospec=True)
    ) as (mock_client, mock_consumer):
        yield mock_client, mock_consumer


@pytest.fixture
def config():
    return KafkaConsumerConfig(
        cluster={'broker_list': ['test_broker:9292'],
                 'zookeeper': 'test_cluster'},
        group_id='test_group',
        client_id='test_client_id'
    )


class TestKafkaSimpleConsumer(object):

    def test_topic_error(self, config):
        with pytest.raises(TypeError):
            KafkaSimpleConsumer(['test_topic'], config)

    def test_partitions_error(self, config):
        with pytest.raises(TypeError):
            KafkaSimpleConsumer('test_topic', config, partitions='1')

    def test_connect(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer,
                                   '_validate_offsets'):
                mock_client.return_value = mock.sentinel.client
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                mock_client.assert_called_once_with(['test_broker:9292'],
                                                    client_id='test_client_id')
                assert not mock_consumer.call_args[0]
                kwargs = mock_consumer.call_args[1]
                assert kwargs['topic'] == 'test_topic'
                assert kwargs['group'] == 'test_group'

    def test_get_message_same_offset(self, config):
        with mock_kafka() as (_, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer,
                                   '_validate_offsets'):
                mock_obj = mock_consumer.return_value
                # get message should return a tuple (partition_id, (offset,
                # Message)). Message is a namedtuple defined in kafka-python that
                # at least contains key and value.
                mock_message = mock.Mock()
                mock_message.value = 'test_content'
                mock_message.key = 'test_key'
                kafka_message = (1, (12345, mock_message))
                mock_obj.get_message.return_value = kafka_message
                # Set the current offset the offset of the message
                mock_obj.offsets = {1: 12345}
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                assert consumer.get_message() == Message(
                    partition=1, offset=12345, key='test_key', value='test_content'
                )

    def test_get_message(self, config):
        with mock_kafka() as (_, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer,
                                   '_validate_offsets'):
                mock_obj = mock_consumer.return_value
                # get message should return a tuple (partition_id, (offset,
                # Message)). Message is a namedtuple defined in kafka-python that
                # at least contains key and value.
                mock_message = mock.Mock()
                mock_message.value = 'test_content'
                mock_message.key = 'test_key'
                kafka_message = (1, (12345, mock_message))
                mock_obj.get_message.return_value = kafka_message
                # Set the current offset the offset of the message + 1
                mock_obj.offsets = {1: 12346}
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                assert consumer.get_message() == Message(
                    partition=1, offset=12345, key='test_key', value='test_content'
                )

    def test_get_old_message(self, config):
        with mock_kafka() as (_, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer,
                                   '_validate_offsets'):
                mock_obj = mock_consumer.return_value
                # get message should return a tuple (partition_id, (offset,
                # Message)). Message is a namedtuple defined in kafka-python that
                # at least contains key and value.
                mock_message = mock.Mock()
                mock_message.value = 'test_content'
                mock_message.key = 'test_key'
                kafka_message = (1, (12344, mock_message))
                mock_obj.get_message.side_effect = [kafka_message, None]
                # Set the current offset the offset of the message + 2 because
                # we are actually receiving an old message
                mock_obj.offsets = {1: 12346}
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                assert consumer.get_message() is None

    def test__valid_offsets(self, config):
        with mock_kafka() as (_, mock_consumer):
            mock_offsets = mock.PropertyMock()
            mock_offsets.side_effect = [
                {0: 12, 1: 12}, {0: 0, 1: 0}, None
            ]
            mock_obj = mock_consumer.return_value
            type(mock_obj).auto_commit = True
            type(mock_obj).fetch_offsets = mock_offsets
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_obj.seek.assert_called_once_with(0, 0)
            mock_offsets.assert_called_with({0: 12, 1: 12})

    def test__invalid_offsets_get_latest(self, config):
        with mock_kafka() as (_, mock_consumer):
            mock_offsets = mock.PropertyMock()
            mock_offsets.side_effect = [
                {0: 0, 1: 0}, {0: 12, 1: 12}, None
            ]
            mock_obj = mock_consumer.return_value
            type(mock_obj).fetch_offsets = mock_offsets
            type(mock_obj).auto_commit = True
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_obj.seek.assert_called_with(-1, 2)

    def test__invalid_offsets_get_earliest(self, config):
        # Change config to use smallest (earliest) offset (default latest)
        config = KafkaConsumerConfig(
            cluster={'broker_list': ['test_broker:9292'],
                     'zookeeper': 'test_cluster'},
            group_id='test_group',
            client_id='test_client_id',
            auto_offset_reset='smallest'
        )
        with mock_kafka() as (_, mock_consumer):
            mock_offsets = mock.PropertyMock()
            mock_offsets.side_effect = [
                {0: 0, 1: 0}, {0: 12, 1: 12}, None
            ]
            mock_obj = mock_consumer.return_value
            type(mock_obj).fetch_offsets = mock_offsets
            type(mock_obj).auto_commit = True
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_obj.seek.assert_called_once_with(0, 0)

    def test_close(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer,
                                   '_validate_offsets'):
                mock_obj = mock_consumer.return_value
                type(mock_obj).auto_commit = True
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                consumer.close()
                mock_consumer.return_value.commit.assert_called_once_with()
                mock_client.return_value.close.assert_called_once_with()

    def test_close_no_commit(self, config):
        config = KafkaConsumerConfig(
            cluster={'broker_list': ['test_broker:9292'],
                     'zookeeper': 'test_cluster'},
            group_id='test_group',
            client_id='test_client_id',
            auto_commit=False
        )
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer,
                                   '_validate_offsets'):
                mock_obj = mock_consumer.return_value
                type(mock_obj).auto_commit = False
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                consumer.close()
                assert not mock_consumer.return_value.commit.called
                mock_client.return_value.close.assert_called_once_with()


class TestKafkaConsumer(object):

    def test_run_and_terminate(self, config):
        message_iterator = iter([
            Message(1, 12345, 'key1', 'value1'),
            Message(1, 12346, 'key2', 'value2'),
            Message(1, 12347, 'key1', 'value3'),
        ])
        with mock_kafka() as (mock_client, mock_consumer):
            with contextlib.nested(
                mock.patch.object(KafkaSimpleConsumer, '_validate_offsets'),
                mock.patch.object(KafkaSimpleConsumer, '__iter__',
                                  return_value=message_iterator)
            ):
                consumer = KafkaConsumerBase('test_topic', config)
                consumer.process = mock.Mock()
                consumer.initialize = mock.Mock()
                consumer.dispose = mock.Mock()
                consumer.terminate()
                consumer.run()
                consumer.initialize.called_once()
                # process should have been called 3 times
                assert consumer.process.call_count == 3
                # check just last call arguments
                consumer.process.calls_args_list([
                    Message(1, 12347, 'key1', 'value3'),
                    Message(1, 12345, 'key1', 'value1'),
                    Message(1, 12346, 'key2', 'value2'),
                ])
                consumer.dispose.assert_called_once_with()
                mock_consumer.return_value.commit.assert_called_once_with()
                mock_client.return_value.close.assert_called_once_with()

    def test_process_error(self, config):
        message_iterator = iter([
            Message(1, 12345, 'key1', 'value1'),
            Message(1, 12346, 'key2', 'value2'),
            Message(1, 12347, 'key1', 'value3'),
        ])
        with mock_kafka() as (mock_client, mock_consumer):
            with contextlib.nested(
                mock.patch.object(KafkaSimpleConsumer, '_validate_offsets'),
                mock.patch.object(KafkaSimpleConsumer, '__iter__',
                                  return_value=message_iterator)
            ):
                consumer = KafkaConsumerBase('test_topic', config)
                consumer.process = mock.Mock(side_effect=Exception('Boom!'))
                consumer.initialize = mock.Mock()
                consumer.dispose = mock.Mock()
                with pytest.raises(ProcessMessageError):
                    consumer.run()
