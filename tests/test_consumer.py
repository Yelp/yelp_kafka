import contextlib
import mock
import pytest

from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.consumer import KafkaConsumer
from yelp_kafka.consumer import Message


@contextlib.contextmanager
def mock_kafka():
    with contextlib.nested(
        mock.patch('yelp_kafka.consumer.KafkaClient', autospec=True),
        mock.patch('yelp_kafka.consumer.SimpleConsumer', autospec=True)
    ) as (mock_client, mock_consumer):
        yield mock_client, mock_consumer


@pytest.fixture
def config():
    return {
        'brokers': 'test_broker:9292',
        'group_id': 'test_group',
        'client_id': 'test_client_id'
    }


class TestKafkaSimpleConsumer(object):

    def test_no_topic(self, config):
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
                mock_client.assert_called_once_with('test_broker:9292',
                                                    client_id='test_client_id')
                expected_args = mock.sentinel.client, 'test_group', 'test_topic'
                assert mock_consumer.call_args[0] == expected_args

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
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                assert consumer.get_message() == Message(
                    partition=1, offset=12345, key='test_key', value='test_content'
                )

    def test__valid_offsets(self, config):
        with mock_kafka() as (_, mock_consumer):
            mock_offsets = mock.PropertyMock()
            mock_offsets.side_effect = [
                {0: 12, 1: 12}, {0: 0, 1: 0}, None
            ]
            mock_obj = mock_consumer.return_value
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
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_obj.seek.assert_called_with(-1, 2)

    def test__invalid_offsets_get_earliest(self, config):
        # Change config to use earliest offset (default latest)
        config['latest_offset'] = False
        with mock_kafka() as (_, mock_consumer):
            mock_offsets = mock.PropertyMock()
            mock_offsets.side_effect = [
                {0: 0, 1: 0}, {0: 12, 1: 12}, None
            ]
            mock_obj = mock_consumer.return_value
            type(mock_obj).fetch_offsets = mock_offsets
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_obj.seek.assert_called_once_with(0, 0)


class TestKafkaConsumer(object):

    def test_run(self, config):
        with mock.patch.object(KafkaSimpleConsumer,
                               'connect') as mock_connect:
            with mock.patch.object(
                KafkaSimpleConsumer, '__iter__'
            ) as mock_iter:
                mock_iter.return_value = iter([
                    Message(1, 12345, 'key1', 'value1'),
                    Message(1, 12346, 'key2', 'value2'),
                    Message(1, 12347, 'key1', 'value3'),
                ])
                consumer = KafkaConsumer('test_topic', config)
                consumer.process = mock.Mock()
                consumer.initialize = mock.Mock()
                consumer.run()
                mock_connect.assert_called_once()
                consumer.initialize.called_once()
                # process should have been called 3 times
                assert consumer.process.call_count == 3
                # check just last call arguments
                consumer.process.assert_called_with(
                    Message(1, 12347, 'key1', 'value3')
                )

    def test_terminate(self, config):
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
                consumer = KafkaConsumer('test_topic', config)
                consumer.process = mock.Mock()
                consumer.dispose = mock.Mock()
                # Terminate before starting the look
                consumer.terminate()
                consumer.run()
                # process should have been called 1 times
                assert consumer.process.call_count == 1
                # check just last call arguments
                consumer.process.assert_called_with(
                    Message(1, 12345, 'key1', 'value1')
                )
                consumer.dispose.assert_called_once_with()
                mock_consumer.return_value.commit.assert_called_once_with()
                mock_client.return_value.close.assert_called_once_with()