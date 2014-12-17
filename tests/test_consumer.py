import contextlib
import mock
import pytest

from kafka.common import Message as KafkaMessage
from kafka.common import OffsetAndMessage
from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.consumer import Message


@contextlib.contextmanager
def mock_kafka(num_messages=0):
    with contextlib.nested(
        mock.patch('yelp_kafka.consumer.KafkaClient', autospec=True),
        mock.patch('yelp_kafka.consumer.SimpleConsumer', autospec=True)
    ) as (mock_client, mock_consumer):
        mocked_messages = [OffsetAndMessage(i, mock.MagicMock()) for i in range(0, num_messages)]
        mock_consumer.return_value.__iter__.return_value = mocked_messages
        yield mock_client, mock_consumer


@pytest.fixture
def config():
    return {
        'brokers': 'test_brokers:9292',
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
                                   '_validate_offsets') as mock_validate:
                mock_client.return_value = mock.sentinel.client
                config['latest_offset'] = False
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                mock_client.assert_called_once_with('test_broker:9292',
                                                    client_id='test_client_id')
                expected_args = mock.sentinel.client, 'test_group', 'test_topic'
                assert mock_consumer.call_args[0] == expected_args
                mock_validate.assert_called_once_with(False)

    def test_get_message(self):
        with mock_kafka() as (_, mock_consumer):
            mock_obj = mock_consumer.return_value
            mock_message = mock.Mock()
            mock_message.value = 'test_content'
            mock_message.key = 'test_key'
            mock_obj.get_message.return_value = (mock.Mock(), mock_message)
            consumer = KafkaSimpleConsumer('test_topic')
            assert consumer.get_message() == Message('test_key', 'test_content')

    def test__valid_offsets(self):
        pass

    def test__invalid_offsets_get_largest(self):
        pass

    def test__invalid_offsets_get_smallest(self):
        pass

#    def test__process(self):
#        consumer = SimpleConsumer('test_topic')
#        process_func = mock.Mock()
#        consumer.process = process_func
#        message = mock.Mock(spec=Message)
#        message.value = 'test_message'
#        message.key = 'test_key'
#        consumer._process(message)
#        process_func.assert_called_once_with(
#            'test_key',
#            'test_message'
#        )
#
#    def test_run(self):
#        num_messages = 5
#        with mock_kafka(num_messages) as (mock_client, mock_consumer):
#            with mock.patch.object(SimpleConsumer,
#                                   '_validate_offsets') as mock_validate:
#                consumer = SimpleConsumer('test_topic')
#                consumer.process = mock.Mock()
#                consumer.run('test_broker:9292', 'test_client_id', 'test_group')
#                mock_validate.assert_called_once()
#                assert consumer.process.call_count == num_messages
