import contextlib
import mock
import pytest

from setproctitle import getproctitle

from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer import(
    KafkaConsumerBase,
    KafkaSimpleConsumer,
    Message,
)
from yelp_kafka.error import ProcessMessageError
from yelp_kafka.offsets import PartitionOffsets


@contextlib.contextmanager
def mock_kafka():
    with contextlib.nested(
        mock.patch('yelp_kafka.consumer.KafkaClient', autospec=True),
        mock.patch('yelp_kafka.consumer.SimpleConsumer', autospec=True)
    ) as (mock_client, mock_consumer):
        mock_consumer.return_value.auto_commit = True
        yield mock_client, mock_consumer


class TestKafkaSimpleConsumer(object):
    topics_watermarks = {
        'test_topic': {
            0: PartitionOffsets('test_topic', 0, 30, 5),
            1: PartitionOffsets('test_topic', 1, 20, 6),
        }
    }

    @contextlib.contextmanager
    def mock_yelpkafka_consumer(self):
        with contextlib.nested(
            mock.patch(
                "yelp_kafka.consumer.get_topics_watermarks",
                return_value=self.topics_watermarks,
                autospec=True
            ),
            mock.patch.object(
                KafkaSimpleConsumer,
                "commit",
                autospec=True
            )
        ) as (mock_get_watermarks, mock_commit):
            yield mock_get_watermarks, mock_commit

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
        group_offsets = {0: 12, 1: 12}
        with mock_kafka() as (mock_client, mock_consumer):
            with self.mock_yelpkafka_consumer(
            ) as (mock_get_watermarks, mock_commit):

                mock_consumer.return_value.fetch_offsets = group_offsets
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()

                mock_get_watermarks.assert_called_with(
                    mock_client.return_value,
                    [consumer.topic]
                )
                assert mock_consumer.return_value.fetch_offsets == group_offsets
                assert mock_consumer.return_value.offsets == group_offsets
                mock_commit.assert_called_once_with(consumer)

    def test_valid_offsets_no_auto_commit(self, config):
        group_offsets = {0: 12, 1: 12}
        with mock_kafka() as (mock_client, mock_consumer):
            with self.mock_yelpkafka_consumer(
            ) as (mock_get_watermarks, mock_commit):

                mock_consumer.return_value.fetch_offsets = group_offsets
                mock_consumer.return_value.auto_commit = False
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()

                mock_get_watermarks.assert_called_with(
                    mock_client.return_value,
                    [consumer.topic]
                )
                assert mock_consumer.return_value.fetch_offsets == group_offsets
                assert mock_consumer.return_value.offsets == group_offsets
                assert not mock_commit.called

    def test_invalid_offsets_below_lowmark_get_latest(self, config):
        group_offsets = {0: 2, 1: 3}
        expected_offsets = {0: 30, 1: 20}
        with mock_kafka() as (mock_client, mock_consumer):
            with self.mock_yelpkafka_consumer(
            ) as (mock_get_watermarks, mock_commit):

                mock_consumer.return_value.fetch_offsets = group_offsets
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()

                mock_get_watermarks.assert_called_with(
                    mock_client.return_value,
                    [consumer.topic]
                )
                assert mock_consumer.return_value.fetch_offsets == expected_offsets
                assert mock_consumer.return_value.offsets == expected_offsets
                mock_commit.assert_called_once_with(consumer)

    def test__invalid_offsets_below_lowmark_get_earliest(self, cluster):
        config = KafkaConsumerConfig(
            cluster=cluster,
            group_id='test_group',
            client_id='test_client_id',
            auto_offset_reset='smallest'
        )
        group_offsets = {0: 12, 1: 3}
        expected_offsets = {0: 12, 1: 6}
        with mock_kafka() as (mock_client, mock_consumer):
            with self.mock_yelpkafka_consumer(
            ) as (mock_get_watermarks, mock_commit):

                mock_consumer.return_value.fetch_offsets = group_offsets
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()

                mock_get_watermarks.assert_called_with(
                    mock_client.return_value,
                    [consumer.topic]
                )
                assert mock_consumer.return_value.fetch_offsets == expected_offsets
                assert mock_consumer.return_value.offsets == expected_offsets
                mock_commit.assert_called_once_with(consumer)

    def test_invalid_offsets_above_highmark_get_latest(self, config):
        group_offsets = {0: 345, 1: 12}
        expected_offsets = {0: 30, 1: 12}
        with mock_kafka() as (mock_client, mock_consumer):
            with self.mock_yelpkafka_consumer(
            ) as (mock_get_watermarks, mock_commit):

                mock_consumer.return_value.fetch_offsets = group_offsets
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()

                mock_get_watermarks.assert_called_with(
                    mock_client.return_value,
                    [consumer.topic]
                )
                assert mock_consumer.return_value.fetch_offsets == expected_offsets
                assert mock_consumer.return_value.offsets == expected_offsets
                mock_commit.assert_called_once_with(consumer)

    def test_invalid_offsets_above_highmark_get_earliest(self, cluster):
        config = KafkaConsumerConfig(
            cluster=cluster,
            group_id='test_group',
            client_id='test_client_id',
            auto_offset_reset='smallest'
        )
        group_offsets = {0: 47, 1: 53}
        expected_offsets = {0: 5, 1: 6}
        with mock_kafka() as (mock_client, mock_consumer):
            with self.mock_yelpkafka_consumer(
            ) as (mock_get_watermarks, mock_commit):

                mock_consumer.return_value.fetch_offsets = group_offsets
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()

                mock_get_watermarks.assert_called_with(
                    mock_client.return_value,
                    [consumer.topic]
                )
                assert mock_consumer.return_value.fetch_offsets == expected_offsets
                assert mock_consumer.return_value.offsets == expected_offsets
                mock_commit.assert_called_once_with(consumer)

    def test_close(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            with contextlib.nested(
                mock.patch.object(KafkaSimpleConsumer, '_validate_offsets', autospec=True),
                mock.patch.object(KafkaSimpleConsumer, 'commit', autospec=True),
            ) as (_, mock_commit):
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                consumer.close()
                mock_commit.assert_called_once_with(consumer)
                mock_client.return_value.close.assert_called_once_with()

    def test_close_no_commit(self, cluster):
        config = KafkaConsumerConfig(
            cluster=cluster,
            group_id='test_group',
            client_id='test_client_id',
            auto_commit=False
        )
        with mock_kafka() as (mock_client, mock_consumer):
            with contextlib.nested(
                mock.patch.object(KafkaSimpleConsumer, '_validate_offsets', autospec=True),
                mock.patch.object(KafkaSimpleConsumer, 'commit', autospec=True),
            ) as (_, mock_commit):
                mock_obj = mock_consumer.return_value
                mock_obj.auto_commit = False
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                consumer.close()
                assert not mock_commit.called
                mock_client.return_value.close.assert_called_once_with()

    def test_commit_all_partittions(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer, '_validate_offsets', autospec=True):
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                consumer.commit()
                mock_consumer.return_value.commit.assert_called_once_with()

    def test_commit_few_partitions(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer, '_validate_offsets', autospec=True):
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                topic_partitions = ['partition1', 'partition2']
                consumer.commit(topic_partitions)
                mock_consumer.return_value.commit.assert_called_once_with(topic_partitions)


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
                mock.patch.object(
                    KafkaSimpleConsumer,
                    '__iter__',
                    return_value=message_iterator
                ),
                mock.patch.object(KafkaSimpleConsumer, 'commit'),
            ) as (_, _, mock_commit):
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
                mock_commit.assert_called_once_with()
                mock_client.return_value.close.assert_called_once_with()

    def test_process_error(self, config):
        message_iterator = iter([
            Message(1, 12345, 'key1', 'value1'),
            Message(1, 12346, 'key2', 'value2'),
            Message(1, 12347, 'key1', 'value3'),
        ])
        with mock_kafka() as (mock_client, _):
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

    def test_set_process_name(self, config):
        consumer = KafkaConsumerBase(
            'my_very_extraordinarily_elongated_topic_name',
            config, ['1', '2', '3', '4', '5'])
        with mock.patch('yelp_kafka.consumer.setproctitle') as mock_setproctitle:
            consumer.set_process_name()
            expected_name = \
                '%s-my_very_extraordinarily_elongated_topic_name-[\'1\', \'2\', \'3\', \'4\', \'5\']' % (getproctitle())
            mock_setproctitle.assert_called_with(expected_name)
