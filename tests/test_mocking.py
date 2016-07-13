# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from kafka.common import OffsetAndMessage

from yelp_kafka.testing.kafka_mock import mock_kafka_python


class TestSmoke(object):

    def test_send_then_receive(self):
        with mock_kafka_python() as kafka_mocks:
            client = kafka_mocks.KafkaClient(
                mock.ANY,
            )

            producer = kafka_mocks.SimpleProducer(
                client,
            )

            producer.send_messages(
                'test_topic',
                'some message 5',
                'some message 6',
            )

            consumer = kafka_mocks.SimpleConsumer(
                client,
                group='test_group_name',
                topic='test_topic',
            )

            messages = consumer.get_messages(count=2)
            assert len(messages) == 2
            assert [msg.offset for msg in messages] == [0, 1]
            assert [msg.message.value for msg in messages] == ['some message 5', 'some message 6']

    def test_send_then_receive_with_keys(self):
        with mock_kafka_python() as kafka_mocks:
            client = kafka_mocks.KafkaClient(
                mock.ANY,
            )

            producer = kafka_mocks.KeyedProducer(
                client,
            )

            producer.send_messages(
                'test_topic',
                0,
                'some message 5',
                'some message 6',
            )

            consumer = kafka_mocks.SimpleConsumer(
                client,
                group='test_group_name',
                topic='test_topic',
            )

            messages = consumer.get_messages(count=2)
            assert len(messages) == 2
            assert [msg.offset for msg in messages] == [0, 1]
            assert [msg.message.value for msg in messages] == ['some message 5', 'some message 6']
            assert [msg.message.key for msg in messages] == [0, 0]


@pytest.yield_fixture
def kafka_mocks_with_messages():
    with mock_kafka_python() as kafka_mocks:
        client = kafka_mocks.KafkaClient(
            mock.ANY,
        )

        producer = kafka_mocks.KeyedProducer(
            client,
        )

        producer.send_messages(
            'test_topic',
            0,
            'some message 5',
            'some message 6',
        )
        yield kafka_mocks


def assert_is_offset_and_message(kafka_message):
    assert isinstance(kafka_message, OffsetAndMessage)


def assert_is_partition_message(kafka_message):
    message_partition, offset_and_message = kafka_message
    assert isinstance(message_partition, int)
    assert isinstance(offset_and_message, OffsetAndMessage)


@pytest.mark.usefixtures('kafka_mocks_with_messages')
class TestConsumers(object):

    def test_simple_consumer(self, kafka_mocks_with_messages):
        consumer = kafka_mocks_with_messages.SimpleConsumer(
            client=mock.ANY,
            group='test_group_name',
            topic='test_topic',
        )
        messages = consumer.get_messages(count=2)
        assert len(messages) == 2
        assert [msg.offset for msg in messages] == [0, 1]
        assert [msg.message.value for msg in messages] == ['some message 5', 'some message 6']
        assert [msg.message.key for msg in messages] == [0, 0]

        consumer = kafka_mocks_with_messages.SimpleConsumer(
            client=mock.ANY,
            group='test_group_name2',
            topic='test_topic',
        )
        msg1 = consumer.get_message()
        assert msg1.offset == 0
        assert msg1.message.key == 0
        assert msg1.message.value == 'some message 5'
        msg2 = consumer.get_message()
        assert msg2.offset == 1
        assert msg2.message.key == 0
        assert msg2.message.value == 'some message 6'

        consumer = kafka_mocks_with_messages.SimpleConsumer(
            client=mock.ANY,
            group='test_group_name3',
            topic='test_topic',
        )
        messages = list(consumer)
        assert len(messages) == 2
        assert [msg.offset for msg in messages] == [0, 1]
        assert [msg.message.value for msg in messages] == ['some message 5', 'some message 6']
        assert [msg.message.key for msg in messages] == [0, 0]

    def test_simple_consumer_get_partition_info(self):
        topic = 'random_topic_name'
        with mock_kafka_python() as kmocks:
            client = kmocks.KafkaClient(mock.ANY)
            producer = kmocks.SimpleProducer(client)
            producer.send_messages(topic, *range(7))
            consumer = kmocks.SimpleConsumer(
                client=mock.ANY,
                group='test_group_name',
                topic=topic,
            )

        assert_is_offset_and_message(consumer.get_message())
        assert_is_partition_message(consumer.get_message(get_partition_info=True))
        assert_is_offset_and_message(consumer.get_message(get_partition_info=False))
        assert_is_offset_and_message(consumer.get_message())
        consumer.provide_partition_info()
        assert_is_partition_message(consumer.get_message())
        assert_is_offset_and_message(consumer.get_message(get_partition_info=False))
        assert_is_partition_message(consumer.get_message(get_partition_info=None))
        assert consumer.get_message(get_partition_info=True) is None
        assert consumer.get_message(get_partition_info=False) is None

    def test_simple_consumer_auto_commit(self):
        topic = 'random_topic_name'
        with mock_kafka_python() as kmocks:
            client = kmocks.KafkaClient(mock.ANY)
            producer = kmocks.SimpleProducer(client)
            producer.send_messages(topic, *range(1, 5))
            consumer = kmocks.SimpleConsumer(
                client=mock.ANY,
                group='test_group_name',
                topic=topic,
            )

        consumer.get_messages(count=2)
        assert consumer.get_message().message.value == 3
        assert consumer.get_message().message.value == 4
        assert consumer._offset == 4

    def test_simple_consumer_non_auto_commit(self):
        topic = 'random_topic_name'
        with mock_kafka_python() as kmocks:
            client = kmocks.KafkaClient(mock.ANY)
            producer = kmocks.SimpleProducer(client)
            producer.send_messages(topic, *range(1, 5))
            consumer = kmocks.SimpleConsumer(
                client=mock.ANY,
                group='test_group_name',
                topic=topic,
                auto_commit=False
            )

        consumer.get_messages(count=2)
        assert consumer.get_message().message.value == 3
        assert consumer.get_message().message.value == 4
        assert consumer._offset == 0
        consumer.commit()
        assert consumer._offset == 4

    def test_yelp_consumer(self, kafka_mocks_with_messages):
        consumer = kafka_mocks_with_messages.KafkaSimpleConsumer(
            'test_topic',
            config=mock.ANY,
        )
        messages = consumer.get_messages(count=2)
        assert len(messages) == 2
        assert [msg.offset for msg in messages] == [0, 1]
        assert [msg.value for msg in messages] == ['some message 5', 'some message 6']
        assert [msg.key for msg in messages] == [0, 0]

        consumer = kafka_mocks_with_messages.KafkaSimpleConsumer(
            'test_topic',
            config=mock.ANY,
        )
        msg1 = consumer.get_message()
        assert msg1.offset == 0
        assert msg1.key == 0
        assert msg1.value == 'some message 5'
        msg2 = consumer.get_message()
        assert msg2.offset == 1
        assert msg2.key == 0
        assert msg2.value == 'some message 6'

        consumer = kafka_mocks_with_messages.KafkaSimpleConsumer(
            'test_topic',
            config=mock.ANY,
        )
        messages = list(consumer)
        assert len(messages) == 2
        assert [msg.offset for msg in messages] == [0, 1]
        assert [msg.value for msg in messages] == ['some message 5', 'some message 6']
        assert [msg.key for msg in messages] == [0, 0]
