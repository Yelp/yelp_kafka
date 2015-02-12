import mock

from yelp_kafka.testing.kafka_mock import mock_kafka_python


class TestSmoke(object):

    def test_send_then_receive(self):
        with mock_kafka_python() as (Client, Producer, KeyedProducer, Consumer):
            client = Client(
                mock.ANY,
            )

            producer = Producer(
                client,
            )

            producer.send_messages(
                'test_topic',
                'some message 5',
                'some message 6',
            )

            consumer = Consumer(
                client,
                group='test_group_name',
                topic='test_topic',
            )

            messages = consumer.get_messages(count=2)
            assert len(messages) == 2
            assert [msg.offset for msg in messages] == [0, 1]
            assert [msg.message.value for msg in messages] == ['some message 5', 'some message 6']

    def test_send_then_receive_with_keys(self):
        with mock_kafka_python() as (Client, Producer, KeyedProducer, Consumer):
            client = Client(
                mock.ANY,
            )

            producer = KeyedProducer(
                client,
            )

            producer.send_messages(
                'test_topic',
                0,
                'some message 5',
                'some message 6',
            )

            consumer = Consumer(
                client,
                group='test_group_name',
                topic='test_topic',
            )

            messages = consumer.get_messages(count=2)
            assert len(messages) == 2
            assert [msg.offset for msg in messages] == [0, 1]
            assert [msg.message.value for msg in messages] == ['some message 5', 'some message 6']
            assert [msg.message.key for msg in messages] == [0, 0]
