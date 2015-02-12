# -*- coding: utf-8 -*-

import contextlib

import kafka
import mock


class Registrar(object):
    def __init__(self):
        self.topic_registry = {}

    def mock_producer_with_registry(self):

        class MockProducer(object):
            ACK_AFTER_CLUSTER_COMMIT = 2

            def __init__(
                inner_self,
                client,
                async=False,
                req_acks=1,
                ack_timeout=1000,
                codec=None,
                batch_send=False,
                batch_send_every_n=20,
                batch_send_every_t=20,
                random_start=False
            ):
                inner_self._client = client

            def send_messages(inner_self, topic, *messages):
                # inner_self so we can address the parent object Registrar
                # with self, thus accessing global test state.
                topic_list = self.topic_registry.setdefault(topic, [])
                current_offset = len(topic_list)
                new_messages = [
                    kafka.common.OffsetAndMessage(
                        offset=i + current_offset,
                        message=kafka.common.Message(
                            magic=0,
                            attributes=0,
                            key=None,
                            value=msg,
                        )
                    ) for i, msg in enumerate(messages)]
                topic_list.extend(new_messages)

        return MockProducer

    def mock_keyed_producer_with_registry(self):
        class MockKeyedProducer(object):
            ACK_AFTER_CLUSTER_COMMIT = 2

            def __init__(
                inner_self,
                client,
                async=False,
                req_acks=1,
                ack_timeout=1000,
                codec=None,
                batch_send=False,
                batch_send_every_n=20,
                batch_send_every_t=20,
                random_start=False
            ):
                inner_self._client = client
                self._client = client

            def send_messages(inner_self, topic, key, *messages):
                # XXX: By default, don't support multiple partitions in the
                # mock. If we need testing support for this, add it later.

                # inner_self so we can address the parent object Registrar
                # with self, thus accessing global test state.
                topic_list = self.topic_registry.setdefault(topic, [])
                current_offset = len(topic_list)
                new_messages = [
                    kafka.common.OffsetAndMessage(
                        offset=i + current_offset,
                        message=kafka.common.Message(
                            magic=0,
                            attributes=0,
                            key=key,
                            value=msg,
                        )
                    ) for i, msg in enumerate(messages)]
                topic_list.extend(new_messages)

        return MockKeyedProducer

    def mock_consumer_with_registrar(self):
        class MockConsumer(object):
            """I personally don't need this to be super hardcore, but anyone who
            wants to, feel free to add auto_commit and fetch_last_known_offset
            support."""
            def __init__(
                inner_self,
                client,
                group,
                topic,
                auto_commit=True,
                partitions=None,
                auto_commit_every_n=100,
                auto_commit_every_t=5000,
                fetch_size_bytes=4096,
                buffer_size=4096,
                max_buffer_size=32768,
                iter_timeout=None
            ):
                # XXX: This just snapshots the current topic. New messages produced
                # won't make it into here.If you need this, build it :)
                inner_self._topic = list(self.topic_registry.get(topic, []))
                inner_self._offset = 0

            def get_messages(inner_self, count=1, block=True, timeout=0.10000000000000001):
                # inner_self so we can address the parent object Registrar
                # with self, thus accessing global test state.
                new_offset = min(inner_self._offset + count, len(inner_self._topic))
                old_offset = inner_self._offset
                inner_self._offset = new_offset
                return inner_self._topic[old_offset:new_offset]
        return MockConsumer


@contextlib.contextmanager
def mock_kafka_python():
    registrar = Registrar()
    with contextlib.nested(
        mock.patch.object(kafka, 'KafkaClient', autospec=True),
        mock.patch.object(kafka, 'SimpleProducer', registrar.mock_producer_with_registry()),
        mock.patch.object(kafka, 'KeyedProducer', registrar.mock_keyed_producer_with_registry()),
        mock.patch.object(kafka, 'SimpleConsumer', registrar.mock_consumer_with_registrar()),
    ) as mocked_stuff:
        yield mocked_stuff
