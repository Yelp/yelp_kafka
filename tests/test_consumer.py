# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

import mock
import pytest
from kafka.common import KafkaError
from kafka.common import OffsetCommitRequest
from setproctitle import getproctitle

from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer import KafkaConsumerBase
from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.consumer import Message
from yelp_kafka.error import ProcessMessageError


@contextlib.contextmanager
def mock_kafka():
    with mock.patch('yelp_kafka.consumer.KafkaClient', autospec=True) as mock_client:
        with mock.patch('yelp_kafka.consumer.SimpleConsumer', autospec=True) as mock_consumer:
            mock_consumer.return_value.auto_commit = True
            yield mock_client, mock_consumer


class TestKafkaSimpleConsumer(object):

    @contextlib.contextmanager
    def mock_yelpkafka_consumer(self):
        with mock.patch.object(KafkaSimpleConsumer, "commit", autospec=True) as mock_commit:
            yield mock_commit

    def test_topic_error(self, config):
        with pytest.raises(TypeError):
            KafkaSimpleConsumer(['test_topic'], config)

    def test_partitions_error(self, config):
        with pytest.raises(TypeError):
            KafkaSimpleConsumer('test_topic', config, partitions='1')

    def test_connect(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            mock_client.return_value = mock.sentinel.client
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_client.assert_called_once_with(
                ['test_broker:9292'],
                client_id='test_client_id'
            )
            assert not mock_consumer.call_args[0]
            kwargs = mock_consumer.call_args[1]
            assert kwargs['topic'] == 'test_topic'.encode()
            assert kwargs['group'] == 'test_group'.encode()

    def test_get_message(self, config):
        with mock_kafka() as (_, mock_consumer):
            mock_obj = mock_consumer.return_value
            # get message should return a tuple (partition_id, (offset,
            # Message)). Message is a namedtuple defined in
            # kafka-python that at least contains key and value.
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
                partition=1,
                offset=12345,
                key='test_key',
                value='test_content',
            )

    def test_close(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(
                KafkaSimpleConsumer,
                'commit',
                autospec=True,
            ) as mock_commit:
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
            with mock.patch.object(
                KafkaSimpleConsumer,
                'commit',
                autospec=True,
            ) as mock_commit:
                mock_obj = mock_consumer.return_value
                mock_obj.auto_commit = False
                consumer = KafkaSimpleConsumer('test_topic', config)
                consumer.connect()
                consumer.close()
                assert not mock_commit.called
                mock_client.return_value.close.assert_called_once_with()

    def test_commit_all_partittions(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            consumer.commit()
            mock_consumer.return_value.commit.assert_called_once_with()

    def test_commit_few_partitions(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            topic_partitions = ['partition1', 'partition2']
            consumer.commit(topic_partitions)
            mock_consumer.return_value.commit.assert_called_once_with(
                topic_partitions,
            )

    def test_commit_message_default(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()

            actual = consumer.commit_message(
                Message(0, 100, 'mykey', 'myvalue'),
            )

            assert actual is True
            mock_client.return_value.send_offset_commit_request \
                .assert_called_once_with(
                    'test_group'.encode(),
                    [OffsetCommitRequest('test_topic'.encode(), 0, 100, None)],
                )

    def test_commit_message_zk(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            config._config['offset_storage'] = 'zookeeper'
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()

            actual = consumer.commit_message(
                Message(0, 100, 'mykey', 'myvalue'),
            )

            assert actual is True
            mock_client.return_value.send_offset_commit_request \
                .assert_called_once_with(
                    'test_group'.encode(),
                    [OffsetCommitRequest('test_topic'.encode(), 0, 100, None)],
                )

    def test_commit_message_kafka(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            config._config['offset_storage'] = 'kafka'
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()

            actual = consumer.commit_message(
                Message(0, 100, 'mykey', 'myvalue'),
            )

            assert actual is True
            assert not mock_client.return_value.send_offset_commit_request.called
            mock_client.return_value.send_offset_commit_request_kafka \
                .assert_called_once_with(
                    'test_group'.encode(),
                    [OffsetCommitRequest('test_topic'.encode(), 0, 100, None)],
                )

    def test_commit_message_dual(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            config._config['offset_storage'] = 'dual'
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()

            actual = consumer.commit_message(
                Message(0, 100, 'mykey', 'myvalue'),
            )

            assert actual is True
            mock_client.return_value.send_offset_commit_request \
                .assert_called_once_with(
                    'test_group'.encode(),
                    [OffsetCommitRequest('test_topic'.encode(), 0, 100, None)],
                )
            mock_client.return_value.send_offset_commit_request_kafka \
                .assert_called_once_with(
                    'test_group'.encode(),
                    [OffsetCommitRequest('test_topic'.encode(), 0, 100, None)],
                )

    def test_commit_message_error(self, config):
        with mock_kafka() as (mock_client, mock_consumer):
            consumer = KafkaSimpleConsumer('test_topic', config)
            consumer.connect()
            mock_client.return_value.send_offset_commit_request \
                .side_effect = KafkaError("Boom!")

            actual = consumer.commit_message(
                Message(0, 100, 'mykey', 'myvalue'),
            )
            assert actual is False


class TestKafkaConsumer(object):

    def test_run_and_terminate(self, config):
        message_iterator = iter([
            Message(1, 12345, 'key1', 'value1'),
            Message(1, 12346, 'key2', 'value2'),
            Message(1, 12347, 'key1', 'value3'),
        ])
        with mock_kafka() as (mock_client, mock_consumer):
            with mock.patch.object(KafkaSimpleConsumer, '__iter__', return_value=message_iterator):
                with mock.patch.object(KafkaSimpleConsumer, 'commit') as mock_commit:
                    consumer = KafkaConsumerBase('test_topic', config)
                    consumer.process = mock.Mock()
                    consumer.initialize = mock.Mock()
                    consumer.dispose = mock.Mock()
                    consumer.terminate()
                    consumer.run()
                    assert consumer.initialize.call_count == 1
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
            with mock.patch.object(
                KafkaSimpleConsumer,
                '__iter__',
                return_value=message_iterator,
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
        with mock.patch(
            'yelp_kafka.consumer.setproctitle',
        ) as mock_setproctitle:
            consumer.set_process_name()
            expected_name = \
                '{procname}-my_very_extraordinarily_elongated_topic_name' \
                '-{messages}'.format(
                    procname=getproctitle(),
                    messages=['1', '2', '3', '4', '5'],
                )
            mock_setproctitle.assert_called_with(expected_name)
