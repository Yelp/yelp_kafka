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

import subprocess
import time
import uuid
from multiprocessing import Process
from multiprocessing import Queue

from kafka import KafkaClient
from kafka.common import ConsumerTimeout
from six.moves.queue import Empty

from yelp_kafka.config import ClusterConfig
from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.consumer_group import KafkaConsumerGroup
from yelp_kafka.producer import YelpKafkaSimpleProducer


ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def create_topic(topic_name, replication_factor, partitions):
    cmd = ['/usr/bin/kafka-topics', '--create',
           '--zookeeper', ZOOKEEPER_URL,
           '--replication-factor', str(replication_factor),
           '--partitions', str(partitions),
           '--topic', topic_name]
    subprocess.check_call(cmd)

    # It may take a little moment for the topic to be ready for writing.
    time.sleep(5)


def create_random_topic(replication_factor, partitions):
    topic_name = str(uuid.uuid1())
    create_topic(topic_name, replication_factor, partitions)
    return topic_name


def test_simple_consumer():
    topic = create_random_topic(1, 1)

    messages = [str(i).encode("UTF-8") for i in range(100)]

    cluster_config = ClusterConfig(None, None, [KAFKA_URL], ZOOKEEPER_URL)
    producer = YelpKafkaSimpleProducer(
        cluster_config=cluster_config,
        report_metrics=False,
        client=KafkaClient(KAFKA_URL),
    )
    producer.send_messages(topic, *messages)

    config = KafkaConsumerConfig(
        'test',
        cluster_config,
        auto_offset_reset='smallest',
        auto_commit=False,
        consumer_timeout_ms=1000
    )
    consumer = KafkaSimpleConsumer(topic, config)

    with consumer:
        for expected_offset in range(100):
            message = consumer.get_message()
            assert message.offset == expected_offset
            assert message.partition == 0
            assert message.value == str(expected_offset).encode("UTF-8")


def test_kafka_consumer_group_one_consumer_one_partition():
    run_kafka_consumer_group_test(1, 1)


def test_kafka_consumer_group_one_consumer_two_partitions():
    run_kafka_consumer_group_test(1, 2)


def test_kafka_consumer_group_two_consumers_one_partition():
    run_kafka_consumer_group_test(2, 1)


def test_kafka_consumer_group_two_consumers_two_partitions():
    run_kafka_consumer_group_test(5, 5)


def run_kafka_consumer_group_test(num_consumers, num_partitions):
    topic = create_random_topic(1, num_partitions)
    cluster_config = ClusterConfig(None, None, [KAFKA_URL], ZOOKEEPER_URL)
    config = KafkaConsumerConfig(
        'test',
        cluster_config,
        auto_offset_reset='smallest',
        partitioner_cooldown=5,
        auto_commit_interval_messages=1,
    )

    queue = Queue()

    def create_consumer():
        def consume():
            consumer = KafkaConsumerGroup([topic], config)
            with consumer:
                while True:
                    try:
                        message = consumer.next()
                        queue.put(message)
                        consumer.task_done(message)
                    except ConsumerTimeout:
                        return

        p = Process(target=consume)
        p.daemon = True
        return p

    consumer_processes = [create_consumer() for _ in range(num_consumers)]

    for consumer_process in consumer_processes:
        consumer_process.start()

    producer = YelpKafkaSimpleProducer(
        cluster_config=cluster_config,
        report_metrics=False,
        client=KafkaClient(KAFKA_URL),
    )
    for i in range(100):
        producer.send_messages(topic, str(i).encode("UTF-8"))

    # wait until all 100 messages have been consumed
    while queue.qsize() < 100:
        time.sleep(0.1)

    received_messages = []
    while True:
        try:
            message = queue.get(block=True, timeout=0.5)
        except Empty:
            break
        received_messages.append(int(message.value))

    assert [i for i in range(100)] == sorted(received_messages)
