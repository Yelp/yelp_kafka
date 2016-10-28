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

import logging

from kafka.common import KafkaUnavailableError


log = logging.getLogger(__name__)


def get_kafka_topics(kafkaclient):
    """Connect to kafka and fetch all the topics/partitions."""
    try:
        kafkaclient.load_metadata_for_topics()
    except KafkaUnavailableError:
        # Sometimes the kafka server closes the connection for inactivity
        # in this case the second call should succeed otherwise the kafka
        # server is down and we should fail
        log.debug("First call to kafka for loading metadata failed."
                  " Trying again.")
        kafkaclient.load_metadata_for_topics()
    return kafkaclient.topic_partitions


def make_scribe_topic(stream, datacenter):
    """Get a scribe topic name

    :param stream: scribe stream name
    :param datacenter: datacenter name
    :returns: topic name
    """
    return "scribe.{0}.{1}".format(datacenter, stream)


def _split_topic_name(topic_name):
    tokens = topic_name.split(".", 2)
    if len(tokens) < 3 or tokens[0] != "scribe":
        raise ValueError("Encountered wrongly formatted topic %s" % topic_name)
    else:
        return tokens


def extract_datacenter(topic_name):
    """Get the datacenter from a kafka topic name

    :param topic_name: Kafka topic name
    :returns: datacenter
    :raises: ValueError if the topic name does not conform to the expected
             format: "scribe.<datacenter>.<stream name>"
    """
    return _split_topic_name(topic_name)[1]


def extract_stream_name(topic_name):
    """Get the stream name from a kafka topic name

    :param topic_name: Kafka topic name
    :returns: stream name
    :raises: ValueError if the topic name does not conform to the expected
               format: "scribe.<datacenter>.<stream name>"
    """
    return _split_topic_name(topic_name)[2]
