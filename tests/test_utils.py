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

import mock
import pytest
from kafka.common import KafkaUnavailableError

from yelp_kafka import utils


def test_make_scribe_topic():
    expected = 'scribe.datacenter.scribe_stream'
    assert expected == utils.make_scribe_topic(
        'scribe_stream', 'datacenter'
    )


def test_get_kafka_topics():
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client = mock.Mock()
    mock_client.topic_partitions = expected
    actual = utils.get_kafka_topics(mock_client)
    assert expected == actual


def test_get_kafka_topics_recover_from_error():
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client = mock.Mock()
    mock_client.topic_partitions = expected
    mock_client.load_metadata_for_topics.side_effect = [KafkaUnavailableError(), None]
    actual = utils.get_kafka_topics(mock_client)
    assert expected == actual


def test_get_kafka_topics_error():
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client = mock.Mock()
    mock_client.topic_partitions = expected
    mock_client.load_metadata_for_topics.side_effect = KafkaUnavailableError('Boom!')
    with pytest.raises(KafkaUnavailableError):
        utils.get_kafka_topics(mock_client)


def test_extract_datacenter():
    topic = "scribe.uswest1-devc.ranger"
    datacenter = utils.extract_datacenter(topic)
    assert datacenter == "uswest1-devc"

    topic = "scribe.uswest1-devc.mylogfile.log"
    datacenter = utils.extract_datacenter(topic)
    assert datacenter == "uswest1-devc"


def test_extract_datacenter_error():
    topic = "scribeuswest1-devcranger"
    with pytest.raises(ValueError):
        utils.extract_datacenter(topic)

    topic = "scribe.uswest1-devcranger"
    with pytest.raises(ValueError):
        utils.extract_datacenter(topic)

    topic = "scribble.uswest1-devc.ranger"
    with pytest.raises(ValueError):
        utils.extract_datacenter(topic)


def test_extract_stream_name():
    topic = "scribe.uswest1-devc.ranger"
    stream = utils.extract_stream_name(topic)
    assert stream == "ranger"

    topic = "scribe.uswest1-devc.mylogfile.log"
    stream = utils.extract_stream_name(topic)
    assert stream == "mylogfile.log"


def test_extract_stream_name_error():
    topic = "scribeuswest1-devcranger"
    with pytest.raises(ValueError):
        utils.extract_stream_name(topic)

    topic = "scribe.uswest1-devcranger"
    with pytest.raises(ValueError):
        utils.extract_stream_name(topic)

    topic = "scribble.uswest1-devc.ranger"
    with pytest.raises(ValueError):
        utils.extract_stream_name(topic)
