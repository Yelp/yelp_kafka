# -*- coding: utf-8 -*-
"""
Tests for `yelp_kafka.producer` module.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from kafka import SimpleProducer

from yelp_kafka import metrics
from yelp_kafka.config import ClusterConfig
from yelp_kafka.error import YelpKafkaError
from yelp_kafka.producer import YelpKafkaProducerMetrics
from yelp_kafka.producer import YelpKafkaSimpleProducer


@pytest.yield_fixture
def mock_metrics_responder():
    def generate_mock(*args, **kwargs):
        return mock.MagicMock()

    with mock.patch('yelp_kafka.yelp_metrics_responder.MeteoriteMetrics', autospec=True) as mock_meteorite:
        # Different mock for each timer creation
        mock_meteorite.get_timer_emitter.side_effect = generate_mock
        yield mock_meteorite


@pytest.yield_fixture
def mock_kafka_send_messages():
    with mock.patch(
        'kafka.SimpleProducer.send_messages',
        spec=SimpleProducer.send_messages,
    ) as mock_send_messages:
        yield mock_send_messages


@pytest.fixture
def mock_kafka_client():
    return mock.Mock(client_id='test_id')


@pytest.fixture()
def mock_cluster_config():
    return mock.Mock(type='test_cluster_type', name='mock_cluster', spec=ClusterConfig)


@pytest.fixture
def mock_kafka_producer(
    mock_kafka_client,
    mock_metrics_responder,
    mock_kafka_send_messages,
    mock_cluster_config,
):
    return YelpKafkaSimpleProducer(
        client=mock_kafka_client,
        cluster_config=mock_cluster_config,
        metrics_responder=mock_metrics_responder
    )


@pytest.fixture
def mock_producer_metrics(
    mock_kafka_client,
    mock_metrics_responder,
    mock_cluster_config,
):
    return YelpKafkaProducerMetrics(
        client=mock_kafka_client,
        cluster_config=mock_cluster_config,
        metrics_responder=mock_metrics_responder
    )


def test_setup_metrics(
    mock_kafka_client,
    mock_metrics_responder,
    mock_cluster_config,
):
    # setup metrics called at init
    YelpKafkaProducerMetrics(
        client=mock_kafka_client,
        cluster_config=mock_cluster_config,
        metrics_responder=mock_metrics_responder
    )
    assert mock_metrics_responder.get_timer_emitter.call_count == len(metrics.TIME_METRIC_NAMES)


def test_send_kafka_metrics(mock_producer_metrics):
    # Test sending a time metrics
    metric = next(iter(metrics.TIME_METRIC_NAMES))
    mock_producer_metrics._send_kafka_metrics(metric, 10)
    mock_producer_metrics.metrics_responder.record.assert_called_once_with(
        mock_producer_metrics. _get_timer(metric),
        10000
    )

    # Create unknown metric timer
    mock_producer_metrics._create_timer('unknown_metric')
    mock_producer_metrics._send_kafka_metrics('unknown_metric', 10)
    assert mock_producer_metrics._get_timer('unknown_metric').record.call_count == 0


def test_send_msg_to_kafka_success(
    mock_kafka_producer,
    mock_kafka_send_messages,
):
    mock_msg = mock.Mock()
    mock_kafka_producer.send_messages('test_topic', mock_msg)
    mock_kafka_send_messages.assert_called_once_with('test_topic', mock_msg)


def test_send_task_to_kafka_failure(
    mock_kafka_producer,
    mock_metrics_responder,
    mock_kafka_send_messages,
):
    mock_msg = mock.Mock()
    mock_kafka_send_messages.side_effect = [YelpKafkaError]

    with pytest.raises(YelpKafkaError):
        mock_kafka_producer.send_messages('test_topic', mock_msg)

    mock_kafka_send_messages.assert_called_once_with('test_topic', mock_msg)
    mock_kafka_producer.metrics.metrics_responder.record.assert_called_once_with(
        mock_kafka_producer.metrics.kafka_enqueue_exception_count,
        1
    )
