# -*- coding: utf-8 -*-
"""
Tests for `yelp_kafka.producer` module.
"""
import mock
import pytest
from kafka import SimpleProducer

from yelp_kafka import metrics
from yelp_kafka.error import YelpKafkaError
from yelp_kafka.producer import YelpKafkaSimpleProducer


@pytest.yield_fixture
def mock_client_hostname():
    with mock.patch('socket.gethostname', autospec=True) as mock_client_host:
        yield mock_client_host


@pytest.yield_fixture
def mock_yelp_meteorite():
    def generate_mock(*args, **kwargs):
        return mock.MagicMock()

    with mock.patch('yelp_kafka.producer.yelp_meteorite', autospec=True) as mock_meteorite:
        # Different mock for each timer creation
        mock_meteorite.create_timer.side_effect = generate_mock
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


@pytest.fixture
def mock_kafka_producer(
    mock_kafka_client,
    mock_client_hostname,
    mock_yelp_meteorite,
    mock_kafka_send_messages,
):
    return YelpKafkaSimpleProducer(mock_kafka_client)


def test_send_kafka_metrics(mock_kafka_producer):
    # Test sending time metrics
    for name in metrics.TIME_METRIC_NAMES:
        mock_kafka_producer._send_kafka_metrics(name, 10)
        mock_kafka_producer._get_timer(name).record.assert_called_once_with(10000)


def test_send_msg_to_kafka_success(
    mock_kafka_producer,
    mock_kafka_send_messages,
):
    mock_msg = mock.Mock()
    mock_kafka_producer.send_messages('test_topic', mock_msg)
    mock_kafka_send_messages.assert_called_once_with('test_topic', mock_msg)


def test_send_task_to_kafka_failure(
    mock_kafka_producer,
    mock_kafka_send_messages,
):
    mock_msg = mock.Mock()
    mock_kafka_send_messages.side_effect = [YelpKafkaError]

    with pytest.raises(YelpKafkaError):
        mock_kafka_producer.send_messages('test_topic', mock_msg)

    mock_kafka_send_messages.assert_called_once_with('test_topic', mock_msg)
    mock_kafka_producer.kafka_enqueue_exception_count.count.assert_called_once_with(1)
