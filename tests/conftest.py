# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from yelp_kafka.config import ClusterConfig
from yelp_kafka.config import KafkaConsumerConfig


MOCK_SERVICES_YAML = {
    'service1.main': {'host': 'host1', 'port': 1111},
    'kafka_discovery.main': {'host': 'host2', 'port': 2222}
}


@pytest.yield_fixture
def mock_swagger_yaml():
    with mock.patch(
        'yelp_kafka.config.load_yaml_config',
        return_value=MOCK_SERVICES_YAML,
        create=True,
    ) as m:
        with mock.patch('os.path.isfile', return_value=True):
            yield m


@pytest.fixture
def cluster():
    return ClusterConfig(
        'cluster_type', 'mycluster', ['test_broker:9292'], 'test_cluster'
    )


@pytest.fixture
def mock_pre_rebalance_cb():
    return mock.Mock()


@pytest.fixture
def mock_post_rebalance_cb():
    return mock.Mock()


@pytest.fixture
def config(
    cluster,
    mock_pre_rebalance_cb,
    mock_post_rebalance_cb
):
    return KafkaConsumerConfig(
        cluster=cluster,
        group_id='test_group',
        client_id='test_client_id',
        partitioner_cooldown=0.5,
        pre_rebalance_callback=mock_pre_rebalance_cb,
        post_rebalance_callback=mock_post_rebalance_cb
    )
