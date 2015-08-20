import pytest
from yelp_kafka.config import ClusterConfig
from yelp_kafka.config import KafkaConsumerConfig


@pytest.fixture
def cluster():
    return ClusterConfig(
        'cluster_type', 'mycluster', ['test_broker:9292'], 'test_cluster'
    )


@pytest.fixture
def config(cluster):
    return KafkaConsumerConfig(
        cluster=cluster,
        group_id='test_group',
        client_id='test_client_id',
        partitioner_cooldown=0.5,
    )
