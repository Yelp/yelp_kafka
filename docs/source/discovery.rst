.. _discovery:

yelp_kafka.discovery
====================
Most of these functions are very Yelp specific and may not be applicable in
your use case. You can instead create your cluster configuration by:

.. code-block:: python

    from yelp_kafka.config import ClusterConfig
    cluster_config = ClusterConfig(
        type="service",
        name="cluster",
        broker_list=["cluster-elb-1:9092"],
        zookeeper="11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1",
    )


.. automodule:: yelp_kafka.discovery
    :members:

