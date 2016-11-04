Yelp_Kafka v\ |version|
***********************

Yelp_Kafka is a library to interact with Kafka. Before reading about Yelp_Kafka, you should know the basics of Kafka. If topic and topic partition are obscure concepts to you, we recommend you to read the introduction of `Kafka documentation`_.
Yelp_Kafka is a wrapper around kafka-python that provides some Yelp specific functions for cluster discovery in addition to custom consumers and producers.
Yelp_Kafka supports consumer groups and multiprocessing consumer groups, that allow multiple
consumer instances to coordinate with each other while consuming messages from different Kafka partitions (see :ref:`consumer_group`).

.. _Kafka documentation: http://kafka.apache.org/documentation.html#introduction

.. toctree::
   :maxdepth: -1

   self
   getting_started
   discovery
   config
   producer
   consumer
   partitioner
   consumer_group
   error
   utils
   monitoring
   offsets


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

