.. Yelp Kafka documentation master file, created by
   sphinx-quickstart on Thu Jan 22 14:07:09 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Yelp Kafka's documentation!
======================================

Yelp_kafka is a library to interact with kafka at Yelp. Yelp_kafka is a wrapper around kafka-python.
Yelp_kafka supports consumer group and multiprocessing consumer group, that allow multiple consumer instances to coordinate with each other while consuming messages from different kafka partitions. See :ref:`consumer_group`.
It also provides consumer offsets validation.

.. note:: 
   
   You need to have libsnappy1 installed in your system in order to be able to consume/produce compressed messages.

Contents:

.. toctree::
   :maxdepth: 2

   discovery
   config
   consumer
   partitioner
   consumer_group
   error


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

