Yelp_Kafka v\ |version|
=======================

Yelp_Kafka is a library to interact with Kafka at Yelp. Before reading about Yelp_Kafka, you should at least have a clear idea about what a Kafka topic and topic partitions are. If these are obscure concepts to you, we recommend you to read the introduction of `Kafka documentation`_.
Yelp_Kafka is a wrapper around kafka-python, it fixes some of the issues still present in
the official kafka-python release and provides some Yelp
specific functions for cluster discovery.
Yelp_Kafka supports consumer groups and multiprocessing consumer groups, that allow multiple
consumer instances to coordinate with each other while consuming messages from different Kafka partitions.

See :ref:`consumer_group`.

.. _Kafka documentation: http://kafka.apache.org/documentation.html#introduction

Getting Started
===============

Standard clusters
-----------------

:py:mod:`yelp_kafka.discovery` provides functions for connecting to any Kafka clusters at Yelp and search topics on it. While in the scribe Kafka cluster the stream name and datacenter identifies a specific topic, in the other clusters there are currently no conventions for topic naming.

Producer
````````

Create a producer for my_topic in the local standard Kafka cluster.

.. code-block:: python

   from yelp_kafka import discovery
   from kafka import SimpleProducer

   # Get a connected KafkaClient from yelp_kafka
   client = discovery.get_kafka_connection('standard')
   # Create the producer and send 2 messages
   producer = SimpleProducer(client)
   producer.send_messages("my_topic", "message1", "message2")


This example makes use of the `SimpleProducer`_ class from kafka-python.

.. seealso:: kafka-python `usage examples`_

.. _usage examples: http://kafka-python.readthedocs.org/en/latest/usage.html
.. _SimpleProducer: http://kafka-python.readthedocs.org/en/latest/apidoc/kafka.producer.html

Consumer
````````

Create a consumer for my_topic in the local standard Kafka cluster:

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer import KafkaSimpleConsumer
   from yelp_kafka.config import KafkaConsumerConfig

   cluster = discovery.get_local_cluster('standard')
   consumer = KafkaSimpleConsumer('my_topic', KafkaConsumerConfig(
       group_id='my_app',
       cluster=cluster,
       auto_offset_reset='smallest',
       auto_commit_every_n=1
   ))
   with consumer:
       for message in consumer:
           print message

Create a consumer group that tails from my_topic in the standard Kafka cluster:

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   cluster = discovery.get_local_cluster('standard')
   config = KafkaConsumerConfig('my_group_id', cluster)

   consumer = KafkaConsumerGroup(['my_topic'], config)
   with consumer:
      for message in consumer:
         print message
         consumer.task_done(message)

See :ref:`group_example` for more information about using KafkaConsumerGroup.

Create a consumer for all topics ending with tools_infra in the standard Kafka
cluster:

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.config import KafkaConsumerConfig
   from kafka import KafkaConsumer

   # If no topics match the pattern, discovery raises DiscoveryError.
   topics, cluster = discovery.search_local_topic_by_regex('standard', '.*tools_infra')
   config = KafkaConsumerConfig(group_id='my_app', cluster=cluster, client_id='my-consumer')
   consumer = KafkaConsumer(topics, **config.get_kafka_consumer_config())
   for message in consumer:
       print message

This example makes use of the `KafkaConsumer`_ from kafka-python. This consumer
class allows to consume from many topics from a Kafka cluster. This solution
cannot be considered efficient for high volume topics, though. The parameter
*client_id* is optional and it identifies the connection between Kafka and the
client.

.. _KafkaConsumer: http://kafka-python.readthedocs.org/en/latest/apidoc/kafka.consumer.html#module-kafka.consumer.kafka

Scribe cluster
--------------

Yelp_Kafka provides some helper functions to interact with the scribe Kafka cluster.
Scribe Kafka is a dedicated cluster for scribe streams. This cluster contains all the logs from
our scribe infrastructure. This has to be considered as a readonly cluster. Indeed, no producers
other than Sekretar are allowed to connect to this cluster, create new topics or write messages on it.

All the topics in the scribe Kafka are named after the scribe stream they represent.
The topic name follows the template: **scribe.<datacenter>.<stream>**.
You usually don't need to generate the topic name, since Yelp_Kafka will do that for you.

The use cases below are the most common when you want to tail a scribe log from Kafka.


Tail a scribe log in the local datacenter using KafkaSimpleConsumer
```````````````````````````````````````````````````````````````````

Yelp_Kafka knows what is both the local scribe cluster and the prefix of the local scribe topic. You can use :py:mod:`yelp_kafka.discovery` and KafkaSimpleConsumer to read messages from it.

Create a KafkaSimpleConsumer to tail from the local ranger log.

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer import KafkaSimpleConsumer
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   topic, cluster = discovery.get_local_scribe_topic('ranger')
   consumer = KafkaSimpleConsumer(topic, KafkaConsumerConfig(
       group_id='my_app',
       cluster=cluster
   ))
   with consumer:
       for message in consumer:
           print message

The code above can be run in a box in devc and it will consume messages from ranger in devc. The same goes for all the other datacenters. Each consumer needs to specify a ``group_id``. The ``group_id`` should represent the application/service that consumer belongs to. Using the topic name or datacenter as part of the consumer group id is not really useful. Kafka already uses the topic name to distinguish between consumers of different topics in the same group id.

.. warning:: You can't have 2 consumers for the same topic/partition in the same consumer group. If you are planning to have multiple instances of the same application for a better scalability and reliability you need to use :ref:`consumer_group`. See :ref:`group_example`.

Tail a scribe log from a specific datacenter using KafkaSimpleConsumer
``````````````````````````````````````````````````````````````````````

You can use :py:func:`yelp_kafka.discovery.get_scribe_topic_in_datacenter` to get the scribe topic that
represents a specific datacenter.

Create a KafkaSimpleConsumer to tail from sfo2 ranger.

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer import KafkaSimpleConsumer
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   topic, cluster = discovery.get_scribe_topic_in_datacenter('ranger', 'sfo2')
   consumer = KafkaSimpleConsumer(topic, KafkaConsumerConfig(
       group_id='my_app',
       cluster=cluster,
       auto_offset_reset='smallest',
       auto_commit_every_n=1
   ))
   with consumer:
       for message in consumer:
           print message

The code about creates a consumer for the ranger log coming from sfo2. The consumer has also set the optional paramenter ``auto_offset_reset`` and ``auto_commit_every_n``. The former instructs the consumer to fetch the earliest (default: oldest) message in the queue if the group offset is too old or not valid, the latter sets the number of message consumed before committing the offset to Kafka (default: 100).

.. note:: The datacenter has to be available from your current runtime env.
.. seealso:: :ref:`config` for all the available configuration options.

Tail a scribe log from all the datacenters using KafkaSimpleConsumer
````````````````````````````````````````````````````````````````````

In order to tail a scribe stream from all the datacenters in the current runtime env we need to create a different consumer for each topic.

.. code-block:: python

   import contextlib
   from yelp_kafka import discovery
   from yelp_kafka.consumer import KafkaSimpleConsumer
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   topics = discovery.get_scribe_topics('ranger')
   consumers = [KafkaSimpleConsumer(topic, KafkaConsumerConfig(
       group_id='my_app',
       cluster=cluster,
   )) for topic, cluster in topics]
   with contextlib.nested(*consumers):
       while True:
           for c in consumers:
               message = c.get_message()
               if message:
                   print message

If the code above is run in prod it creates a consumer for each datacenter and consumes from all of them in a single process.

.. note:: Consuming from big streams is not very efficient when done in a single process. You usually want to have consumers running in parallel on different instances or processes. You can still increase the parallelism by consuming from different partitions in different processes by using :ref:`consumer_group`.

.. _group_example:

Using consumer groups to tail from scribe
`````````````````````````````````````````

Yelp_Kafka currently provides three *consumer group* interfaces for consuming
from Kafka.

- :py:class:`yelp_kafka.consumer_group.MultiprocessingConsumerGroup` is for
  consuming from high volume topics since it starts as many consumer as topic
  partitions. It also handles process monitoring and restart upon failures.

- :py:class:`yelp_kafka.consumer_group.ConsumerGroup` is used for single process
  consumers. Since ConsumerGroup periodically checks for changes in the number
  of partitions, it assures that your consumers will always receive messages
  from all of them.

- :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup` is the recommended
  class to use if you want start multiple instances of your consumer. You may
  start as many instances as you wish (balancing partitions will happen
  automatically), but you can still control when to mark messages as processed
  (via `task_done` and `commit`).

Tailing from a scribe topic using
:py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup`:

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   topic, cluster = discovery.get_local_scribe_topic('ranger')
   config = KafkaConsumerConfig('my_group_id', cluster)

   consumer = KafkaConsumerGroup([topic], config)
   with consumer:
      for message in consumer:
         print message

         # If auto_commit is disabled in KafkaConsumerGroup, then you must call
         # consumer.commit() yourself.
         #
         # auto_commit is enabled by default, so here we are implicitly
         # letting KafkaConsumerGroup decide when to inform Kafka of our
         # completed messages.
         consumer.task_done(message)

Tailing from a scribe topic using
:py:class:`yelp_kafka.consumer_group.ConsumerGroup`:

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import ConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   topic, cluster = discovery.get_local_scribe_topic('ranger')

   def my_process_function(message):
       print message

   consumer = ConsumerGroup(
       topic,
       KafkaConsumerConfig(
           group_id='my_app',
           cluster=cluster
        ),
        my_process_function
   )
   consumer.run()

Contents:
=========

.. toctree::
   :maxdepth: 2

   discovery
   config
   consumer
   partitioner
   consumer_group
   error
   utils
   monitoring


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

