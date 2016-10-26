Getting Started
===============

Standard clusters
-----------------

:py:mod:`yelp_kafka.discovery` provides functions for connecting to any Kafka clusters at Yelp and searching topics on it. While in the scribe Kafka cluster the stream name and datacenter identifies a specific topic, in the other clusters there are currently no conventions for topic naming.

.. _producer_example:

Producer
^^^^^^^^

Create a producer for my_topic in the local standard Kafka cluster.

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.producer import YelpKafkaSimpleProducer
   from kafka import KafkaClient
   from kafka.common import ConsumerTimeout
   from kafka.common import FailedPayloadsError
   from kafka.common import KafkaUnavailableError
   from kafka.common import LeaderNotAvailableError
   from kafka.common import NotLeaderForPartitionError
   from kafka.common import UnknownTopicOrPartitionError

   # Get cluster configuration (We recommend using the region cluster which
   # defaults to the region of the caller (can be overridden) but superregion
   # or name cluster discovery is possible)
   cluster_config = discovery.get_region_cluster('standard', 'my-client-id')
   # Create a kafka Client
   client = KafkaClient(cluster_config.broker_list, client_id='my-client-id')
   # Create the producer and send 2 messages
   producer = YelpKafkaSimpleProducer(
       client=client,
       cluster_config=cluster_config,
       report_metrics=True,
   )
   try:
       producer.send_messages("my_topic", "message1", "message2")
   except (
       FailedPayloadsError,
       KafkaUnavailableError,
       LeaderNotAvailableError,
       NotLeaderForPartitionError,
       UnknownTopicOrPartitionError,
   ):
       # Usually we want to retry a certain number of times when encountering these exceptions
       pass



This example makes use of the `YelpKafkaSimpleProducer`_ class from yelp_kafka.

``client_id`` identifies the client connection in Kafka and it is used by Kafka 0.9.0 to enforce
quota limit per client. We recommend to use a ``client_id`` that represents the application.

In the example there are some exceptions that usually should be safe to just retry.

``KafkaUnavailableError`` can happen when the metadata request to Kafka fails, this
request is broker unaware so a simple retry would pick another broker of the cluster and possibly succeed.

``LeaderNotAvailableError`` and ``NotLeaderForPartitionError`` may happen during a cluster
rolling restart or upon broker failure. In this case a new leader will be elected, kafka-python
by default refreshes the metadata when encountering these errors, thus upon retry it would
hopefully use a new leader and succeed. However, Kafka doesn't give us any guarantee on how quickly
a new leader will be elected. We measured that for small clusters the elections happens in the order
of hundreds of ms but for large clusters it can take up to several seconds.
Usually an application should retry for a limited amount of time and then consider the request failed and react accordingly.

Finally, ``FailedPayloadsError`` may happen in many cases, for example when a leader is missing
or the connection fails in the middle of a request. Metadata is automatically refreshed for this exception as well.

.. seealso:: kafka-python `usage examples`_

.. _usage examples: http://kafka-python.readthedocs.org/en/v0.9.5/usage.html
.. _SimpleProducer: http://kafka-python.readthedocs.org/en/v0.9.5/apidoc/kafka.producer.html

.. _consumer_group_example:

Consumer
^^^^^^^^

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig
   from yelp_kafka.error import PartitionerError
   from kafka.common import ConsumerTimeout
   from kafka.common import FailedPayloadsError
   from kafka.common import KafkaUnavailableError
   from kafka.common import LeaderNotAvailableError
   from kafka.common import NotLeaderForPartitionError

   # Get cluster configuration (We recommend using the region cluster which
   # defaults to the region of the caller (can be overridden) but superregion
   # or name cluster discovery is possible)
   cluster_config = discovery.get_region_cluster('standard', 'my-client-id')
   config = KafkaConsumerConfig(
       'my_group_id',
       cluster_config,
       auto_offset_reset='smallest',
       auto_commit_interval_ms=60000,  # By default 60 seconds
       auto_commit_interval_messages=100,  # By default 100 messages
       consumer_timeout_ms=100,  # By default 100 ms
   )

   consumer = KafkaConsumerGroup(['my_topic'], config)

   def consume_messages(consumer):
       while True:
           try:
               message = consumer.next()
               print message.value
               consumer.task_done(message)
               # If auto_commit is disabled in KafkaConsumerGroup, then you must call
               # consumer.commit() yourself.
               #
               # auto_commit is enabled by default, so here we are implicitly
               # letting KafkaConsumerGroup decide when to inform Kafka of our
               # completed messages.

           except ConsumerTimeout:
               # Applications usually just ignore the ConsumerTimeout
               # exception or check a termination flag.
               pass
           except (FailedPayloadsError, KafkaUnavailableError, LeaderNotAvailableError, NotLeaderForPartitionError):
               # See producer example above, usually these exceptions should be retried

   while True:
       try:
           with consumer:
               consume_messages(consumer)
       except PartitionerError:
           # In this case we can't just retry, because the connection to zookeeper is lost.
           # We can either fail the application or re-initialize the consumer connection as
           # done in this example.
           pass

See :ref:`producer_example` for more information about the exceptions to retry.
See :ref:`consumer_group_example` for more information about using KafkaConsumerGroup.
The ``group_id`` should represent the application/service that consumer belongs to. It is recommended to follow the naming 
convention ``services.<descriptive_name>`` or ``batch.<descriptive_name>`` to enable `consumer monitoring`_ in SignalFx.

.. seealso:: :ref:`config` for all the available configuration options.

.. _consumer monitoring: https://trac.yelpcorp.com/wiki/Kafka#ConsumerMonitoring

.. note:: When bootstrapping a new consumer group it is usually recommended to set ``auto_offset_reset`` to **largest**.
          It assures that a huge amount of past messages are not consumed the first time a consumer is launched.
          ``auto_offset_reset`` should be set to **smallest** immediately after the first run (after the offsets are committed for the first time).
          When ``auto_offset_reset`` is set to **smallest** no messages are lost when adding new partitions.
          
Create a consumer for all topics ending with mytopic in the standard local region kafka
cluster:

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.config import KafkaConsumerConfig
   from kafka import KafkaConsumer

   # If no topics match the pattern, discovery raises DiscoveryError.
   region_cluster = discovery.get_region_cluster('standard', 'my-client-id')
   topics, cluster = discovery.search_topics_by_regex('.*mytopic', [region_cluster])
   config = KafkaConsumerConfig(group_id='my_app', cluster=cluster, client_id='my-consumer')
   consumer = KafkaConsumer(topics, **config.get_kafka_consumer_config())
   for message in consumer:
       print message

This example makes use of the `KafkaConsumer`_ from kafka-python. This consumer
class should be considered deprecated and should not be used anymore. 

.. _KafkaConsumer: http://kafka-python.readthedocs.org/en/v0.9.5/apidoc/kafka.consumer.html#module-kafka.consumer.kafka

Scribe cluster
--------------

Yelp_Kafka provides some helper functions to interact with the scribe Kafka clusters.
Scribe Kafka is a dedicated cluster for scribe streams. This cluster contains all the logs from
our scribe infrastructure. This has to be considered as a readonly cluster. In fact, no producers
other than Sekretar are allowed to connect to this cluster, create new topics or write messages to it.
In addition new partitions and topics can be automatically created in the scribe Kafka cluster at any time.
You should never rely on the number of partitions for a scribe topic.

All the topics in the scribe Kafka are named after the scribe stream they represent.
You usually don't need to generate the topic name, since Yelp_Kafka will do that for you.

The use cases below are the most common when you want to tail a scribe log from Kafka.

Tail a scribe log in the local region using KafkaConsumerGroup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Yelp_Kafka knows what is both the local region scribe cluster and the prefix of the local scribe topic.
In :py:mod:`yelp_kafka.discovery` there are many functions to search for scribe topics in Kafka.

.. note:: The local region cluster refers to the scribe cluster for the region your application is currently running.
          We have a separate scribe Kafka cluster for each region (this reflects the scribe logs region).
          However, Paasta unit of deployment is superregion. This means that if a consumer is deployed
          in the norcal-prod Paasta cluster it may consume either logs from uswest1-prod or sfo12-prod.
          It is recommended that consumers that run on Paasta never refer to the local cluster but always
          explicitly configure the name of the cluster they want to read from. 

Create a KafkaConsumerGroup to tail from the local region ranger log.

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   [(topics, cluster)] = discovery.get_region_logs_stream('my-client-id', 'ranger')
   consumer = KafkaConsumerGroup(topics, KafkaConsumerConfig(
       group_id='my_app',
       client_id='my_client_id',
       cluster=cluster,
   ))
   # Actual consumer code...


The code above can be run on a devc box and it will consume messages from ranger in devc.
The same goes for all the other regions. Using the topic name or region as part of the consumer group id is not really useful.
Kafka already uses the topic name to distinguish between consumers of different topics in the same group id.
See :ref:`consumer_group_example` for more details about the consumer code. 

Tail a scribe log in the local superregion using KafkaConsumerGroup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create KafkaConsumerGroup(s) to tail from the local superregion ranger log.

.. code-block:: python

   import contextlib
   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   topics_cluster = discovery.get_superregion_logs_stream('my-client-id', 'ranger')
   consumers = [KafkaConsumerGroup(topics, KafkaConsumerConfig(
       group_id='my_app',
       client_id='my_client_id',
       cluster=cluster,
   )) for topics, cluster in topics_cluster.items()]
   with contextlib.nested(*consumers):
       while True:
           # Iterate over the list of consumers to consume messages



Tail a scribe log from a specific region
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   [(topics, cluster)] = discovery.get_region_logs_stream('my-client-id', 'ranger', 'uswest1-prod')
   # get scribe topics returns a list of topics but there may only be a single topic
   # matching a scribe log for each cluster.

   consumer = KafkaConsumerGroup(topics, KafkaConsumerConfig(
       group_id='my_app',
       cluster=cluster,
   ))
   # Actual consumer code

Tail a scribe log from a specific superregion using KafkaConsumerGroup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a KafkaConsumerGroup(s) to tail from sfo12-prod ranger.

.. code-block:: python

   import contextlib
   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import KafkaConsumerConfig

   # If the stream does not exist, discovery raises DiscoveryError.
   topics_cluster  = discovery.get_superregion_logs_stream('my-client-id', 'ranger', 'sfo12-prod')
   consumers = [KafkaConsumerGroup(topics, KafkaConsumerConfig(
       group_id='my_app',
       cluster=cluster,
   )) for topics, cluster in topics_cluster.items()]
   with contextlib.nested(*consumers):
      while True:
          # Iterate over the list of consumers to consume messages

The code above creates consumer(s) for the ranger log coming from sfo12-prod.

.. note:: The superregion has to be available from your current runtime env.

Tail a scribe log from all the data centers using KafkaConsumerGroup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most likely you shouldn't ever have to do this. If you do and have a good reason
please ask in #kafka or come to dist-sys office hours for guidance

.. warning:: Consuming from multiple clusters within the same process is safe when there
             is only one consumer instance running for the same consumer group. Please place
             distsys_streaming on any code review


Other consumer groups
^^^^^^^^^^^^^^^^^^^^^

Yelp_Kafka currently provides three *consumer group* interfaces for consuming
from Kafka.

- :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup` is the recommended
  class to use if you want start multiple instances of your consumer. You may
  start as many instances as you wish (balancing partitions will happen
  automatically), and you can control when to mark messages as processed (via
  `task_done` and `commit`).

- :py:class:`yelp_kafka.consumer_group.MultiprocessingConsumerGroup` is for
  consuming from high volume topics since it starts as many consumer processes as topic
  partitions. It also handles process monitoring and restart upon failures.

- :py:class:`yelp_kafka.consumer_group.ConsumerGroup` provides the same set of
  features as KafkaConsumerGroup, but with a less convenient interface.
  This class is considered deprecated.

Reporting metrics to SignalFx
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're using :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup`, you
can send metrics on request latency and error counts. This is on by default
for yelp_kafka and uses an instance of
:py:class:`yelp_kafka.metrics_responder.MetricsResponder` for reporting metrics

Reporting metrics directly from the kafka client is an option that is only
available in Yelp's fork of kafka-python (which yelp_kafka uses as
a dependency).

Producer metrics can also be reported and are reported by default by the YelpKafkaSimpleProducer
through the `report_metrics` parameter. This defaults to True but can be turned off

.. code-block:: python

   # Get cluster configuration
   cluster_config = discovery.get_region_cluster('standard', 'my-client-id')
   # Create a kafka Client
   client = KafkaClient(cluster_config.broker_list, client_id='my-client-id')
   # Create the producer and send 2 messages
   producer = YelpKafkaSimpleProducer(
       client=client,
       cluster_config=cluster_config,
       report_metrics=True,
   )

.. note::

If you want to plug in your own metric responder module, please use
:py:class:`yelp_kafka.metrics_responder.MetricsResponder` and pass it in
:py:class:`yelp_kafka.producer.YelpKafkaSimpleProducer` or
:py:class:`yelp_kafka.producer.YelpKafkaKeyedProducer` or
:py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup`.



Integration tests using kafka discovery
---------------------------------------

If you are using :py:mod:`yelp_kafka.discovery` and wish to do integration tests, you will need
to set up docker containers for the Kafka discovery service which is utilized to extract both
cluster and log information.

Add the following to your docker-compose.yml and link kafkadiscovery for your build

.. code-block:: yaml

    kafkadiscovery:
      image: docker-dev.yelpcorp.com/kafka-discovery-testing:latest
      expose:
       - "8888"
      volumes_from:
       - kafkadiscoveryconfigs
      links:
       - kafka
       - zookeeper

    kafkadiscoveryconfigs:
      image: docker-dev.yelpcorp.com/kafka-discovery-configs-testing:latest

    zookeeper:
      image: docker-dev.yelpcorp.com/zookeeper-testing:latest
      expose:
        - "2181"

    kafka:
      image: docker-dev.yelpcorp.com/kafka-testing:latest
      expose:
        - "9092"
      links:
        - zookeeper

The kafka-cluster available will be called `local_cluster` and the default superregion is
`acceptance_test_superregion`, default region is `acceptance_test_region`. These will be
obtained by default by the discovery functions. All cluster types are available for discovery.
