# Yelp Kafka

## Producer

Create a producer for my_topic.

```python

   from yelp_kafka import discovery
   from yelp_kafka.config import ClusterConfig
   from yelp_kafka.producer import YelpKafkaSimpleProducer
   from kafka import KafkaClient
   from kafka.common import ConsumerTimeout
   from kafka.common import FailedPayloadsError
   from kafka.common import KafkaUnavailableError
   from kafka.common import LeaderNotAvailableError
   from kafka.common import NotLeaderForPartitionError
   from kafka.common import UnknownTopicOrPartitionError

   # Cluster configuration
   cluster_config = ClusterConfig(
       type="service",
       name="cluster",
       broker_list=["cluster-elb-1:9092"],
       zookeeper="11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1",
   )
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

```

This example makes use of the [YelpKafkaSimpleProducer](yelp_kafka/producer.py) 
from yelp_kafka.

__client_id__ identifies the client connection in Kafka and it is used by Kafka 0.9.0 to enforce
quota limit per client. We recommend to use a __client_id__ that represents the application.

In the example there are some exceptions that usually should be safe to just retry.

**KafkaUnavailableError** can happen when the metadata request to Kafka fails, this
request is broker unaware so a simple retry would pick another broker of the cluster and possibly succeed.

**LeaderNotAvailableError** and **NotLeaderForPartitionError** may happen during a cluster
rolling restart or upon broker failure. In this case a new leader will be elected, kafka-python
by default refreshes the metadata when encountering these errors, thus upon retry it would
hopefully use a new leader and succeed. However, Kafka doesn't give us any guarantee on how quickly
a new leader will be elected. We measured that for small clusters the elections happens in the order
of hundreds of ms but for large clusters it can take up to several seconds.
Usually an application should retry for a limited amount of time and then consider the request failed and react accordingly.

Finally, **FailedPayloadsError** may happen in many cases, for example when a leader is missing
or the connection fails in the middle of a request. Metadata is automatically refreshed for this exception as well.

See Also: [kafka-python](http://kafka-python.readthedocs.org/en/v0.9.5/usage.html) and [SimpleProducer](http://kafka-python.readthedocs.org/en/v0.9.5/apidoc/kafka.producer.html)



## Consumer

```python

   from yelp_kafka import discovery
   from yelp_kafka.consumer_group import KafkaConsumerGroup
   from yelp_kafka.config import ClusterConfig
   from yelp_kafka.config import KafkaConsumerConfig
   from yelp_kafka.error import PartitionerError
   from kafka.common import ConsumerTimeout
   from kafka.common import FailedPayloadsError
   from kafka.common import KafkaUnavailableError
   from kafka.common import LeaderNotAvailableError
   from kafka.common import NotLeaderForPartitionError

   # Cluster configuration
   cluster_config = ClusterConfig(
       type="service",
       name="cluster",
       broker_list=["cluster-elb-1:9092"],
       zookeeper="11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1",
   )
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
```

See __producer_example__ above for more information about the exceptions to retry.
The __group_id__ should represent the application/service the consumer belongs to.


**Note** When bootstrapping a new consumer group it is usually recommended to set ``auto_offset_reset`` to **largest**.
          It assures that a huge amount of past messages are not consumed the first time a consumer is launched.
          ``auto_offset_reset`` should be set to **smallest** immediately after the first run (after the offsets are committed for the first time).
          When ``auto_offset_reset`` is set to **smallest** no messages are lost when adding new partitions.
          
Create a consumer for all topics ending with mytopic:

```python

   from yelp_kafka import discovery
   from yelp_kafka.config import ClusterConfig
   from yelp_kafka.config import KafkaConsumerConfig
   from kafka import KafkaConsumer

   # Cluster configuration
   cluster_config = ClusterConfig(
       type="service",
       name="cluster",
       broker_list=["cluster-elb-1:9092"],
       zookeeper="11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1",
   )
   topics, cluster = discovery.search_topics_by_regex('.*mytopic', [cluster_config])
   config = KafkaConsumerConfig(group_id='my_app', cluster=cluster, client_id='my-consumer')
   consumer = KafkaConsumer(topics, **config.get_kafka_consumer_config())
   for message in consumer:
       print message
```

This example makes use of the KafkaConsumer from kafka-python. This consumer
class should be considered deprecated and should not be used anymore. 

See Also: [KafkaConsumer](http://kafka-python.readthedocs.org/en/v0.9.5/apidoc/kafka.consumer.html#module-kafka.consumer.kafka)


## Reporting Metrics


If you're using `yelp_kafka.consumer_group.KafkaConsumerGroup`, you
can send metrics on request latency and error counts. This is on by default
for yelp_kafka and uses an instance of
`yelp_kafka.metrics_responder.MetricsResponder` for reporting metrics

Reporting metrics directly from the kafka client is an option that is only
available in Yelp's fork of kafka-python: https://github.com/Yelp/kafka-python

Producer metrics can also be reported and are reported by default by the YelpKafkaSimpleProducer
through the `report_metrics` parameter. This defaults to True but can be turned off


If you want to plug in your own metric responder module, please use
`yelp_kafka.metrics_responder.MetricsResponder` and pass it in
`yelp_kafka.producer.YelpKafkaSimpleProducer` or
`yelp_kafka.producer.YelpKafkaKeyedProducer` or
`yelp_kafka.consumer_group.KafkaConsumerGroup`.


## Other consumer groups


Yelp_Kafka currently provides three *consumer group* interfaces for consuming
from Kafka.

- `yelp_kafka.consumer_group.KafkaConsumerGroup` is the recommended
  class to use if you want start multiple instances of your consumer. You may
  start as many instances as you wish (balancing partitions will happen
  automatically), and you can control when to mark messages as processed (via
  __task_done__ and __commit__).

- 'yelp_kafka.consumer_group.MultiprocessingConsumerGroup` is for
  consuming from high volume topics since it starts as many consumer processes as topic
  partitions. It also handles process monitoring and restart upon failures.

- `yelp_kafka.consumer_group.ConsumerGroup` provides the same set of
  features as KafkaConsumerGroup, but with a less convenient interface.
  This class is considered deprecated.
