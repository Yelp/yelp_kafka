# -*- coding: utf-8 -*-
import logging

import yelp_meteorite
from kafka import SimpleProducer
from kafka.common import KafkaError

from yelp_kafka import metrics
from yelp_kafka.error import YelpKafkaError


METRIC_PREFIX = 'yelp_kafka.YelpKafkaProducer.'


class YelpKafkaSimpleProducer(SimpleProducer):
    """ YelpKafkaSimpleProducer is an extension of the kafka SimpleProducer that
    reports metrics about the producer to yelp_meteorite. These metrics include
    enqueue latency for both success and failure to send and the number of exceptions
    encountered trying to send.

    If metrics reporting isn't required for your producer, specify report_metrics=False.
    We highly recommend reporting metrics for monitoring purposes e.g. production latency.

    Note: This producer expects usage of kafka-python==0.9.4.post2 where metrics_responder
    is implemented in KafkaClient

    Usage is the similar to SimpleProducer, the only difference is cluster_config is
    required. E.g.:

        # Get a connected KafkaClient and cluster_config from yelp_kafka for standard cluster type
        client = discovery.get_kafka_connection('standard', client_id='my-client-id')
        cluster_config = discovery.get_local_cluster('standard')
        producer = YelpKafkaSimpleProducer(
            client=client,
            cluster_config=cluster_config,
            report_metrics=True
        )
        producer.send_messages("my_topic", "message1", "message2") # send msgs to kafka

    :param cluster_config: producer cluster configuration
    :type cluster_config: config.ClusterConfig
    :param report_metrics: whether or not to report kafka production metrics. Defaults to True
    :type report_metrics: bool

    Additionally all kafka.SimpleProducer params are usable here.
    """

    def __init__(self, cluster_config=None, report_metrics=True, *args, **kwargs):
        super(YelpKafkaSimpleProducer, self).__init__(*args, **kwargs)
        self.log = logging.getLogger(self.__class__.__name__)
        self.cluster_config = cluster_config
        self.timers = {}
        self.report_metrics = report_metrics
        self.setup_metrics()

    def get_kafka_dimensions(self):
        return {
            'client_id': self.client.client_id,
            'cluster_type': self.cluster_config.type,
            'cluster_name': self.cluster_config.name,
        }

    def setup_metrics(self):
        if self.report_metrics:
            self.client.metrics_responder = self._send_kafka_metrics
            kafka_dimensions = self.get_kafka_dimensions()
            self.kafka_enqueue_exception_count = yelp_meteorite.create_counter(
                METRIC_PREFIX + metrics.PRODUCE_EXCEPTION_COUNT,
                kafka_dimensions
            )
            for name in metrics.TIME_METRIC_NAMES:
                self._create_timer(name, kafka_dimensions)

    def _send_kafka_metrics(self, key, value):
        if key in metrics.TIME_METRIC_NAMES:
            # kafka-python emits time in seconds, but yelp_meteorite wants
            # milliseconds
            time_in_ms = value * 1000
            self._get_timer(key).record(time_in_ms)
        else:
            self.log.warn("Unknown metric: {0}".format(key))

    def _create_timer(self, name, dimensions=None):
        if dimensions is None:
            dimensions = {}
        new_name = METRIC_PREFIX + name
        self.timers[new_name] = yelp_meteorite.create_timer(
            new_name,
            default_dimensions=dimensions
        )

    def _get_timer(self, name):
        return self.timers[METRIC_PREFIX + name]

    def send_messages(self, topic, *msg):
        try:
            super(YelpKafkaSimpleProducer, self).send_messages(topic, *msg)
        except (YelpKafkaError, KafkaError):
            if self.report_metrics:
                self.kafka_enqueue_exception_count.count(1)
            raise
