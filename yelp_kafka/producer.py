# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from kafka import KeyedProducer
from kafka import SimpleProducer
from kafka.common import KafkaError
from py_zipkin.zipkin import zipkin_span

from yelp_kafka import metrics
from yelp_kafka.error import YelpKafkaError
from yelp_kafka.metrics_reporter import MetricReporter

METRIC_PREFIX = 'yelp_kafka.YelpKafkaProducer.'


class YelpKafkaProducerMetrics(object):
    """
        Used to setup and report producer metrics

         Args:
            cluster_config(Object) : this the kafka cluster config. The structure is present in
                :py:class:`yelp_kafka.config.ClusterConfig`
            report_metrics(Boolean): Flag to enable reporting metrics. Defaults to False.
            client: Kafka client for which metrics are to be reported
            metrics_reporter: A metrics reporter instance to report metrics
    """

    def __init__(
        self,
        cluster_config,
        client,
        metrics_reporter=None
    ):
        self.log = logging.getLogger(self.__class__.__name__)
        self.cluster_config = cluster_config
        self.client = client
        self.timers = {}
        self.metrics_reporter = metrics_reporter
        if self.metrics_reporter:
            if not isinstance(self.metrics_reporter, MetricReporter):
                raise Exception("Metric Reporter is not of type yelp_kafka.metrics_reporter.MetricReporter")
            self.setup_metrics()

    def get_kafka_dimensions(self):
        return {
            'client_id': self.client.client_id,
            'cluster_type': self.cluster_config.type,
            'cluster_name': self.cluster_config.name,
        }

    def setup_metrics(self):
        if self.metrics_reporter:
            self.client.metrics_responder = self._send_kafka_metrics
            kafka_dimensions = self.get_kafka_dimensions()
            self.kafka_enqueue_exception_count = self.metrics_reporter.get_counter_emitter(
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
            self.metrics_reporter.record(self._get_timer(key), time_in_ms)
        else:
            self.log.warn("Unknown metric: {0}".format(key))

    def _create_timer(self, name, dimensions=None):
        if dimensions is None:
            dimensions = {}
        new_name = METRIC_PREFIX + name
        self.timers[new_name] = self.metrics_reporter.get_timer_emitter(
            new_name,
            default_dimensions=dimensions
        )

    def _get_timer(self, name):
        return self.timers[METRIC_PREFIX + name]


class YelpKafkaSimpleProducer(SimpleProducer):
    """ YelpKafkaSimpleProducer is an extension of the kafka SimpleProducer that
    reports metrics about the producer to yelp_meteorite. These metrics include
    enqueue latency for both success and failure to send and the number of exceptions
    encountered trying to send.

    If metrics reporting isn't required for your producer, specify report_metrics=False.
    We highly recommend reporting metrics for monitoring purposes e.g. production latency.

    Note: This producer expects usage of kafka-python==0.9.4.post2 where metrics_responder
    is implemented in KafkaClient

    :param cluster_config: producer cluster configuration
    :type cluster_config: config.ClusterConfig
    :param report_metrics: whether or not to report kafka production metrics. Defaults to True
    :type report_metrics: bool

    Additionally all kafka.SimpleProducer params are usable here. See `_SimpleProducer`_.

    .. _SimpleProducer: http://kafka-python.readthedocs.org/en/v0.9.5/apidoc/kafka.producer.html
    """

    def __init__(
        self,
        cluster_config=None,
        report_metrics=True,
        metrics_reporter=None,
        *args, **kwargs
    ):
        super(YelpKafkaSimpleProducer, self).__init__(*args, **kwargs)

        if report_metrics and (not metrics_reporter):
            try:
                from yelp_kafka.yelp_metrics_reporter import MeteoriteMetrics
                metrics_reporter = MeteoriteMetrics()
            except ImportError:
                logging.error("yelp_meteorite is not present")
        elif not report_metrics:
            metrics_reporter = None

        self.metrics = YelpKafkaProducerMetrics(
            cluster_config=cluster_config,
            client=self.client,
            metrics_reporter=metrics_reporter
        )

    @zipkin_span(service_name='yelp_kafka', span_name='send_messages_simple_producer')
    def send_messages(self, topic, *msg):
        try:
            super(YelpKafkaSimpleProducer, self).send_messages(topic, *msg)
        except (YelpKafkaError, KafkaError):
            if self.metrics.metrics_reporter:
                self.metrics.metrics_reporter.record(self.metrics.kafka_enqueue_exception_count, 1)
            raise


class YelpKafkaKeyedProducer(KeyedProducer):
    """ YelpKafkaKeyedProducer is an extension of the kafka KeyedProducer that
    reports metrics about the producer to yelp_meteorite.

    Usage is the same as YelpKafkaSimpleProducer

    :param cluster_config: producer cluster configuration
    :type cluster_config: config.ClusterConfig
    :param report_metrics: whether or not to report kafka production metrics. Defaults to True
    :type report_metrics: bool

    Additionally all kafka.KeyedProducer params are usable here. See `_KeyedProducer`_.

    .. _KeyedProducer: http://kafka-python.readthedocs.org/en/v0.9.5/apidoc/kafka.producer.html
    """

    def __init__(
        self,
        cluster_config=None,
        report_metrics=True,
        metrics_reporter=None,
        *args,
        **kwargs
    ):
        super(YelpKafkaKeyedProducer, self).__init__(*args, **kwargs)

        if report_metrics and (not metrics_reporter):
            try:
                from yelp_kafka.yelp_metrics_reporter import MeteoriteMetrics
                metrics_reporter = MeteoriteMetrics()
            except ImportError:
                logging.error("yelp_meteorite is not present")
        elif not report_metrics:
            metrics_reporter = None

        self.metrics = YelpKafkaProducerMetrics(
            cluster_config,
            self.client,
            metrics_reporter
        )

    @zipkin_span(service_name='yelp_kafka', span_name='send_messages_keyed_producer')
    def send_messages(self, topic, *msg):
        try:
            super(YelpKafkaKeyedProducer, self).send_messages(topic, *msg)
        except (YelpKafkaError, KafkaError):
            if self.metrics.metrics_reporter:
                self.metrics.metrics_reporter.record(self.metrics.kafka_enqueue_exception_count, 1)
            raise
