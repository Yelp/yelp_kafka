# -*- coding: utf-8 -*-
import logging
import os
import socket
import time

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

    If metrics reporting isn't required for your producer, use kafka's SimpleProducer
    instead of this.

    Note: This producer expects usage of kafka-python==0.9.4.post2 where metrics_responder
    is implemented in KafkaClient

    Usage is the same as SimpleProducer:
        producer = YelpKafkaSimpleProducer(client)
        producer.send_messages("my_topic", "message1", "message2") # send msgs to kafka
    """

    def __init__(self, *args, **kwargs):
        super(YelpKafkaSimpleProducer, self).__init__(*args, **kwargs)
        self.log = logging.getLogger(self.__class__.__name__)
        self.client.metrics_responder = self._send_kafka_metrics
        self.timers = {}
        self._setup_metrics()

    def _setup_metrics(self):
        default_dimensions = {'hostname': self._get_hostname()}
        self._create_timer(metrics.ENQUEUE_LATENCY_SUCCESS_TIMER, default_dimensions)
        self._create_timer(metrics.ENQUEUE_LATENCY_FAILURE_TIMER, default_dimensions)
        self._create_timer(metrics.ENQUEUE_LATENCY_SUCCESS_NO_DIMENSIONS_TIMER)
        self._create_timer(metrics.ENQUEUE_LATENCY_FAILURE_NO_DIMENSIONS_TIMER)

        self.kafka_enqueue_exception_count = yelp_meteorite.create_counter(
            METRIC_PREFIX + metrics.ENQUEUE_EXCEPTION_COUNT,
            default_dimensions
        )

        kafka_dimensions = {'client_id': self.client.client_id}
        kafka_dimensions.update(default_dimensions)
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
        timer = yelp_meteorite.create_timer(new_name, default_dimensions=dimensions)
        self.timers[new_name] = timer

    def _get_timer(self, name):
        return self.timers[METRIC_PREFIX + name]

    def _send_success_metrics(self, latency):
        self._get_timer(metrics.ENQUEUE_LATENCY_SUCCESS_TIMER).record(latency)
        self._get_timer(metrics.ENQUEUE_LATENCY_SUCCESS_NO_DIMENSIONS_TIMER).record(latency)

    def _send_failure_metrics(self, latency):
        self._get_timer(metrics.ENQUEUE_LATENCY_FAILURE_TIMER).record(latency)
        self._get_timer(metrics.ENQUEUE_LATENCY_FAILURE_NO_DIMENSIONS_TIMER).record(latency)
        self.kafka_enqueue_exception_count.count(1)

    def send_messages(self, topic, *msg):
        start_time = time.time()
        try:
            super(YelpKafkaSimpleProducer, self).send_messages(topic, *msg)
        except (YelpKafkaError, KafkaError):
            self._send_failure_metrics(time.time() - start_time)
            raise
        else:
            self._send_success_metrics(time.time() - start_time)

    def _get_hostname(self):
        hostname = socket.gethostname()
        # If we're in a docker container, gethostname returns the container id.
        # We can grab the actual hostname from the HOST env variable.
        HOST = os.getenv('HOST')
        return '{0}:{1}'.format(HOST, hostname) if HOST else hostname
