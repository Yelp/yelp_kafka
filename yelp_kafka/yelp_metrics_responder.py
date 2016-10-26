from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import yelp_meteorite

from yelp_kafka.metrics_responder import MetricsResponder


class MeteoriteMetrics(MetricsResponder):
    """Used for publishing metrics with a metric responder instance."""

    def __init__(self):
        super(MeteoriteMetrics, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def get_counter_emitter(self, metric, default_dimensions=None):
        return yelp_meteorite.create_counter(
            metric,
            default_dimensions
        )

    def get_timer_emitter(self, metric, default_dimensions=None):
        return yelp_meteorite.create_timer(
            metric,
            default_dimensions
        )

    def record(self, registered_reporter, value, timestamp=None):
        if isinstance(registered_reporter, yelp_meteorite.metrics.Count):
            registered_reporter.count(value)
        if isinstance(registered_reporter, yelp_meteorite.metrics.Timer):
            registered_reporter.record(value)
        else:
            self.log.error("Reporter Instance is not defined")
