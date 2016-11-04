# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import yelp_meteorite

from yelp_kafka.metrics_responder import MetricsResponder


class MeteoriteMetricsResponder(MetricsResponder):
    """
        Used for publishing metrics with a metric reporter instance
    """

    def __init__(self):
        super(MeteoriteMetricsResponder, self).__init__()
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
        if isinstance(registered_reporter, yelp_meteorite.metrics.Counter):
            registered_reporter.count(value)
        if isinstance(registered_reporter, yelp_meteorite.metrics.Timer):
            registered_reporter.record(value)
        else:
            self.log.error("Reporter Instance is not defined")
