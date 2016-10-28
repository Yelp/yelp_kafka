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

PRODUCE_EXCEPTION_COUNT = 'produce_exception_count'

TIME_METRIC_NAMES = set([
    'metadata_request_timer',
    'produce_request_timer',
    'fetch_request_timer',
    'offset_request_timer',
    'offset_commit_request_timer',
    'offset_fetch_request_timer',
    'offset_fetch_request_timer_kafka',
    'consumer_metadata_request_timer',
    'offset_commit_request_timer_kafka',
])

FAILURE_COUNT_METRIC_NAMES = set([
    'failed_paylads_count',
    'out_of_range_counts',
    'not_leader_for_partition_count',
    'request_timed_out_count'
])
