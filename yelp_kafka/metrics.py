# -*- coding: utf-8 -*-
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
