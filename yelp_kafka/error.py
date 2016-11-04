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


class YelpKafkaError(Exception):
    """Base class for yelp_kafka errors."""
    pass


class DiscoveryError(YelpKafkaError):
    """Errors while using discovery functions."""
    pass


class ConsumerError(YelpKafkaError):
    """Error in consumer."""
    pass


class ConfigurationError(YelpKafkaError):
    """Error in configuration. For example. Missing configuration file
    or misformatted configuration."""
    pass


class InvalidOffsetStorageError(YelpKafkaError):
    """Unknown source of offsets."""
    pass


class ProcessMessageError(YelpKafkaError):
    """Error processing a message from kafka."""
    pass


class ConsumerGroupError(YelpKafkaError):
    """Error in the consumer group"""
    pass


class PartitionerError(YelpKafkaError):
    """Error in the partitioner"""
    pass


class PartitionerZookeeperError(YelpKafkaError):
    """Error in partitioner communication with Zookeeper"""
    pass


class UnknownTopic(YelpKafkaError):
    pass


class UnknownPartitions(YelpKafkaError):
    pass


class OffsetCommitError(YelpKafkaError):

    def __init__(self, topic, partition, error):
        self.topic = topic
        self.partition = partition
        self.error = error

    def __eq__(self, other):
        if all([
            self.topic == other.topic,
            self.partition == other.partition,
            self.error == other.error,
        ]):
            return True
        return False


class InvalidClusterTypeOrRegionError(YelpKafkaError):
    pass


class InvalidClusterTypeOrNameError(YelpKafkaError):
    pass


class InvalidClusterTypeOrSuperregionError(YelpKafkaError):
    pass


class InvalidClusterType(YelpKafkaError):
    pass


class InvalidLogOrRegionError(YelpKafkaError):
    pass


class InvalidLogOrSuperregionError(YelpKafkaError):
    pass
