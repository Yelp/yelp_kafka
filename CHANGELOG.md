This changelog only guarantees backward incompatible changes will be listed.
# v5.0.0 (2016-09-09)
## backward incompatible changes
- `yelp_kafka.discovery` methods using kafka discovery service instead of files
- `yelp_kafka.discovery` methods have different names and take different parameters. An additional
  client_id is often needed. This affects:
  - get_all_clusters
  - get_kafka_connection (client_id is now mandatory)
  - get_all_kafka_connections (client_id is now mandatory)
- Deprecated `yelp_kafka.discovery` methods deleted. These are
  - get_local_cluster
  - get_cluster_by_name
  - get_local_scribe_topic
  - get_scribe_topics
  - get_all_local_scribe_topics
  - get_scribe_topic_in_datacenter
  - scribe_topic_exists_in_datacenter
  - search_local_scribe_topics_by_regex
  - search_local_topics_by_regex
  - local_scribe_topic_exists
  - get_all_consumer_config

# v4.0.0 (2015-08-24)
## backward incompatible changes
- `yelp_kafka.config.ClusterConfig`:
  - add 'type' attribute to ClusterConfig nametuple

# v3.0.0 (2015-05-28)
## backward incompatible changes
- `yelp_kafka.monitoring.get_current_consumer_offsets`:
  - function signature changed from (client, group, topics, fail_on_error)
    to (client, group, topics, raise_on_error)
  - moved to `yelp_kafka.offsets.get_current_consumer_offsets`
- `yelp_kafka.monitoring.get_topics_watermarks`:
  - function signature changed from (client, topics, fail_on_error)
    to (client, topics, raise_on_error)
  - moved to `yelp_kafka.offsets.get_topics_watermarks`
- `yelp_kafka.monitoring.PartitionOffsets` namedtuple moved to
  `yelp_kafka.offsets.PartitionOffsets`
## added
- `yelp_kafka.offsets` new functions added:
  advance_consumer_offsets, rewind_consumer_offsets, set_consumer_offsets

# v2.0.0 (2015-03-26)
## backward incompatible changes
- `yelp_kafka.monitoring.*` function signatures have been changed to
  (client, group, topics) and (client, group, topic, partitions).
