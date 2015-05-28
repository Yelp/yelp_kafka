This changelog only guarantees backward incompatible changes will be listed.

# v2.0.0 (2015-03-26)
## backward incompatible changes
- `yelp_kafka.monitoring.*` function signatures have been changed to
  (client, group, topics) and (client, group, topic, partitions).

# v3.0.0 (2015-05-28)
## backward incompatible changes
- `yelp_kafka.monitoring` all consumer offset management related functions
  moved to `yelp_kafka.offsets`
