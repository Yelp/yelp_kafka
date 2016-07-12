FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

# We need to install Java and Kafka in order to use Kafka CLI. The Kafka server
# will never run in this container; the Kafka server will run in the "kafka"
# container.
RUN apt-get update && apt-get install -y \
    java-8u20-oracle \
    confluent-kafka=0.9.0.0-1

ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN apt-get install -y python \
    python2.7 \
    python3.5 \
    python-pkg-resources \
    python-pip \
    python-setuptools \
    python-virtualenv \
    python-tox2

COPY run_tests.sh /scripts/run_tests.sh
RUN chmod 755 /scripts/run_tests.sh

WORKDIR /work
