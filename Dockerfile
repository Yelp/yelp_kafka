FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

ENV KAFKA_VERSION="0.8.2.1" SCALA_VERSION="2.10"

RUN apt-get update
RUN apt-get install -qq curl
RUN apt-get install -qq java-8u20-oracle
ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"

RUN curl http://www.us.apache.org/dist/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz -o /tmp/kafka.tgz
RUN tar xf /tmp/kafka.tgz -C /opt

RUN apt-get install -qq build-essential

RUN apt-get install -qq python
RUN apt-get install -qq python2.7
RUN apt-get install -qq python-pkg-resources
RUN apt-get install -qq python-pip
RUN apt-get install -qq python-setuptools

RUN pip install tox

ADD . /work
WORKDIR /work
