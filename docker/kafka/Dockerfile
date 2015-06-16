FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN apt-get update && apt-get -y install java-8u20-oracle confluent-kafka
ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"

EXPOSE 9092
CMD /usr/bin/kafka-server-start /etc/kafka/server.properties

