FROM docker-dev.yelpcorp.com/lucid_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN apt-get update
RUN apt-get install -y python python2.7
RUN apt-get install -y python-pkg-resources python-pip python-setuptools
RUN pip install tox

WORKDIR /work
CMD tox tests/integration
