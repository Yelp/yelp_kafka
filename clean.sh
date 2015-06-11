#!/usr/bin/env bash

# This gets invoked by `make clean` and is run within a docker container.

make -C docs clean
rm -rf build/ dist/ yelp_kafka.egg-info/ .tox/
find . -name '*.pyc' -delete
find . -name '__pycache__' -delete
rm -rf docs/build/
