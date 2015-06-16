MOUNT_DIR := /work
IMAGE_NAME := yelp_kafka_lucid
DOCKER_RUN := docker run --rm -t -v $(CURDIR):$(MOUNT_DIR):rw $(IMAGE_NAME)

.DELETE_ON_ERROR:

all: test

test: .build_image
	$(DOCKER_RUN) /work/start_kafka.sh

sdist:
	python setup.py sdist

bdist_wheel:
	python setup.py bdist_wheel

docs: .build_image
	$(DOCKER_RUN) tox -e docs

clean: .build_image
	$(DOCKER_RUN) $(MOUNT_DIR)/clean.sh

.build_image: Dockerfile
	docker build -t $(IMAGE_NAME) .
	touch .build_image

.PHONY: docs
