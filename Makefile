.DELETE_ON_ERROR:

all: test itest

test:
	tox2 tests

itest:
	tox2 -e docker_itest

sdist:
	python setup.py sdist

bdist_wheel:
	python setup.py bdist_wheel

docs:
	tox2 -e docs

clean:
	make -C docs clean
	rm -rf build/ dist/ yelp_kafka.egg-info/ .tox/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf docs/build/

.PHONY: docs
