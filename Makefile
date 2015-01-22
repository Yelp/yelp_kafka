.DELETE_ON_ERROR:

all: test

test:
	tox tests

sdist:
	python setup.py sdist

bdist_wheel:
	python setup.py bdist_wheel

docs:
	tox -e docs

clean:
	make -C docs clean
	rm -rf build/ dist/ yelp_kafka.egg-info/ .tox/
	find . -name '*.pyc' -delete
	rm -rf docs/build/
