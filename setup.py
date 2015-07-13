#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import yelp_kafka


class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(self.test_args)
        sys.exit(errno)


class Coverage(Tox):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-e', 'coverage']
        self.test_suite = True


setup(
    name='yelp_kafka',
    version=yelp_kafka.__version__,
    author='Tools-Infra Team',
    author_email='tools-infra@yelp.com',
    license='Copyright Yelp 2014, All Rights Reserved',
    url="http://servicedocs.yelpcorp.com/docs/yelp_kafka/index.html",
    description='A library to interact with Apache Kafka at Yelp',
    packages=find_packages(exclude=["tests"]),
    setup_requires=['setuptools'],
    install_requires=[
        'kafka-python==0.9.4',
        'kazoo>=2.0.post2',
        'PyYAML>=3.10',
        'requests==2.6.0',
        'setproctitle>=1.1.8',
        'simplejson==3.6.5',
    ],
    cmdclass={
        'test': Tox,
        'coverage': Coverage
    },
)
