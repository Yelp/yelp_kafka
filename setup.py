# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import sys

from setuptools import find_packages
from setuptools import setup
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
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        'bravado',
        'bravado_decorators',
        'kafka-python==0.9.5.post3',
        'kazoo>=2.0.post2',
        'PyYAML>=3.10',
        'requests',
        'setproctitle>=1.1.8',
        'simplejson',
        'six',
        'yelp-lib',
        'yelp_meteorite',
    ],
    cmdclass={
        'test': Tox,
        'coverage': Coverage
    },
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Intended Audience :: Developers",
    ],
)
