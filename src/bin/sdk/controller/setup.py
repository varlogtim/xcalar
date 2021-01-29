# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

REQUIREMENTS = [
    'xcalar-external', 'confluent-kafka', 'fastavro', 'pandas', 'protobuf',
    'requests', 'ipython', 'pyparsing', 'sqlparse', 'requests_kerberos',
    'tabulate', 'transitions', 'jsonpickle', 'structlog', 'contextvars',
    'colorama'
]

BASE = {
    'author':
        'Xcalar Inc.',
    'author_email':
        'support@xcalar.com',
    'url':
        'http://xcalar.com',
    'license':
        'Proprietary',
    'classifiers': [
        'Development Status :: 4 - Beta',
        'Intended Audience :: customer4',
        'Topic :: Software Development :: Refiner',
        'License :: Proprietary',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    'keywords':
        'xcalar refiner PWM',
}

setup(
    name='xcalar-controller',
    version='0.0.2',
    description='xcalar controller module',
    long_description='xcalar controller module',
    packages=find_packages(exclude=[]),
    install_requires=REQUIREMENTS,
    **BASE)
