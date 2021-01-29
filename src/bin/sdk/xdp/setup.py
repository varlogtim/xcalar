# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

versionFile = os.path.join(os.environ["XLRDIR"], "VERSION")
with open(versionFile, 'r') as f:
    XLRVERSION = f.read().strip()

REQUIREMENTS = [
    "xcalar-compute-coretypes",
]

BASE = {
    "author":
        'Xcalar Inc.',
    "author_email":
        'support@xcalar.com',
    "url":
        'http://xcalar.com',
    "license":
        'Proprietary',
    "classifiers": [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Compute Engine',
        'License :: Proprietary',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    "keywords":
        'xcalar compute engine database',
}

setup(
    name='xcalar-external',
    version=XLRVERSION,
    description='Xcalar Compute Engine Python Client',
    long_description=README,
    packages=find_packages(exclude=[]),
    install_requires=REQUIREMENTS,
    **BASE)
