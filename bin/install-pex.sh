#!/bin/bash

set -e
PYTHON=${PYTHON:-python}
PYTHON_COMMAND=$(command -v $PYTHON)
PYVER=$($PYTHON_COMMAND --version 2>&1| cut -d' ' -f2 | cut -d'.' -f1-2)
PIP=${PIP:-pip${PYVER:0}}
if test -e /etc/redhat-release; then
    (
    sudo yum install -y byacc patchutils python-devel libffi-devel openssl-devel
    curl -L https://raw.githubusercontent.com/pypa/get-pip/master/2.6/get-pip.py | sudo $PYTHON_COMMAND
    sudo $PIP install urllib3[secure]==1.22
    sudo $PIP install virtualenv
#    curl -sSL https://setup.ius.io | sudo bash
#    sudo yum install -y python27-devel python27-devel python27-pip openssl-devel libffi-devel
#    sudo $PIP install -U pip
#    sudo $PIP install virtualenv
    )>&2
fi
TMPDIR=/tmp/`id -u`/$$
mkdir -p $TMPDIR
/usr/bin/virtualenv -q --python=$PYTHON_COMMAND $TMPDIR >&2
source $TMPDIR/bin/activate
# 1.2.10 is the last version to support py2.6
${PIP} install urllib3[secure]==1.22 pex==1.2.10
OUTPUT="${1:-pex.pex}"
pex --version
pex pex==1.2.10 requests[security] urllib3[secure]==1.22 --python-shebang="#!$PYTHON_COMMAND" -c pex -o "${OUTPUT}" >&2
echo "${OUTPUT}"
echo >&2 "Wrote ${OUTPUT}"
deactivate
rm -rf "$TMPDIR"
