#!/bin/bash

set -e

yum install -y epel-release
yum groupinstall -y 'Development tools'
yum install -y python-devel python-setuptools python-setuptools-devel python-pip openssl-devel libffi-devel curl
command -v easy_install &>/dev/null || curl -sSL https://bootstrap.pypa.io/ez_setup.py | python -
command -v pip &>/dev/null || curl -sSL https://bootstrap.pypa.io/get-pip.py | python -
pip install virtualenv
virtualenv pex
source pex/bin/activate
pip install pex
mkdir -p $HOME/bin
pex pex requests -c pex -o $HOME/bin/pex
export PATH=$PATH:$HOME/bin
pex ansible -c ansible-playbook -o ansible-playbook.pex
./ansible-playbook.pex --version
