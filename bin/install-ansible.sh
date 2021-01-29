#!/bin/bash

#yum groupinstall -y 'Development tools'
yum install -y byacc  patchutils diffstat flex gcc-c++ indent gettext
curl -sSL https://setup.ius.io | bash
yum install -y python27-devel python27-devel python27-pip openssl-devel libffi-devel
pip2.7 install virtualenv
virtualenv --python=`which python2.7`  /tmp/pex
source /tmp/pex/bin/activate
pip2.7 install pex
pex pex requests -c pex -o pex.pex
pex ansible -c ansible-playbook -o ansible-playbook.pex
pex ansible -c ansible -o ansible.pex
chown $(stat -c '%u:%g' .) ansible*.pex
./ansible-playbook.pex --version
./ansible.pex --version
