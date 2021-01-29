# The purpose of this script is essentially to cache the environment needed
# to build the Xcalar RPM. Building this Dockerfile will produce such an
# environment while executing it will (after updating the build environment)
# produce a Xcalar RPM based on the HEAD of your git repo and whatever
# dependency RPMs presently reside on /netstore.
FROM el7-build

RUN printf '%s\n%s\n' 'NAME="Alibaba Group Enterprise Linux Server"' 'VERSION="7.2 (Paladin)"' | tee /etc/os-release \
    && printf '%s\n' 'Alibaba Group Enterprise Linux Server release 7.2 (Paladin)' | tee /etc/system-release \
    && rm -f /etc/system-release-cpe
