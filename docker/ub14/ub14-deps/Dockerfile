FROM ub14-build

WORKDIR /usr/src

USER root

ADD ./bin/osid /usr/bin/
ADD ./bin/install-jemalloc.sh /usr/bin/
RUN install-jemalloc.sh

ADD ./bin/install-ccache.sh /usr/bin/
RUN install-ccache.sh

ADD ./bin/install-python.sh /usr/bin/
ADD ./bin/install-python-package.sh /usr/bin/
RUN mkdir -p /usr/src/pkg src/3rd/pyodbc
COPY ./requirements.txt /usr/src
COPY ./pkg/requirements.txt /usr/src/pkg
COPY ./src/3rd/pyodbc/pyodbc-pip-patch-0001.diff /usr/src/src/3rd/pyodbc
RUN install-python.sh 3.6.2
RUN rm -rf /opt/xcalar
RUN dpkg -i xcalar-python36_3.6.2*.deb
ENV PATH="/opt/xcalar/bin:${PATH}"

ADD ./bin/install-protobuf3.sh /usr/bin/
RUN install-protobuf3.sh

ADD ./bin/install-boost.sh /usr/bin/
RUN install-boost.sh

ADD ./bin/install-thrift.sh /usr/bin/
RUN install-thrift.sh

RUN /opt/xcalar/bin/pip2.7 install cython simplejson
RUN /opt/xcalar/bin/pip2.7 install -U setuptools

ADD ./bin/install-cmake.sh /usr/bin/
RUN install-cmake.sh


CMD ["/bin/bash","-l"]
