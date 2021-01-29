## There's a bug in ruby 2.5 on alpine :(
#FROM alpine:3.9.4
#
#RUN apk add --no-cache  rpm-dev \
#                        tar gzip \
#                        bzip2 \
#                        bash \
#                        make \
#                        g++ \
#                        ninja \
#                        ruby-dev \
#                        ruby-etc \
#                        fakeroot \
#                        xz \
#                        cpio
ARG CENTOS7
FROM $CENTOS7
RUN yum -y install rubygems ruby-devel make gcc gzip rpm-build xz bzip2 tar yum-plugin-remove-with-leaves \
    && gem install --no-rdoc --no-ri fpm \
    && yum --remove-leaves -y remove make gcc ruby-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

ENTRYPOINT ["/usr/local/bin/fpm"]

CMD ["--help"]
