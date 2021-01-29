#FROM alpine:3.6
#
#RUN apk add --no-cache squid
#ADD storeid_rpmdeb /usr/lib/squid/
#ADD squid.conf /etc/squid/
#ADD storeid.db /etc/squid/
#VOLUME ["/var/spool/squid"]
#RUN /usr/sbin/squid -N -z
#EXPOSE 3128
#
#ENTRYPOINT ["/usr/sbin/squid"]
#CMD ["-N","-d","INFO"]

FROM centos/systemd

RUN yum install -y epel-release && \
    yum install -y squid && \
    yum clean all && \
    rm -rf /var/cache/yum

ADD squid.conf /etc/squid/
ADD storeid.db /etc/squid/
ADD storeid_rpmdeb /usr/lib64/squid/storeid_rpmdeb

RUN /usr/sbin/squid -N -z

EXPOSE 3128

VOLUME ["/var/spool/squid"]

CMD ["/usr/sbin/squid","-N","-d","INFO"]

