ARG EL7_BUILD
FROM $EL7_BUILD
ARG EL7_BUILD

ARG REFRESHED_ON=2020-06-19-1
ENV REFRESHED_ON=$REFRESHED_ON

ARG CONTAINER_UID=1000
ARG CONTAINER_USER=xcalardev
ARG CONTAINER_HOME=/home/xcalardev
ARG JENKINS_HOME=/home/jenkins_home
ARG JENKINS_AGENT_WORKDIR=/home/jenkins_home/agent
ARG SHARED_CACHE=/var/opt/cache

ENV CONTAINER_UID=$CONTAINER_UID
ENV CONTAINER_USER=$CONTAINER_USER
ENV CONTAINER_HOME=$CONTAINER_HOME
ENV JENKINS_HOME=$JENKINS_HOME
ENV SHARED_CACHE=$SHARED_CACHE
ENV JENKINS_USER=$CONTAINER_USER
ENV JENKINS_UID=$CONTAINER_UID
ENV JENKINS_AGENT_WORKDIR=$JENKINS_AGENT_WORKDIR

ARG VERSION=3.29
ARG SWARM_VERSION=3.19
ENV SWARM_JAR=/usr/share/jenkins/swarm-client.jar
ENV AGENT_JAR=/usr/share/jenkins/agent.jar

ARG XCALAR_REPOPKG_ORG=http://repo.xcalar.net

RUN mkdir -p /usr/share/jenkins \
  && curl -fsSLo $AGENT_JAR ${XCALAR_REPOPKG_ORG}/deps/jenkins/remoting-${VERSION}.jar \
  && curl -fsSLo $SWARM_JAR ${XCALAR_REPOPKG_ORG}/deps/jenkins/swarm-client-${SWARM_VERSION}.jar \
  && curl -fsSLo /usr/bin/jenkins-agent.sh ${XCALAR_REPOPKG_ORG}/deps/jenkins/jenkins-agent-v1 \
  && chmod 0755 /usr/share/jenkins /usr/bin/jenkins-agent.sh \
  && chmod 0644 $AGENT_JAR $SWARM_JAR

ADD docker/el7/el7-build-agent/jenkins-swarm /usr/bin/
ADD docker/el7/el7-build-agent/99-jenkins.sh /docker-entrypoint.d/

RUN mkdir -p $JENKINS_HOME $JENKINS_AGENT_WORKDIR -m 0755 && chown $CONTAINER_UID:$CONTAINER_UID $JENKINS_HOME $JENKINS_AGENT_WORKDIR

