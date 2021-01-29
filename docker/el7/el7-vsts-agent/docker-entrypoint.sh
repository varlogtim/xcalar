#!/bin/bash

JENKINS_AGENT_WORKDIR=${JENKINS_AGENT_WORKDIR:-$HOME/agent}
if ! test -e $JENKINS_AGENT_WORKDIR; then
    mkdir -p $JENKINS_AGENT_WORKDIR
fi
chown $JENKINS_USER $JENKINS_AGENT_WORKDIR

su-exec $JENKINS_USER:$JENKINS_USER "$@"