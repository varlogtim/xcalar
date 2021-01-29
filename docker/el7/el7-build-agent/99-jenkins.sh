#!/bin/bash

if [ $1 = fixups ] && [ -n "$JENKINS_AGENT_WORKDIR" ]; then
    JENKINS_AGENT_WORKDIR=${JENKINS_AGENT_WORKDIR:-$HOME/agent}
    if ! test -e $JENKINS_AGENT_WORKDIR; then
        mkdir -p $JENKINS_AGENT_WORKDIR
    fi
    chown $JENKINS_USER $JENKINS_AGENT_WORKDIR
fi
