FROM el7-build

ARG AGENT_UID=$CONTAINER_UID
ARG AGENT_USER=$CONTAINER_USER
ENV AGENT_HOME=$CONTAINER_HOME
ENV AGENT_WORK=${AGENT_HOME}/work

#VOLUME ["$AGENT_HOME"]
#RUN chown $CONTAINER_USER -R $AGENT_HOME

WORKDIR /azp
COPY ./start.sh .
RUN chmod +x start.sh
VOLUME ["/azp"]

#ARG VSTS_AGENT_URL="https://vstsagentpackage.azureedge.net/agent/2.155.1/vsts-agent-linux-x64-2.155.1.tar.gz"
#ENV VSTS_AGENT_INPUT_WORK=${AGENT_HOME}/agent
#ENV VSTS_AGENT_INPUT_URL=https://dev.azure.com/xcalar


#RUN curl -f -L $VSTS_AGENT_URL | tar zxvf - && mkdir $VSTS_AGENT_INPUT_WORK

#ARG AGENT_NAME=agent1
#ARG VSTS_AGENT_INPUT_TOKEN
#RUN ./config.sh --unattended --url https://dev.azure.com/xcalar --auth pat --pool Default --agent $AGENT_NAME --replace

#ADD docker-entrypoint.sh /


#WORKDIR $AGENT_HOME
CMD ["./start.sh"]
