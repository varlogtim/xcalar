# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
Usage: Executed within the Xcalar Data Platform

This is a Xcalar daemon system app which is responsible for complex
event processing based on CSV events consumed from Kafka. Currently
other event formats such as JSON, AVRO are not supported. This will
be supported in the future.

All configs are read from Xcalar's default.cfg config file.
Upon an incoming "trigger", this daemon executes a configured
batch data flow. This never exits, unless there is an untoward
event or has been configured to be cep.enable=False.

A typical default.cfg should look like:

cep.kafka.topic=forex
cep.kafka.hosts=localhost:9092
cep.kafka.group=demo
cep.kafka.timeout=inf
cep.kafka.recvbuf=98304
cep.batchMap={"EURUSD" : "EURUSDSpread", "AUDUSD" : "AUDUSDSpread",
    "GBPUSD" : "GBPUSDSpread", "CADUSD" : "CADUSDSpread",
    "JPYUSD" : "JPYUSDSpread", "realtime" : "realtime"}
cep.cepExpr=float(fields[3]) > 18 and fields[0] != 'USDJPY'
cep.batchKeyIndex=0
cep.enabled=true

CEP only processes a single topic at a time.  If you would like
to process events in real time, you must apply a rule by using
cepExpr on every rule. Usually a small percentage of rules are
satisfied and result in triggering a batch dataflow. The cepExpr
rule is picked by the user and specified in default.cfg.

If you would like to handle all events which must be less frequent
and normally trigger batch dataflows that are heavy workloads, have
CEP listen onto a specific topic. The only way to indicate you want
to process all events is by not specifying a cepExpr, which is the
business rule to be applied to an incoming kafka event. So in this
case no rule is applied as a filter, and every single
event triggers a batch dataflow.
"""

import time
import sys
import subprocess
import os
import json
import optparse
import logging
import logging.handlers
from socket import gethostname

try:
    from kafka import KafkaConsumer, KafkaClient, TopicPartition
except:
    raise
try:
    import xcalar
except:
    pass

XLRDIR = os.getenv('XLRDIR','/opt/xcalar')
DEFAULT_XC_CONFIG_FILE = os.getenv('XCE_CONFIG','/etc/xcalar/default.cfg')
hostname = gethostname()

# Set up logging
logger = logging.getLogger('CEP Connector')
logger.setLevel(logging.INFO)
# This module gets loaded multiple times. The global variables do not
# get cleared between loads. We need something in place to prevent
# multiple handlers from being added to the logger.
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    errorHandler = logging.StreamHandler(sys.stderr)
    formatterString = ('CEP: %(message)s')
    formatter = logging.Formatter(formatterString)
    errorHandler.setFormatter(formatter)
    logger.addHandler(errorHandler)
    logger.debug("initialized; hostname:{}".format(hostname))

cepEnabled = 'cep.enabled'
cepKafkaTopic = 'cep.kafka.topic'
cepKafkaHosts = 'cep.kafka.hosts'
cepKafkaGroup = 'cep.kafka.group'
cepKafkaTimeout = 'cep.kafka.timeout'
cepKafkaRecvbuf = 'cep.kafka.recvbuf'
cepBatchMap = 'cep.batchMap'
cepBatchKeyIndex = 'cep.batchKeyIndex'
cepCepExpr = 'cep.cepExpr'

batchInterval = 100 # commit offsets more often, as fewer events
realTimeInterval = 2000   # commit offsets less often, to avoid slowness
BUFSIZE = 32768*3
cepCommand = "(export XLRDIR={}; python {}/scripts/scheduleRetinas/executeFromCron/executeScheduledRetina.py {}_XcalarScheduledRetina) 2>&1 | /usr/bin/logger -t ScheduledRetina_{}"
#cepCommand = "(export XLRDIR={}; python {}/scripts/scheduleRetinas/executeFromCron/executeScheduledRetina.py {}_XcalarScheduledRetina) 2>&1 | /usr/bin/logger -t ScheduledRetina_{}"
eventTrunc = 50

def execProg(topic, batchKey, batchMap):
    try:
        batchName = batchMap[batchKey]
    except:
        logger.warn("Invalid batchKey {}, ignoring command {}.".format(batchKey, command))
        return
    cmd = cepCommand.format(XLRDIR, XLRDIR, batchName, batchName)
    logger.info("cmd = {}".format(cmd))
    ret = os.system(cmd)
    if (ret == 0):
        logger.info("Success")
    else:
        logger.info("Could not execute batch dataflow")
    return

def _readCfg(cfgPath=DEFAULT_XC_CONFIG_FILE):
    configdict = {}
    with open(cfgPath, "r") as f:
        for line in f:
            if "=" in line and not line.startswith("//") and not line.startswith("#"):
                name, var = line.partition("=")[::2]
                configdict[name] = var.strip()
    logger.info("configdict = {}".format(configdict))
    return configdict

def createConsumer(kCfg):
    hostList = kCfg[cepKafkaHosts].split(',')
    client = None
    try:
        client = KafkaClient(hostList)
    except:
        logger.error("KafkaClient error {}".format(sys.exc_info()))
        return client
    partitionIds = client.get_partition_ids_for_topic(kCfg[cepKafkaTopic])
    numPartitions = len(partitionIds)
    try:
        nodeCount = xcalar.get_node_count()
    except:
        nodeCount = 1
    try:
        nodeId = xcalar.get_node_id(xcalar.get_xpu_id())
    except:
        nodeId = 0
    partitionStart = 0
    partitionStop = numPartitions - 1
    if nodeCount > 1:
        partitionsPerNode = numPartitions/nodeCount
        partitionStart = partitionsPerNode*nodeId
        if nodeId != nodeCount+1:
            partitionStop = partitionStart + partitionsPerNode
        partitionsIds = partitionsIds[partitionStart:partitionStop]
    topicPartitions = []
    for ii in partitionIds:
         topicPartitions.append(TopicPartition(kCfg[cepKafkaTopic], ii))
    if not kCfg[cepCepExpr]:
        consumer = KafkaConsumer(group_id=kCfg[cepKafkaGroup], bootstrap_servers=hostList,
            consumer_timeout_ms=float('inf'), auto_commit_interval_ms=batchInterval)
    else:
        consumer = KafkaConsumer(group_id=kCfg[cepKafkaGroup], bootstrap_servers=hostList,
            consumer_timeout_ms=kCfg[cepKafkaTimeout], receive_buffer_bytes=kCfg[cepKafkaRecvbuf],
            auto_commit_interval_ms=realTimeInterval)
    consumer.assign(topicPartitions)
    for ii in partitionIds:
        offset = consumer.position(topicPartitions[ii])
        logger.info('offset for partition {} = {}'.format(topicPartitions[ii], offset))
        consumer.seek(topicPartitions[ii], offset)
    return consumer

def processMessages(kCfg):
    consumer = createConsumer(kCfg)
    if not consumer:
        return
    startTime = time.time()
    numConsumed = 0
    totalConsumed = 0
    for message in consumer:
        numConsumed += 1
        totalConsumed += 1
        fields = message.value.split(',')
        if not kCfg[cepCepExpr] or eval(kCfg[cepCepExpr]):
            msgTrunc = message.value[:min(eventTrunc, len(message.value))]
            if kCfg[cepCepExpr]:
                currTime = time.time()
                logger.info("Trigger processed: {}, events/sec: {}, trigger: {}, event preview: {}"
                    .format(totalConsumed, int(numConsumed/(currTime-startTime)),
                    fields[kCfg[cepBatchKeyIndex]], msgTrunc))
                numConsumed = 0
                startTime = currTime
            else:
                logger.info("Processing trigger: {}".format(msgTrunc))
            execProg(kCfg[cepKafkaTopic], fields[kCfg[cepBatchKeyIndex]], kCfg[cepBatchMap])
    return consumer

def _getConfig(cfgPath=DEFAULT_XC_CONFIG_FILE):
    configs = _readCfg(cfgPath)
    kCfg = {}

    # Use this flag to disable or enable the CEP all together. If 1, then
    # enabled, and if 0, then disabled.
    kCfg[cepEnabled] = configs.get(cepEnabled, "false") == "true"

    if not kCfg[cepEnabled]:
        return kCfg

    # Name of the Kafka topic to listen to. Each CEP listens to a unique
    # topic. If you need to process multiple topics, you will need to start
    # multiple CEPs each listening on a topic. Multiple CEP instances can
    # however listen to the same topic. See group below.
    if cepKafkaTopic in configs:
        kCfg[cepKafkaTopic] = configs[cepKafkaTopic]
    else:
        logger.error("You must define a Kafka topic in {} using the config key {} to consume data from. Disabling CEP. For instance, if the kafka topic is 'test', then you should enter your config to look like {}=test".format(cfgPath, cepKafkaTopic, cepKafkaTopic))
        kCfg[cepEnabled] = False
        return kCfg

    # The host:port list (comma separated)  of the Kafka/Zookeeper cluster.
    # For instance "host1:9092,host2:9092". You can have a single host.
    if cepKafkaHosts in configs:
        kCfg[cepKafkaHosts] = configs[cepKafkaHosts]
    else:
        kCfg[cepKafkaHosts] = 'localhost:9092'
        logger.warn("Kafka hosts not specified. Defaulting to localhost, \
        and this could be a problem, since you may not have kafka running on localhost.")

    # The Kafka consumer group to use. This group allows multiple consumers
    # to coordinate with each other to distribute the event workload. This
    # allows us to run multiple CEPs on a Xcalar cluster, typically but not
    # limited to one per node.
    if cepKafkaGroup in configs:
        kCfg[cepKafkaGroup] = configs[cepKafkaGroup]
    else:
        testGroup = 'test'
        kCfg[cepKafkaGroup] = testGroup
        logger.warn("You did not define kafka.group! \
        We are defaulting to group {}, but this may not be what you want. \
        The choice of a consumer group may adversely affect how the events are consumed, \
        and unexpected behavior may be observed. If this is not what you want, please \
        go ahead and define a consumer group.".format(testGroup))

    # index of the column in csv event, whose value is the key to lookup
    # batchMap to get the name of the batch dataflow to invoke.
    kCfg[cepBatchKeyIndex] = int(configs.get(cepBatchKeyIndex, 0))

    # This is the CEP business rules. Currently, one expression can be
    # specified, but you can write any python compilable expression which
    # must return a boolean. So it can be a conditional expression or a call
    # to function that returns a boolean. If you need realtime processing
    # where every incoming message triggers a batch dataflow, you do not
    # need to set cepExpr.
    kCfg[cepCepExpr] = configs.get(cepCepExpr, "")

    # The batchMap allows a CEP to trigger a different batch dataflow
    # and all batch dataflows are configured in this map. You must
    # have a key called "realtime" in the batchMap, if you are going
    # to skip a cepExpr rule (see below) and would like to invoke
    # all batch dataflows in realtime.
    kCfg[cepBatchMap] = json.loads(configs.get(cepBatchMap, '{"realtime" : "realtime"}'))

    # This is the kafka timeout. If set to 'inf', it is infinity and
    # will never timeout. If set to too low, the CEP will exit.
    kCfg[cepKafkaTimeout] = float(configs.get(cepKafkaTimeout, 'inf'))

    # The receive buffer size can be tuned for incoming network traffic
    # and size of the events. This is activated only when cepExpr is true.
    # This is because when cepExpr is true, we are processing events in realtime
    # and the consumer will not wait for the buffer to be full, but instead
    # act immediately when an event arrives.
    kCfg[cepKafkaRecvbuf] = int(configs.get(cepKafkaRecvbuf, BUFSIZE))

    logger.info("Config = {}".format(kCfg))
    return kCfg

def processMain(kCfg):
    consumer = None
    try:
        consumer = processMessages(kCfg)
    except:
        logger.exception("Could not consume messages {}".format(sys.exc_info()))
        raise
    return consumer

"""
This method is called when cep is executed as a system app. It attempts
to read default.cfg and enter processMain which should never exit. If it
does exit, it's usually because of an error condition or a kafka timeout.
If this throws an exception or gets shot down due to SIGKILL, main() should be called
again, else if main returns a status, do not attempt to call main() again.
"""
def main(dummyInput):
    ret = 0
    try:
        ret = xcalar.getChildId()
    except:
        pass
    if ret != 0:
        return json.dumps({"status" : "CEP child exited gracefully."})

    # if any errors occur during parsing config, do not restart
    cfgPath = os.getenv('XCE_CONFIG','/etc/xcalar/default.cfg')
    try:
        kCfg = _getConfig(cfgPath)
    except Exception:
        logger.exception("error in config {}".format(sys.exc_info()))
        return json.dumps({"status" : "Config errors found {} int config file {}.".format(sys.exc_info(), cfgPath)})

    # if CEP is disabled, do not restart (TODO: Caller should not call main in the first place)
    if not kCfg[cepEnabled]:
        logger.info("CEP is disabled. Exiting CEP gracefully.")
        return json.dumps({"status": "CEP is disabled. To enable CEP, use the {} flag in the config file {}.".format(cepEnabled, cfgPath)})

    consumer = None
    try:
        consumer = processMain(kCfg)
    except:
        logger.error("Consumer processing Error occurred, restart advised.{}".format(sys.exc_info()))
        raise

    # If KafkaClient fails, we should not restart
    if not consumer:
        return json.dumps({"status" : "CEP Kafka brokers not available at {}. Please check configs at {} to see if the {} has been defined correctly.".format(kCfg[cepKafkaHosts], DEFAULT_XC_CONFIG_FILE, cepKafkaHosts)})

    return json.dumps({"status" : "Unexpected exit occurred, restart recommended."})

if __name__ == "__main__":
    logger.info(main(None))
