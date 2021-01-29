#!/bin/bash

export SPARK_MASTER_HOST=`hostname`

export SPARK_HOME=/spark

. "$SPARK_HOME/sbin/spark-config.sh"

. "$SPARK_HOME/bin/load-spark-env.sh"

mkdir -p $SPARK_MASTER_LOG


ln -sf /dev/stdout $SPARK_MASTER_LOG/spark-master.out

cd $SPARK_HOME/sbin
./start-thriftserver.sh

export SPARK_NO_DAEMONIZE=1
cd $SPARK_HOME/bin
exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT

export SPARK_NO_DAEMONIZE=1
#cd $SPARK_HOME/sbin
#exec ./start-thriftserver.sh
