#!/bin/bash

RESOURCE_GROUP="${RESOURCE_GROUP:-blim-tpch-xcalar-64x323nx6psmgwse6w}"
SSH_USER="${SSH_USER:-azureuser}"
SSH_PASSWORD="${SSH_PASSWORD:-i0turb1ne!}"
DATAFLOW_PATH="${DATAFLOW_PATH:-/netstore/qa/dataflows/msftTpch/q15/TPCH-Q15-final.tar.gz}"

DATASET_URL="${DATASET_URL:-azblob://xcdatasetswestus2/tpch-sf1000-10kfiles}"
PUBIP="13.66.223.200"

python2.7 run.py -r ${RESOURCE_GROUP} -u ${SSH_USER} -p ${SSH_PASSWORD} -b ${DATAFLOW_PATH} -s ${DATASET_URL} -i ${PUBIP}
