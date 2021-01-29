# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import sys
import time

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.Retina import Retina

retinaName = "ret"
api = XcalarApi(bypass_proxy=True)
retina = Retina(api)


def convertLoad(node, graph):
    # remove this node and all nodes until the project
    datasetName = node.operator["args"]["loadArgs"]["sourceArgsList"][0]["path"].split('/')[-1]

    while node.operator["operation"] != "XcalarApiProject":
        assert(len(node.children) == 1)
        graph.removeNode(node.name)
        node = node.children[0]

    assert(node.operator["operation"] == "XcalarApiProject")
    project = node

    # convert the project to a synthesize
    project.operator["operation"] = "XcalarApiSynthesize"
    project.operator["args"]["source"] = datasetName
    project.operator["args"]["sameSession"] = False
    project.operator["args"]["columns"] = []

def removeExistanceFilter(node, graph):
    # check if this filter is only doing exists by counting
    # the exists statements.
    evalStr = node.operator["args"]["eval"][0]["evalString"]
    if evalStr.count("exists") == evalStr.count(",") + 1:
        graph.removeNode(node.name)

for retinaPath in sys.argv[1:]:
    print("==========Fixing Retina " + retinaPath)

    with open(retinaPath, "rb") as fp:
        retinaContents = fp.read()

    try:
        retina.add(retinaName, retinaContents)
    except:
        retina.delete(retinaName)
        retina.add(retinaName, retinaContents)

    graph = retina.getGraph(retinaName)
    retObj = retina.getDict(retinaName)

    for nodeName, node in graph.dag.copy().items():
        if node.operator["operation"] == "XcalarApiBulkLoad":
            convertLoad(node, graph)
        elif node.operator["operation"] == "XcalarApiFilter":
            removeExistanceFilter(node, graph)

    retObj["query"] = graph.toQueryDict()

    retina.update(retinaName, retObj)

    exportedRetina = retina.export(retinaName)
    outPath = retinaPath.split('/')[-1]

    out = open(outPath, "wb")
    out.write(exportedRetina)
