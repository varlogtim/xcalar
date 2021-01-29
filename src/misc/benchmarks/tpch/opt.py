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
from xcalar.external.LegacyApi.Optimizer import Optimizer

retinaName = "ret1"
api = XcalarApi(bypass_proxy=True)
retina = Retina(api)

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

    optimizer = Optimizer(retObj["query"], retObj["tables"])
    optimizer.optimize()

    retObj["query"] = optimizer.graph.toQueryDict()
    import pprint
    pp = pprint.PrettyPrinter(indent=4)

    pp.pprint(retObj["query"])
    retina.update(retinaName, retObj)

    exportedRetina = retina.export(retinaName)
    outPath = retinaPath.split('/')[-1]

    out = open(outPath, "wb")
    out.write(exportedRetina)
