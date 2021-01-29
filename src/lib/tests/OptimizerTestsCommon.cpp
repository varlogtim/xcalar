// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/MemTrack.h"
#include "dag/DagLib.h"
#include "queryparser/QueryParser.h"
#include "sys/XLog.h"
#include "TestQueries.h"
#include "optimizer/Optimizer.h"
#include "OptimizerTestsCommon.h"

static constexpr const char *moduleName = "libOptimizerFuncTest";

// This allows selectively disabling assert() verifications.
//
// Tests that do straight forward operations that are deterministic and
// verifiable should have assert checking done.
//
// Tests which "spew" operations at the library non-deterministically
// can't check asserts as the interactions of the operations occur in a
// random fashion.  Thus the only "pass" is not not crash.
static void
myAssert(bool condition, bool doVerify)
{
    if (!doVerify) {
        // In the future we might want to do some checking
        return;
    }
    assert(condition);
}

// Iterate through the dag nodes in the query graph and d
static Status
examineQueryGraph(Dag *queryGraph)
{
    Status status = StatusUnknown;
    DagTypes::NodeId dagNodeId;
    char dagNodeName[DagTypes::MaxNameLen];
    DagNodeTypes::Node *queryGraphNode = NULL;
    Optimizer *optimizer = Optimizer::get();
    char(*projectedFieldNames)[DfMaxFieldNameLen + 1] = NULL;
    unsigned numProjectedFieldNames;

    status = queryGraph->getFirstDagInOrder(&dagNodeId);
    BailIfFailed(status);

    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->getDagNodeName(dagNodeId,
                                            dagNodeName,
                                            sizeof(dagNodeName));
        BailIfFailed(status);

        queryGraph->lock();
        status = queryGraph->lookupNodeById(dagNodeId, &queryGraphNode);
        queryGraph->unlock();
        BailIfFailed(status);

        // Now given a dagNode, take its dagNode->annotations, and pass it to
        // getProjectedFieldNames, to have the optimizer tell you what fields
        // are required by this dagNode and its descendants.
        status = optimizer->getProjectedFieldNames(queryGraphNode->annotations,
                                                   &projectedFieldNames,
                                                   &numProjectedFieldNames);
        BailIfFailed(status);

#ifdef TESTING_THE_TEST
        for (uint64_t ii = 0; ii < numProjectedFieldNames; ii++) {
            xSyslog(moduleName,
                    XlogVerbose,
                    "Dag name: %s, field name[%d]: %s",
                    dagNodeName,
                    (int) ii,
                    projectedFieldNames[ii]);
        }
#else
        xSyslog(moduleName,
                XlogVerbose,
                "Dag %s has %d projected field names",
                dagNodeName,
                numProjectedFieldNames);
#endif

        if (projectedFieldNames != NULL) {
            memFree(projectedFieldNames);
            projectedFieldNames = NULL;
        }

        status = queryGraph->getNextDagInOrder(dagNodeId, &dagNodeId);
        BailIfFailed(status);
    }

CommonExit:

    return status;
}

Status
optimizeCmd(const char *query)
{
    Status status;
    Dag *queryGraph = NULL;
    uint64_t numQueryGraphNodes;
    QueryParser *qp = QueryParser::get();
    DagLib *daglib = DagLib::get();
    Optimizer *optimizer = Optimizer::get();

    // Parse the command into a Query Graph

    status = qp->parse((char *) query, NULL, &queryGraph, &numQueryGraphNodes);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "DoOptimize: Unable to parse query: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(queryGraph != NULL);

    status = optimizer->optimize(queryGraph, NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "DoOptimize: Unable to optimize query: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Look around the resultant optimized query graph.
    status = examineQueryGraph(queryGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "DoOptimize: Unable to examine query: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (queryGraph != NULL) {
        Status status2 =
            daglib->destroyDag(queryGraph,
                               DagTypes::DestroyDeleteAndCleanNodes);
        assert(status2 == StatusOk);
        queryGraph = NULL;
    }

    return status;
}

Status
optDoOptimizerTest(uint32_t threadNum)
{
    Status status = StatusOk;
    char buffer[MaxTestQueryBufSize];
    static const size_t ThreadUniqueBufSize = 256;
    char threadUniqueBuf[ThreadUniqueBufSize];
    static const size_t RandomMemoryBufSize = 4 * KB;
    char *randomMemory = NULL;
    Config *config = Config::get();

    // XXX: change if random, interacting tests
    bool verify = true;

    // Each thread must uniqueify its dataset names to avoid stomping
    // on each other.
    snprintf(threadUniqueBuf,
             ThreadUniqueBufSize,
             "%s-Node%dThread%d",
             moduleName,
             config->getMyNodeId(),
             threadNum);

    // Customer 1 Query
    snprintf(buffer,
             MaxTestQueryBufSize,
             cust1Query,
             threadUniqueBuf,
             threadUniqueBuf,
             threadUniqueBuf,
             threadUniqueBuf,
             cust1QueryFormat1);
    status = optimizeCmd(buffer);
    myAssert(status == StatusOk, verify);

    // Customer 2 Query
    snprintf(buffer,
             MaxTestQueryBufSize,
             cust2Query,
             threadUniqueBuf,
             threadUniqueBuf);
    status = optimizeCmd(buffer);
    myAssert(status == StatusOk, verify);

    // Flight Demo Query
    snprintf(buffer,
             MaxTestQueryBufSize,
             fdQuery,
             threadUniqueBuf,
             threadUniqueBuf,
             threadUniqueBuf,
             threadUniqueBuf);
    status = optimizeCmd(buffer);
    myAssert(status == StatusOk, verify);

    // Another Customer 2 Query
    snprintf(buffer,
             MaxTestQueryBufSize,
             cust1_2Query,
             threadUniqueBuf,
             threadUniqueBuf,
             threadUniqueBuf,
             threadUniqueBuf);
    status = optimizeCmd(buffer);
    myAssert(status == StatusOk, verify);

    // Random memory
    randomMemory = (char *) memAllocExt(RandomMemoryBufSize, moduleName);
    if (randomMemory == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // Null terminate the random memory
    randomMemory[RandomMemoryBufSize - 1] = '\0';

    // Depending on what's in the memory will decide which module returns
    // which error.
    status = optimizeCmd(randomMemory);
    myAssert(status == StatusOk || status == StatusDgDagEmpty ||
                 status == StatusInval ||
                 status == StatusAstMalformedEvalString ||
                 status == StatusDgDagAlreadyExists ||  // Xc-6338
                 status == StatusJsonQueryParseError ||
                 status == StatusJsonError || status == StatusCliParseError,
             verify);

    // Random memory which previously lead to crashed (Xc-5982)
    randomMemory[0] = 0xcf;
    randomMemory[1] = 0x5c;
    randomMemory[2] = 0x20;
    randomMemory[3] = 0;
    status = optimizeCmd(randomMemory);
    myAssert(status == StatusInval, verify);

    // XXX: Add more tests

CommonExit:
    if (randomMemory != NULL) {
        memFree(randomMemory);
        randomMemory = NULL;
    }

    return status;
}
