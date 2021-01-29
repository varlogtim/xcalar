// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>
#include <string.h>
#include <assert.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "df/DataFormatTypes.h"
#include "df/DataFormat.h"
#include "stat/StatisticsTypes.h"
#include "cli/CliCoreUtils.h"
#include "util/CmdParser.h"
#include "QueryParserEnums.h"
#include "queryparser/QueryParser.h"
#include "util/MemTrack.h"
#include "exec/ExecXcalarCmd.h"

bool cliUserIdUniqueUserSpecified = false;
char cliUsername[LOGIN_NAME_MAX + 1];
unsigned int cliUserIdUnique;
// TODO: pull from source file.  extern usage
//       from "outside"
const char *cliDestIp = XcalarApiDefaultDestIP;
uint16_t cliDestPort = XcalarApiDefaultDestPort;

// TODO: pull from source file.  extern usage
//       from "outside"
const ExecCommandMapping execCoreCommandMappings[] = {
    // Core utils
    {.commandString = "shutdown",
     .commandDesc = "Shuts down Xcalar cluster",
     .main = cliShutdownMain,  // coreutils/Shutdown.cpp
     .usage = cliShutdownHelp},
    {
        .commandString = "load",
        .commandDesc = "Load data into Xcalar DB",
        .main = cliLoadMain,  // coreutils/Load.cpp
        .usage = cliLoadHelp,
    },
    {.commandString = "list",
     .commandDesc = "Get a list of stuff (tables or datasets)",
     .main = cliListMain,  // coreutils/List.cpp
     .usage = cliListHelp},
    {.commandString = "index",
     .commandDesc = "Create index on dataset",
     .main = cliIndexMain,  // coreutils/Index.cpp
     .usage = cliIndexHelp},
    {.commandString = "drop",
     .commandDesc = "Drop a dag node",
     .main = cliDropMain,  // coreutils/Drop.cpp
     .usage = cliDropHelp},
    {.commandString = "delist",
     .commandDesc = "Delist a dataset",
     .main = cliDelistMain,  // coreutils/Delist.cpp
     .usage = cliDelistHelp},
    {.commandString = "inspect",
     .commandDesc = "Get detailed information about a table",
     .main = cliInspectMain,  // coreutils/Inspect.cpp
     .usage = cliInspectHelp},
    {.commandString = "project",
     .commandDesc = "Specify a set of columns to keep and discard the rest",
     .main = cliProjectMain,  // coreutils/Project.cpp
     .usage = cliProjectHelp},
    {.commandString = "filter",
     .commandDesc = "Filter table by operation",
     .main = cliFilterMain,  // coreutils/Filter.cpp
     .usage = cliFilterHelp},
    {.commandString = "groupBy",
     .commandDesc = "Aggregate an operation on values of same key",
     .main = cliGroupByMain,  // coreutils/GroupBy.cpp
     .usage = cliGroupByHelp},
    {.commandString = "join",
     .commandDesc = "Join 2 tables to spawn a new table",
     .main = cliJoinMain,  // coreutils/Join.cpp
     .usage = cliJoinHelp},
    {.commandString = "stats",
     .commandDesc = "Get stats from node(s)",
     .main = cliGetStatsMain,  // coreutils/Stats.cpp
     .usage = cliGetStatsHelp},
    {.commandString = "resetstats",
     .commandDesc = "Reset stats on node",
     .main = cliResetStatsMain,  // coreutils/Stats.cpp
     .usage = cliResetStatsHelp},
    {
        .commandString = "map",
        .commandDesc = "Evaluate an expression across all records",
        .main = cliMapMain,  // coreutils/Map.cpp
        .usage = cliMapHelp,
    },
    {
        .commandString = "getRowNum",
        .commandDesc = "Add row number as a column in table",
        .main = cliGetRowNumMain,  // coreutils/GetRowNum.cpp
        .usage = cliGetRowNumHelp,
    },
    {.commandString = "aggregate",
     .commandDesc = "Aggregate an operation on all values of a table",
     .main = cliAggregateMain,
     .usage = cliAggregateHelp},
    // XXX: integrate with the inspect function
    {.commandString = "inspectquery",
     .commandDesc = "Inspect query status",
     .main = cliQueryInspectMain,  // coreutils/Query.cpp
     .usage = cliQueryInspectHelp},
    {.commandString = "rename",
     .commandDesc = "Rename a column in a CSV dataset",
     .main = cliRenameMain,  // coreutils/Rename.cpp
     .usage = cliRenameHelp},
    {.commandString = "cast",
     .commandDesc = "Change the type of a column in a CSV dataset",
     .main = cliCastMain,  // coreutils/Cast.cpp
     .usage = cliCastHelp},
    {
        .commandString = "dag",
        .commandDesc = "Get the DAG for a table",
        .main = cliDagMain,  // coreutils/DagCli.cpp
        .usage = cliDagHelp,
    },
    {
        .commandString = "cancel",
        .commandDesc = "cancel an operation",
        .main = cliCancelMain,  // coreutils/CancelCli.cpp
        .usage = cliCancelHelp,
    },
    {
        .commandString = "opstats",
        .commandDesc = "get operation stats",
        .main = cliGetOpStatusMain,  // coreutils/GetOpSTatusCli.cpp
        .usage = cliGetOpStatusHelp,
    },
    {.commandString = "session",
     .commandDesc = "Display and manage sessions",
     .main = cliSessionMain,  // coreutils/Session.cpp
     .usage = cliSessionHelp},
    // XXX: Integrate with stats module
    {
        .commandString = "top",
        .commandDesc = "Get cpu and memory information",
        .main = cliTopMain,  // coreutils/Stats.cpp
        .usage = cliTopHelp,
    },
    {
        .commandString = "dht",
        .commandDesc = "DHT related functions",
        .main = cliDhtMain,  // coreutils/Dht.cpp
        .usage = cliDhtHelp,
    },
    {
        .commandString = "support",
        .commandDesc = "Support and diagnostic functions",
        .main = cliSupportMain,  // coreutils/Support.cpp
        .usage = cliSupportHelp,
    },
    {
        .commandString = "python",
        .commandDesc = "Python related functions",
        .main = cliPythonMain,  // coreutils/Python.cpp
        .usage = cliPythonHelp,
    },
    {
        .commandString = "functests",
        .commandDesc = "Functional test suite",
        .main = cliFuncTestsMain,  // coreutils/FuncTests.cpp
        .usage = cliFuncTestsHelp,
    },
    {
        .commandString = "loglevelset",
        .commandDesc = "Set the logging level, flush level and flush period",
        .main = cliLogLevelSetMain,  // coreutils/LogLevelSet.cpp
        .usage = cliLogLevelSetHelp,
    },
    {
        .commandString = "getipaddr",
        .commandDesc = "Get a node's IP address",
        .main = cliGetIpAddrMain,  // coreutils/GetIpAddr.cpp
        .usage = cliGetIpAddrHelp,
    },
    {
        .commandString = "loglevelget",
        .commandDesc = "Get the logging level, and log flush period",
        .main = cliLogLevelGetMain,  // coreutils/LogLevelGet.cpp
        .usage = cliLogLevelGetHelp,
    },
};

const unsigned execNumCoreCommandMappings =
    (unsigned) ArrayLen(execCoreCommandMappings);
