// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CLI_COREUTILS_H_
#define _CLI_COREUTILS_H_

#include "WorkItem.h"

// ----- Global variables provided by CLI environment -----
extern const char *cliDestIp;
extern uint16_t cliDestPort;
extern char cliDestIpBuf[];
extern char cliUsername[LOGIN_NAME_MAX + 1];
extern unsigned int cliUserIdUnique;
extern bool cliUserIdUniqueUserSpecified;  // If false, we pick a default

// ----- Core utils prototype -----
// XXX:this file could be replaced by Macros
// cat
extern void cliCatMain(int argc,
                       char *argv[],
                       XcalarWorkItem *workItemIn,
                       bool prettyPrint,
                       bool interactive);
extern void cliCatHelp(int argc, char *argv[]);

// drop
extern void cliDropMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
extern void cliDropHelp(int argc, char *argv[]);

// project
extern void cliProjectMain(int argc,
                           char *argv[],
                           XcalarWorkItem *workItemIn,
                           bool prettyPrint,
                           bool interactive);
extern void cliProjectHelp(int argc, char *argv[]);

// filter
extern void cliFilterMain(int argc,
                          char *argv[],
                          XcalarWorkItem *workItemIn,
                          bool prettyPrint,
                          bool interactive);
extern void cliFilterHelp(int argc, char *argv[]);

// stats
extern void cliGetStatsMain(int argc,
                            char *argv[],
                            XcalarWorkItem *workItemIn,
                            bool prettyPrint,
                            bool interactive);
extern void cliGetStatsHelp(int argc, char *argv[]);

// groupBy
extern void cliGroupByMain(int argc,
                           char *argv[],
                           XcalarWorkItem *workItemIn,
                           bool prettyPrint,
                           bool interactive);
extern void cliGroupByHelp(int argc, char *argv[]);

// index
extern void cliIndexMain(int argc,
                         char *argv[],
                         XcalarWorkItem *workItemIn,
                         bool prettyPrint,
                         bool interactive);
extern void cliIndexHelp(int argc, char *argv[]);

// inspect
extern void cliInspectMain(int argc,
                           char *argv[],
                           XcalarWorkItem *workItemIn,
                           bool prettyPrint,
                           bool interactive);
extern void cliInspectHelp(int argc, char *argv[]);

// join
extern void cliJoinMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
extern void cliJoinHelp(int argc, char *argv[]);

// list
extern void cliListMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
extern void cliListHelp(int argc, char *argv[]);

// load
extern void cliLoadMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
extern void cliLoadHelp(int argc, char *argv[]);

// resetstats
extern void cliResetStatsMain(int argc,
                              char *argv[],
                              XcalarWorkItem *workItemIn,
                              bool prettyPrint,
                              bool interactive);
extern void cliResetStatsHelp(int argc, char *argv[]);

// shutdown
extern void cliShutdownMain(int argc,
                            char *argv[],
                            XcalarWorkItem *workItemIn,
                            bool prettyPrint,
                            bool interactive);
extern void cliShutdownHelp(int argc, char *argv[]);

// map
extern void cliMapMain(int argc,
                       char *argv[],
                       XcalarWorkItem *workItemIn,
                       bool prettyPrint,
                       bool interactive);
extern void cliMapHelp(int argc, char *argv[]);

// getRowNum
extern void cliGetRowNumMain(int argc,
                             char *argv[],
                             XcalarWorkItem *workItemIn,
                             bool prettyPrint,
                             bool interactive);
extern void cliGetRowNumHelp(int argc, char *argv[]);

// aggregate
extern void cliAggregateMain(int argc,
                             char *argv[],
                             XcalarWorkItem *workItemIn,
                             bool prettyPrint,
                             bool interactive);
extern void cliAggregateHelp(int argc, char *argv[]);

// query
extern void cliQueryHelp(int argc, char *argv[]);

// inspectquery
extern void cliQueryInspectMain(int argc,
                                char *argv[],
                                XcalarWorkItem *workItemIn,
                                bool prettyPrint,
                                bool interactive);
extern void cliQueryInspectHelp(int argc, char *argv[]);

// rename
extern void cliRenameMain(int argc,
                          char *argv[],
                          XcalarWorkItem *workItemIn,
                          bool prettyPrint,
                          bool interactive);
extern void cliRenameHelp(int argc, char *argv[]);

// cast
extern void cliCastMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
extern void cliCastHelp(int argc, char *argv[]);

// execute cli cmds from within c function
extern Status cliExecuteCmd(char *input);

// dag
void cliDagHelp(int argc, char *argv[]);
void cliDagMain(int argc,
                char *argv[],
                XcalarWorkItem *workItemIn,
                bool prettyPrint,
                bool interactive);
// Cancel
void cliCancelHelp(int argc, char *argv[]);
void cliCancelMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive);
// get op stats
void cliGetOpStatusHelp(int argc, char *argv[]);
void cliGetOpStatusMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);

// Session
void cliSessionHelp(int argc, char *argv[]);
void cliSessionMain(int argc,
                    char *argv[],
                    XcalarWorkItem *workItemIn,
                    bool prettyPrint,
                    bool interactive);

// retina
void cliRetinaHelp(int argc, char *argv[]);
void cliRetinaMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive);

void cliExecRetinaHelp(int argc, char *argv[]);
void cliExecRetinaMain(int argc,
                       char *argv[],
                       XcalarWorkItem *workItemIn,
                       bool prettyPrint,
                       bool interactive);

// top
void cliTopHelp(int argc, char *argv[]);
void cliTopMain(int argc,
                char *argv[],
                XcalarWorkItem *workItemIn,
                bool prettyPrint,
                bool interactive);

// memory
void cliMemoryHelp(int argc, char *argv[]);
void cliMemoryMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive);

// dht
void cliDhtHelp(int argc, char *argv[]);
void cliDhtMain(int argc,
                char *argv[],
                XcalarWorkItem *workItemIn,
                bool prettyPrint,
                bool interactive);

// support
void cliSupportHelp(int argc, char *argv[]);
void cliSupportMain(int argc,
                    char *argv[],
                    XcalarWorkItem *workItemIn,
                    bool prettyPrint,
                    bool interactive);

// python
void cliPythonHelp(int argc, char *argv[]);
void cliPythonMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive);

// functests
void cliFuncTestsHelp(int argc, char *argv[]);
void cliFuncTestsMain(int argc,
                      char *argv[],
                      XcalarWorkItem *workItemIn,
                      bool prettyPrint,
                      bool interactive);

// delist
void cliDelistHelp(int argc, char *argv[]);
void cliDelistMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive);

// loglevelset
void cliLogLevelSetHelp(int argc, char *argv[]);
void cliLogLevelSetMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);

// loglevelget
void cliLogLevelGetHelp(int argc, char *argv[]);
void cliLogLevelGetMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);

// getipaddr
void cliGetIpAddrHelp(int argc, char *argv[]);
void cliGetIpAddrMain(int argc,
                      char *argv[],
                      XcalarWorkItem *workItemIn,
                      bool prettyPrint,
                      bool interactive);
#endif  // _CLI_COREUTILS_H_
