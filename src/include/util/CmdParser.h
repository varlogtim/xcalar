// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CMDPARSER_H_
#define _CMDPARSER_H_

#include <stdio.h>

#include "primitives/Primitives.h"

typedef enum {
    CmdInteractiveMode,
    CmdReadFromFileMode,
    CmdReadFromOptArgMode,
} CmdMode;

typedef struct CmdParserCursorStruct CmdParserCursor;

Status cmdInitParseCmdCursor(CmdParserCursor **cmdParserCursor);

Status cmdParseQuery(char *optArgCmd,
                     int *argcIn,
                     char **argvIn[],
                     CmdParserCursor *cmdParserCursor);
Status cmdParseCmd(char *optArgCmd,
                   FILE *fp,
                   int *argcIn,
                   char **argvIn[],
                   CmdParserCursor *cmdParserCursor,
                   CmdMode cmdMode,
                   bool addHistory);
void cmdFreeParseCmdCursor(CmdParserCursor **cmdParserCursor);

#endif  // _CMDPARSER_H_
