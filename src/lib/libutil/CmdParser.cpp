// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <cstdlib>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <getopt.h>
#include "StrlFunc.h"

#include "util/CmdParser.h"
#include "libapis/LibApisCommon.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "LibUtilConstants.h"

using namespace util;

struct CmdParserCursorStruct {
    // This is used to keep track of whether CmdReadFromOptArgMode has been
    // called
    bool init;
    char *rawCommandLine;
    char *commandLine;    // trimmed pointer into rawCommandLine
    void *commandCursor;  // pointer into commandLine;
    Status status;
};

static constexpr const char *moduleName = "cmdParser";

static Status
getNextCommand(const char *commandLine,
               void **commandCursorIn,
               int *argcIn,
               char **argvIn[])
{
    State currentState;
    const char *commandCursor;
    int argc, numArgs, jj;
    size_t *argLen = NULL;
    char **argv = NULL;
    bool stopParsing;
    unsigned ii;
    Status status = StatusUnknown;

    // Unfortunately, we have to parse input thrice
    // First time to figure out size of argv,
    // second time to figure out size of each arglen
    // third time to actually populate argv
    argv = NULL;
    numArgs = 0;
    for (ii = 0; ii < 3; ii++) {
        argc = 0;
        currentState = Whitespace;
        commandCursor = (*commandCursorIn == NULL)
                            ? commandLine
                            : *((char **) commandCursorIn);
        stopParsing = false;

        while (!stopParsing) {
            switch (currentState) {
            case DblQuoteOpened:
                if (*commandCursor == '"') {
                    currentState = DefaultState;
                } else {
                    if (*commandCursor == '\\') {
                        commandCursor++;
                    }

                    if (argv != NULL) {
                        assert(argLen != NULL);
                        if (argv[argc - 1] != NULL) {
                            (argv[argc - 1])[argLen[argc - 1]] = *commandCursor;
                        }
                        argLen[argc - 1]++;
                    }
                }
                break;
            case SingleQuoteOpened:
                if (*commandCursor == '\'') {
                    currentState = DefaultState;
                } else if (argv != NULL) {
                    assert(argLen != NULL);
                    if (argv[argc - 1] != NULL) {
                        (argv[argc - 1])[argLen[argc - 1]] = *commandCursor;
                    }
                    argLen[argc - 1]++;
                }
                break;
            case Whitespace:
                if (*commandCursor == '#') {
                    currentState = Comment;
                } else if (!strIsWhiteSpace(*commandCursor)) {
                    currentState = DefaultState;
                    argc++;
                    continue;
                }
                break;
            case Comment:
                // For single line comment, everythign is a sink until the \n
                if (*commandCursor == '\n') {
                    currentState = Whitespace;
                }
                break;
            case DefaultState:
                if (*commandCursor == '"') {
                    currentState = DblQuoteOpened;
                } else if (*commandCursor == '\'') {
                    currentState = SingleQuoteOpened;
                } else if (strIsWhiteSpace(*commandCursor) ||
                           *commandCursor == '\0' || *commandCursor == ';') {
                    if (argv != NULL) {
                        if (argv[argc - 1] == NULL) {
                            argv[argc - 1] =
                                (char *) memAllocExt(argLen[argc - 1] + 1,
                                                     moduleName);
                            if (argv[argc - 1] == NULL) {
                                status = StatusNoMem;
                                goto CommonExit;
                            }
                        } else {
                            assert(argLen != NULL);
                            (argv[argc - 1])[argLen[argc - 1]] = '\0';
                        }
                    }

                    if (strIsWhiteSpace(*commandCursor)) {
                        currentState = Whitespace;
                    } else if (*commandCursor == ';') {
                        status = StatusMore;
                        stopParsing = true;
                    }
                } else {
                    if (*commandCursor == '\\') {
                        commandCursor++;
                    }
                    if (argv != NULL) {
                        assert(argLen != NULL);
                        if (argv[argc - 1] != NULL) {
                            (argv[argc - 1])[argLen[argc - 1]] = *commandCursor;
                        }
                        argLen[argc - 1]++;
                    }
                }
                break;
            default:
                assert(0);
                break;
            }

            if (*commandCursor == '\0') {
                stopParsing = true;
            } else {
                commandCursor++;
            }
        }

        if (argc == 0) {
            assert(argv == NULL);
            assert(argLen == NULL);
            break;
        }

        assert(argc > 0);
        if (argv == NULL) {
            numArgs = argc;
            argv = (char **) memAllocExt(sizeof(*argv) * argc, moduleName);
            if (argv == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            for (jj = 0; jj < argc; jj++) {
                argv[jj] = NULL;
            }

            argLen = (size_t *) memAllocExt(sizeof(*argLen) * argc, moduleName);
            if (argLen == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            for (jj = 0; jj < argc; jj++) {
                argLen[jj] = 0;
            }
        }

        assert(argv != NULL);
        assert(argLen != NULL);
        for (jj = 0; jj < argc; jj++) {
            argLen[jj] = 0;
        }
    }

    if (*commandCursor == '\0') {
        status = StatusOk;
    } else {
        assert(status == StatusMore);
    }

    for (jj = 0; jj < argc; jj++) {
        if (argv[jj] == NULL) {
            // Funky random parsing strings can lead to this
            status = StatusInval;
            goto CommonExit;
        }
    }

    *commandCursorIn = (void *) commandCursor;
    *argcIn = argc;
    *argvIn = argv;

CommonExit:
    if (status != StatusOk && status != StatusMore) {
        if (argv != NULL) {
            assert(numArgs > 0);
            for (jj = 0; jj < numArgs; jj++) {
                if (argv[jj] != NULL) {
                    memFree(argv[jj]);
                    argv[jj] = NULL;
                }
            }
            memFree(argv);
            argv = NULL;
        }
    }

    if (argLen != NULL) {
        memFree(argLen);
        argLen = NULL;
    }

    return status;
}

static Status
getNextRawLine(
    char *optArgCmd, FILE *fp, char **bufIn, size_t *bufLenIn, CmdMode cmdMode)
{
    Status status;
    char *buf = NULL;
    size_t bufLen;   // Doens't include '\0'
    size_t bufSize;  // Includes '\0'
    unsigned start;
    size_t optArgLen;

    assert(bufIn != NULL);
    assert(bufLenIn != NULL);

    switch (cmdMode) {
    case CmdInteractiveMode:
        // @SymbolCheckIgnore
        panic("Command line is no longer supported.");
        break;
    case CmdReadFromOptArgMode: {
        assert(optArgCmd != NULL);
        optArgLen = strlen(optArgCmd);

        buf = (char *) memAllocExt(optArgLen + 1, moduleName);
        if (buf == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        status = strStrlcpy(buf, optArgCmd, optArgLen + 1);
        BailIfFailed(status);
        break;
    }
    case CmdReadFromFileMode:
        if (feof(fp)) {
            status = StatusNoData;
            goto CommonExit;
        }

        bufSize = 256;
        buf = (char *) memAllocExt(bufSize, moduleName);
        if (buf == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        if (fgets(buf, (int) bufSize, fp) == NULL) {
            status = (ferror(fp)) ? StatusCliErrorReadFromFile : StatusNoData;
            goto CommonExit;
        }

        break;
    }

    if (buf == NULL) {
        status = StatusNoData;
        goto CommonExit;
    }

    bufLen = strlen(buf);
    if (cmdMode == CmdReadFromFileMode && bufLen == bufSize - 1) {
        status = StatusCliLineTooLong;
        goto CommonExit;
    }

    if (*bufLenIn == 0) {
        start = 0;
        *bufLenIn = bufLen;
    } else {
        assert(*bufLenIn <= UINT32_MAX);
        start = (uint32_t) *bufLenIn;  // Overwrite the previous NULL
        *bufLenIn += bufLen + 1;       // Including the '\n' character
    }

    bufSize = *bufLenIn + 1;  // Include the '\0'
    *bufIn = (char *) memReallocExt(*bufIn, bufSize, moduleName);

    if (start == 0) {
        strlcpy(*bufIn, buf, bufLen + 1);
    } else {
        (*bufIn)[start] = '\n';
        start++;
        memcpy((char *) ((uintptr_t)(*bufIn) + start), buf, bufLen + 1);
    }

    status = StatusOk;

CommonExit:
    if (buf) {
        if (cmdMode == CmdInteractiveMode) {
            // In this case, it is being allocated within getline
            // @SymbolCheckIgnore
            free(buf);
            buf = NULL;
        } else {
            // Otherwise, we are allocating it above
            memFree(buf);
            buf = NULL;
        }
    }

    return status;
}

static inline void
updateState(State *prevState, State *currState, State newState)
{
    assert(currState != NULL);

    if (prevState != NULL) {
        *prevState = *currState;
    }

    *currState = newState;
}

static Status
getNextCommandLine(char *optArgCmd,
                   FILE *fp,
                   char **commandLineIn,
                   size_t *commandLineLengthIn,
                   CmdMode cmdMode)
{
    Status status;
    State previousState;
    State currentState = Whitespace;
    size_t commandLineLength = 0;
    char *commandLine = NULL;
    size_t cursor = 0;
    bool stopParsing = false;

    status = getNextRawLine(optArgCmd,
                            fp,
                            &commandLine,
                            &commandLineLength,
                            cmdMode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    previousState = currentState = Whitespace;
    do {
        while (cursor < commandLineLength) {
            switch (currentState) {
            case DblQuoteOpened:
                if (commandLine[cursor] == '"') {
                    updateState(&previousState, &currentState, DefaultState);
                } else if (commandLine[cursor] == '\\') {
                    updateState(&previousState, &currentState, EscapeChar);
                }
                break;

            case SingleQuoteOpened:
                if (commandLine[cursor] == '\'') {
                    updateState(&previousState, &currentState, DefaultState);
                }
                break;
            case Whitespace:
                if (commandLine[cursor] == '#') {
                    updateState(&previousState, &currentState, Comment);
                } else if (!strIsWhiteSpace(commandLine[cursor])) {
                    updateState(&previousState, &currentState, DefaultState);
                    continue;
                }
                break;
            case EscapeChar:
                // Do not use updateState here
                updateState(NULL, &currentState, previousState);
                break;

            case DefaultState:
                if (commandLine[cursor] == '"') {
                    updateState(&previousState, &currentState, DblQuoteOpened);
                } else if (commandLine[cursor] == '\'') {
                    updateState(&previousState,
                                &currentState,
                                SingleQuoteOpened);
                } else if (strIsWhiteSpace(commandLine[cursor]) ||
                           commandLine[cursor] == ';') {
                    updateState(&previousState, &currentState, Whitespace);
                } else if (commandLine[cursor] == '\\') {
                    updateState(&previousState, &currentState, EscapeChar);
                }
                break;

            case Comment:
                // A sink for the rest of the line
                break;

            default:
                assert(0);
                break;
            }
            cursor++;
        }

        if (currentState == DblQuoteOpened ||
            currentState == SingleQuoteOpened || currentState == EscapeChar) {
            stopParsing = false;
            if (cmdMode == CmdReadFromOptArgMode) {
                status = StatusNoData;
                goto CommonExit;
            }
            status = getNextRawLine(optArgCmd,
                                    fp,
                                    &commandLine,
                                    &commandLineLength,
                                    cmdMode);
            if (status != StatusOk) {
                if (status == StatusNoData) {
                    status = StatusCliUnclosedQuotes;
                }
                goto CommonExit;
            }
        } else {
            stopParsing = true;
        }
    } while (!stopParsing);

    assert(status == StatusOk);
    *commandLineIn = commandLine;
    *commandLineLengthIn = commandLineLength;

CommonExit:
    if (status != StatusOk) {
        if (commandLine != NULL) {
            memFree(commandLine);
            commandLine = NULL;
            commandLineLength = 0;
        }
    }
    return status;
}

Status
cmdInitParseCmdCursor(CmdParserCursor **cmdParserCursor)
{
    CmdParserCursorStruct *cursor =
        (CmdParserCursorStruct *) memAllocExt(sizeof(CmdParserCursorStruct),
                                              moduleName);
    if (cursor == NULL) {
        return StatusNoMem;
    }

    cursor->init = false;
    cursor->rawCommandLine = NULL;
    cursor->commandLine = NULL;
    cursor->commandCursor = NULL;
    cursor->status = StatusUnknown;

    *cmdParserCursor = cursor;

    return StatusOk;
}

void
cmdFreeParseCmdCursor(CmdParserCursor **cmdParserCursor)
{
    if (*cmdParserCursor != NULL) {
        memFree(*cmdParserCursor);
        *cmdParserCursor = NULL;
    }
}

// A wrapper for cmdParseCmd func in CmdReadFromOptArgMode
Status
cmdParseQuery(char *optArgCmd,
              int *argcIn,
              char **argvIn[],
              CmdParserCursor *cmdParserCursor)
{
    Status status;
    status = cmdParseCmd(optArgCmd,
                         NULL,
                         argcIn,
                         argvIn,
                         cmdParserCursor,
                         CmdReadFromOptArgMode,
                         false);
    return status;
}

Status
cmdParseCmd(char *optArgCmd,
            FILE *fp,
            int *argcIn,
            char **argvIn[],
            CmdParserCursor *cmdParserCursor,
            CmdMode cmdMode,
            bool addHistory)
{
    Status status = StatusOk;
    size_t commandLineLength, rawCommandLineLength;
    CmdParserCursorStruct *cursor;

    cursor = cmdParserCursor;
    if (cursor == NULL) {
        return StatusNoData;
    }

    if (cursor->status == StatusOk || cursor->init == false) {
        if (cmdParserCursor->init && cmdMode == CmdReadFromOptArgMode) {
            return StatusNoData;
        }
        status = getNextCommandLine(optArgCmd,
                                    fp,
                                    &cursor->rawCommandLine,
                                    &rawCommandLineLength,
                                    cmdMode);
        cmdParserCursor->init = true;

        cursor->status = status;
        if (status != StatusOk) {
            goto CommonExit;
        }

        cursor->commandLine = strTrim(cursor->rawCommandLine);
        cursor->commandCursor = NULL;
    }

    assert(cursor->status == StatusMore || cursor->status == StatusOk);

    commandLineLength = strlen(cursor->commandLine);
    if (commandLineLength > 0) {
        // this is where we would add history, if we had a command line
        status = getNextCommand(cursor->commandLine,
                                &cursor->commandCursor,
                                argcIn,
                                argvIn);
        assert(status == StatusOk || status == StatusMore ||
               status == StatusInval);
        if (status != StatusMore) {
            memFree(cursor->rawCommandLine);
            cursor->rawCommandLine = NULL;
        }
        cursor->status = status;
    } else {
        *argcIn = 0;
        *argvIn = NULL;
    }

CommonExit:
    if (status != StatusOk && status != StatusMore) {
        if (cursor->rawCommandLine != NULL) {
            memFree(cursor->rawCommandLine);
            cursor->rawCommandLine = NULL;
        }
    }

    return status;
}
