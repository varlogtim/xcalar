// Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// Session.c   CLI module to support the session command
//
// Supported functions:
//   new             Create a new session
//   new with fork   Create a new session based on the existing session
//   rename          Change the session name, session ID is unchanged
//   switch          Make an inactive session the active session
//   delete          Delete a session or sessions
//   list            List the user's sessions
//   inact           Force a session to become inactive
//   perist          Write one or more sessions' data to disk
//
// General note: The input session names allowed by some functions when
//               they appear to be unnecessary exist due to the needs of
//               the Xcalar Insight UI.  The UI attempts to never expose
//               or track the session ID; only the name which corresponds
//               to the workbook name.

#include <cstdlib>
#include <stdio.h>
#include <unistd.h>  // getlogin, getpid
#include <pwd.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "util/MemTrack.h"  // Xcalar malloc and free replacements

//  Help information for the session command
void
cliSessionHelp(int argc, char *argv[])
{
    printf("Usage:\n");
    printf("WARNING: Use of this tool is deprecated!!!!!\n\n");
    printf("  session --new --name <\"session name\">");
    printf(" [--fork | --forkfrom <\"session name\">]\n");
    printf("  session --list  [pattern]\n");
    printf("  session --rename <\"new session name\">");
    printf(" [<\"old session name\">]\n");
    printf("  session --switch --id <sessionId>\n");
    printf("  session --switch --name <\"session name\">\n");
    printf("  session --delete --id <sessionId>\n");
    printf("  session --delete --name <\"session name\">");
    printf(" |  <\"session name pattern\">\n");
    printf("  session --inact  --id <sessionId>\n");
    printf("  session --inact  --name <\"session name\">\n");
    printf("  session --persist --id <sessionId>\n");
    printf("  session --persist --name <\"session name\">\n");
}

// Print the output from session --list or other commands that return
// a list of sessions as output

void
printListOutput(bool prettyPrint, XcalarApiSessionListOutput *sessionListOutput)
{
    unsigned ii;
    NodeId nodeId;

    if (prettyPrint) {
        printf("\n");
        printf("%-12s%-20s%-16s%-6s%-48s\n",
               "State",
               "ID",
               "Info",
               "Node",
               "Name");
        printf(
            "=================================================="
            "==================================================\n");
    }

    for (ii = 0; ii < sessionListOutput->numSessions; ii++) {
        if (sessionListOutput->sessions[ii].activeNode == NodeIdInvalid) {
            nodeId = -1;
        } else {
            nodeId = sessionListOutput->sessions[ii].activeNode;
        }
        printf("%-12s%-20lX%-16s%-6d%-48s\n",
               sessionListOutput->sessions[ii].state,
               sessionListOutput->sessions[ii].sessionId,
               sessionListOutput->sessions[ii].info,
               nodeId,
               sessionListOutput->sessions[ii].name);
        //    if (session name > field width) {
        //        print in groups of up to slightly less than 80 until done
        //        printf("    %76\n", substring() );
        //    }
    }

    if (prettyPrint) {
        printf(
            "=================================================="
            "==================================================\n");
        printf("%u session(s)\n", ii);
    }
}

// Create a new session

static Status
newSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    StatusCode *statusOutput = NULL;
    XcalarWorkItem *workItem = NULL;
    char sessionName[256] = "\0";
    size_t sessionNameLength = 0;
    char forkedSessionName[256] = "\0";
    size_t forkedSessionNameLength = 0;
    bool forkArg = false;
    bool nameArg = false;
    bool newArg = false;
    unsigned ii;

    XcalarApiSessionNewOutput *sessionNewOutput = NULL;

    // Parse the input, verify all required parameters were passed
    // --new, --name, --fork and --forkfrom can appear in any order.
    // (It is not expected that --fork and --forkfrom will both be requested,
    //  but if they are --forkfrom will be honored.)

    for (ii = 1; ii < (unsigned) argc; ii++) {
        if (strcasecmp(argv[ii], "--new") == 0) {
            newArg = true;
            continue;
        }

        if (strncasecmp(argv[ii], "--fork", 6) == 0) {
            forkArg = true;
            if (strncasecmp(argv[ii], "--forkfrom", 10) == 0) {
                if (ii + 1 < (unsigned) argc && strlen(argv[ii + 1]) > 0 &&
                    strlen(argv[ii + 1]) <= 255) {
                    forkedSessionNameLength = strlen(argv[ii + 1]);
                    strlcpy(forkedSessionName,
                            argv[ii + 1],
                            forkedSessionNameLength + 1);
                    ii++;
                } else {
                    printf("Invalid session name\n");
                    status = StatusCliParseError;
                    break;
                }
            }
            continue;
        }

        if (strcasecmp(argv[ii], "--name") == 0) {
            if (ii + 1 < (unsigned) argc && strlen(argv[ii + 1]) > 0 &&
                strlen(argv[ii + 1]) <= 255) {
                sessionNameLength = strlen(argv[ii + 1]);
                strlcpy(sessionName, argv[ii + 1], sessionNameLength + 1);
                nameArg = true;
                ii++;
            } else {
                printf("Invalid session name\n");
                status = StatusCliParseError;
                break;
            }

        } else {
            printf("Unrecognized parameter\n");
            status = StatusCliParseError;
            break;
        }
    }

    if (newArg == false) {
        printf("--new expected but not found\n");
        status = StatusCliParseError;
    }

    if (nameArg == false) {
        printf("Invalid or missing session name\n");
        status = StatusCliParseError;
    }

    if (sessionNameLength == forkedSessionNameLength &&
        strcmp(sessionName, forkedSessionName) == 0) {
        printf("New session and forked session name must be different\n");
        status = StatusCliParseError;
    }

    if (status == StatusCliParseError) goto NewSessionExit;

    // simple debug code, if needed
    //   printf("session --new syntax passed checking\n");
    //   if (forkArg == true)
    //      printf("--fork was requested\n");

    status = StatusOk;

    workItem = xcalarApiMakeSessionNewWorkItem(sessionName,
                                               forkArg,
                                               forkedSessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto NewSessionExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        printf("session --new request failed\n");
    } else {
        sessionNewOutput = &workItem->output->outputResult.sessionNewOutput;
        statusOutput = &workItem->output->hdr.status;
        if (*statusOutput == StatusOk.code())
            printf("New session created, ID = %lX (%lu)\n",
                   sessionNewOutput->sessionId,
                   sessionNewOutput->sessionId);
        else
            printf("Error: server returned: %s\n",
                   strGetFromStatusCode(*statusOutput));
    }

NewSessionExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}  // End of newSession

// List the user's sessions, optionally with a pattern to match

static Status
listSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    StatusCode *statusOutput = NULL;
    XcalarWorkItem *workItem = NULL;
    char sessionNamePattern[256] = "*";
    size_t sessionNameLength = 1;

    XcalarApiSessionListOutput *sessionListOutput = NULL;

    // Check if a pattern filter was input.  The default
    // is all sessions the user owns, i.e. *

    if (argc > 2) {
        sessionNameLength = strlen(argv[2]);
        if (sessionNameLength > 0 && sessionNameLength < 255) {
            strlcpy(sessionNamePattern, argv[2], sessionNameLength + 1);
        } else {
            printf("Invalid session pattern\n");
            status = StatusCliParseError;
            goto ListSessionExit;
        }
    }

    // get the session information and output it

    workItem = xcalarApiMakeSessionListWorkItem(sessionNamePattern);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto ListSessionExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) goto ListSessionExit;

    statusOutput = &workItem->output->hdr.status;
    if (*statusOutput != StatusOk.code()) {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(*statusOutput));
        goto ListSessionExit;
    }

    sessionListOutput = &workItem->output->outputResult.sessionListOutput;
    assert(sessionListOutput->numSessions >= 1);

    // Print the results...

    printListOutput(prettyPrint, sessionListOutput);

    status = StatusOk;

ListSessionExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}  // End of listSession

// Delete an existing session

static Status
deleteSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    uint64_t sessionId = 0;
    bool inputId = false;
    bool inputName = false;

    char sessionName[256] = "\0";
    size_t sessionNameLength = 0;

    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    // Determine if we are deleting by name or ID

    if (strcasecmp(argv[2], "--id") == 0) {
        inputId = true;
    }

    if (strcasecmp(argv[2], "--name") == 0) {
        inputName = true;
    }

    if (inputId == false && inputName == false) {
        printf("Required parameter missing\n");
        status = StatusCliParseError;
        goto DeleteSessionExit;
    }
    // If --id was specified, verify that a possible session ID was passed.
    // The session ID is the fourth input argument which has already been
    // verified to exist.
    // The session ID is input in hex chars, limited to 16 bytes

    if (inputId) {
        if (strlen(argv[3]) < 1 || strlen(argv[3]) > 16) {
            printf("Invalid session ID\n");
            status = StatusCliParseError;
            goto DeleteSessionExit;
        }

        // Note: strtoul will terminate conversion if an invalid character is
        //       found, treating the input value as if it were fewer characters.
        //       If the leading character is invalid, 0 is returned.

        sessionId = strtoul(argv[3], NULL, 16);
        if (sessionId != 0) {
            //   printf("Input Session ID = %lu\n", sessionId);
        } else {
            printf("Invalid session ID\n");
            status = StatusCliParseError;
            goto DeleteSessionExit;
        }
    } else {
        // We have an individual session name or pattern.
        sessionNameLength = strlen(argv[3]);
        if (sessionNameLength == 0 || sessionNameLength > 255) {
            printf("Session name must be 1 to 255 characters in length\n");
            status = StatusCliParseError;
            goto DeleteSessionExit;
        }
        strlcpy(sessionName, argv[3], sessionNameLength + 1);
    }

    workItem = xcalarApiMakeSessionDeleteWorkItem(sessionName);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto DeleteSessionExit;
    }
    // Due to the way the UI works, the session ID is only used by CLI and
    // without C++ function overloading, it was easier to set the ID here if
    // needed.  If --id was input, the name is a zero length string.  If
    // --name was input the session ID is zero by default.

    workItem->input->sessionDeleteInput.sessionId = sessionId;

    // noCleanup is ignored by delete, but we may as well avoid any confusion
    workItem->input->sessionDeleteInput.noCleanup = false;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        printf("Session delete failed\n");
    } else {
        sessionGenericOutput =
            &workItem->output->outputResult.sessionGenericOutput;
        assert(sessionGenericOutput != NULL);

        if (workItem->output->hdr.status != StatusOk.code()) {
            printf("Error: server returned: %s\n",
                   strGetFromStatusCode(workItem->output->hdr.status));
        }
        printf("%s\n", sessionGenericOutput->errorMessage);
    }

DeleteSessionExit:

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// Force an active session to be marked inactive.

static Status
inactSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    uint64_t sessionId = 0;
    bool inputId = false;
    bool inputName = false;

    char sessionName[256] = "\0";
    size_t sessionNameLength = 0;

    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    // Determine if we are deleting by name or ID

    if (strcasecmp(argv[2], "--id") == 0) {
        inputId = true;
    }

    if (strcasecmp(argv[2], "--name") == 0) {
        inputName = true;
    }

    if (inputId == false && inputName == false) {
        printf("Required parameter missing\n");
        status = StatusCliParseError;
        goto InactSessionExit;
    }
    // If --id was specified, verify that a possible session ID was passed.
    // The session ID is the fourth input argument which has already been
    // verified to exist.
    // The session ID is input in hex chars, limited to 16 bytes

    if (inputId) {
        if (strlen(argv[3]) < 1 || strlen(argv[3]) > 16) {
            printf("Invalid session ID\n");
            status = StatusCliParseError;
            goto InactSessionExit;
        }

        // Note: strtoul will terminate conversion if an invalid character is
        //       found, treating the input value as if it were fewer characters.
        //       If the leading character is invalid, 0 is returned.

        sessionId = strtoul(argv[3], NULL, 16);
        if (sessionId != 0) {
            //   printf("Input Session ID = %lu\n", sessionId);
        } else {
            printf("Invalid session ID\n");
            status = StatusCliParseError;
            goto InactSessionExit;
        }
    } else {
        // We have an individual session name or pattern.
        sessionNameLength = strlen(argv[3]);
        if (sessionNameLength == 0 || sessionNameLength > 255) {
            printf("Session name must be 1 to 255 characters in length\n");
            status = StatusCliParseError;
            goto InactSessionExit;
        }
        strlcpy(sessionName, argv[3], sessionNameLength + 1);
    }

    // Not a typo, the input to inact is the same as delete
    workItem = xcalarApiMakeSessionDeleteWorkItem(sessionName);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto InactSessionExit;
    }
    workItem->api = XcalarApiSessionInact;
    workItem->input->sessionDeleteInput.sessionId = sessionId;

    if (argc > 4) {
        printf("Extraneous parameters ignored\n");
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        printf("Session inact failed\n");
    } else {
        sessionGenericOutput =
            &workItem->output->outputResult.sessionGenericOutput;
        assert(sessionGenericOutput != NULL);
        if (workItem->output->hdr.status != StatusOk.code()) {
            printf("Error: server returned: %s\n",
                   strGetFromStatusCode(workItem->output->hdr.status));
        }
        printf("%s\n", sessionGenericOutput->errorMessage);
    }

InactSessionExit:

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// Write a session's data and its DAG to disk (i.e., persist it)

static Status
persistSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    uint64_t sessionId = 0;
    bool inputId = false;
    bool inputName = false;

    char sessionName[256] = "\0";
    size_t sessionNameLength = 0;

    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionListOutput *sessionListOutput = NULL;

    // Determine if we are deleting by name or ID

    if (strcasecmp(argv[2], "--id") == 0) {
        inputId = true;
    }

    if (strcasecmp(argv[2], "--name") == 0) {
        inputName = true;
    }

    if (inputId == false && inputName == false) {
        printf("Required parameter missing\n");
        status = StatusCliParseError;
        goto PersistSessionExit;
    }
    // If --id was specified, verify that a possible session ID was passed.
    // The session ID is the fourth input argument which has already been
    // verified to exist.
    // The session ID is input in hex chars, limited to 16 bytes

    if (inputId) {
        if (strlen(argv[3]) < 1 || strlen(argv[3]) > 16) {
            printf("Invalid session ID\n");
            status = StatusCliParseError;
            goto PersistSessionExit;
        }

        // Note: strtoul will terminate conversion if an invalid character is
        //       found, treating the input value as if it were fewer characters.
        //       If the leading character is invalid, 0 is returned.

        sessionId = strtoul(argv[3], NULL, 16);
        if (sessionId != 0) {
            // printf("Input Session ID = %lu\n", sessionId);
        } else {
            printf("Invalid session ID\n");
            status = StatusCliParseError;
            goto PersistSessionExit;
        }
    } else {
        // We have an individual session name or pattern.
        sessionNameLength = strlen(argv[3]);
        if (sessionNameLength == 0 || sessionNameLength > 255) {
            printf("Session name must be 1 to 255 characters in length\n");
            status = StatusCliParseError;
            goto PersistSessionExit;
        }
        strlcpy(sessionName, argv[3], sessionNameLength + 1);
    }

    // Create a work item for this request
    workItem = xcalarApiMakeSessionPersistWorkItem(sessionName);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto PersistSessionExit;
    }
    workItem->input->sessionDeleteInput.sessionId = sessionId;
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk.code()) {
        printf("Session persist failed\n");
        if (status == StatusOk) {
            status.fromStatusCode(workItem->output->hdr.status);
            printf("Server returned: %s\n", strGetFromStatus(status));
        }
        goto PersistSessionExit;
    } else {
        sessionListOutput = &workItem->output->outputResult.sessionListOutput;
        assert(sessionListOutput != NULL);
        // Print the results
        printListOutput(prettyPrint, sessionListOutput);
    }

PersistSessionExit:

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// Switch the active session from the current one
// to an existing inactive session

static Status
switchSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    size_t newSessionLength = 0;
    char newSessionName[256] = "\0";
    bool inputId = false;
    bool inputName = false;

    StatusCode *statusOutput = NULL;
    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    // Check to see if the target is a session ID or name

    if (strcasecmp(argv[2], "--id") == 0) {
        assert(0 && "Use of --id is deprecated");
        inputId = true;
    }

    if (strcasecmp(argv[2], "--name") == 0) {
        inputName = true;
    }

    if (inputId == false && inputName == false) {
        printf("Required parameter missing\n");
        status = StatusCliParseError;
        goto SwitchSessionExit;
    }

    // Session ID was input ...
    // Verify that a possible session ID was passed.  The session ID is the
    // fourth input argument which was already checked for existence.  The
    // the session ID is input in char hex, limited to 16 bytes.

    if (inputId) {
        // Deprecated
    } else {
        // A session name was input.
        newSessionLength = strlen(argv[3]);
        if (newSessionLength < 1 || newSessionLength > 255) {
            printf(
                "Session name must be between 1 and 255 characters "
                "in length\n");
            status = StatusCliParseError;
            goto SwitchSessionExit;
        }
        strlcpy(newSessionName, argv[3], newSessionLength + 1);
    }

    workItem = xcalarApiMakeSessionActivateWorkItem(newSessionName);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto SwitchSessionExit;
    }

    if (argc > 4) {
        printf("Extraneous parameters ignored\n");
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        printf("Session activate failed\n");
    } else {
        statusOutput = &workItem->output->hdr.status;
        sessionGenericOutput =
            &workItem->output->outputResult.sessionGenericOutput;
        assert(sessionGenericOutput != NULL);

        if (*statusOutput == StatusOk.code()) {
            // XXX feedback is temporary for testing on success
            printf("Session successfully switched\n");
        } else {
            printf("Error: server returned: %s\n",
                   strGetFromStatusCode(*statusOutput));
            if (sessionGenericOutput != NULL &&
                strlen(sessionGenericOutput->errorMessage) > 0) {
                printf("Additional info: %s\n",
                       sessionGenericOutput->errorMessage);
            }
        }
    }

SwitchSessionExit:

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// Rename a session.  The default is to rename the current session,
// which is why the old session name is optional.  If the old session
// name is supplied (and can be found), it will be renamed.

static Status
renameSession(int argc, char *argv[], bool prettyPrint, bool interactive)
{
    Status status = StatusUnknown;
    StatusCode *statusOutput = NULL;

    char newSessionName[256] = "\0";
    size_t newSessionNameLength = 0;
    char oldSessionName[256] = "\0";
    size_t oldSessionNameLength = 0;

    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    newSessionNameLength = strlen(argv[2]);

    // If someone manages to input a zero length string, we'll ignore it

    if (argc == 4) {
        oldSessionNameLength = strlen(argv[3]);
        if (oldSessionNameLength > 0 && oldSessionNameLength < 255) {
            strlcpy(oldSessionName, argv[3], oldSessionNameLength + 1);
        }
    }

    if (newSessionNameLength < 1 || newSessionNameLength > 255 ||
        oldSessionNameLength > 255) {  // old length can be zero
        printf("Session name must be 1 to 255 characters in length\n");
        status = StatusCliParseError;
        goto RenameSessionExit;
    }

    strlcpy(newSessionName, argv[2], newSessionNameLength + 1);

    if (newSessionNameLength == oldSessionNameLength &&
        strcmp(oldSessionName, newSessionName) == 0) {
        printf("New session name must be different than the old ");
        printf("session name. \n");
        status = StatusCliParseError;
        goto RenameSessionExit;
    }

    workItem =
        xcalarApiMakeSessionRenameWorkItem(newSessionName, oldSessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto RenameSessionExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        printf("Session --rename failed\n");
    } else {
        statusOutput = &workItem->output->hdr.status;
        sessionGenericOutput =
            &workItem->output->outputResult.sessionGenericOutput;

        if (sessionGenericOutput != NULL && *statusOutput == StatusOk.code()) {
            printf("Session successfully renamed:\n");
        } else {
            printf("Error: server returned: %s\n",
                   strGetFromStatusCode(*statusOutput));
            if (sessionGenericOutput != NULL &&
                (strlen(sessionGenericOutput->errorMessage) > 0)) {
                printf("Additional info: %s\n",
                       sessionGenericOutput->errorMessage);
            }
        }
    }

RenameSessionExit:

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// Main session routine
//   Parse input args and invoke routine to do the work

void
cliSessionMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    Status status = StatusUnknown;

    // If too many or not enough args passed, print help and exit
    //  (session --new with --fork has 5 args)

    if (argc < 2 || argc > 6) {
        status = StatusCliParseError;
        cliSessionHelp(argc, argv);
        goto MainExit;
    }

    // Determine what needs to be done (this was easier to read until
    // the cstyle checker complained...)

    if (strcasecmp(argv[1], "--new") == 0 ||
        strcasecmp(argv[1], "--name") == 0 ||
        strncasecmp(argv[1], "--fork", 6) == 0) {
        status = newSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--list") == 0 && (argc == 2 || argc == 3)) {
        status = listSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--delete") == 0 && argc == 4) {
        status = deleteSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--rename") == 0 &&
               (argc == 3 || argc == 4)) {
        status = renameSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--switch") == 0 &&
               (argc == 4 || argc == 5)) {
        status = switchSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--inact") == 0 &&
               (argc == 4 || argc == 5)) {
        status = inactSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--persist") == 0 && argc == 4) {
        status = persistSession(argc, argv, prettyPrint, interactive);
    } else if (strcasecmp(argv[1], "--help") == 0) {
        cliSessionHelp(argc, argv);
        status = StatusOk;
    } else {
        printf("Error: Unrecognized or missing parameter(s)\n");
        status = StatusCliParseError;
    }

    // Parse error may be set in the initial checking above or in
    // one of the functions of the SESSION command

    if (status == StatusCliParseError) cliSessionHelp(argc, argv);

MainExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
