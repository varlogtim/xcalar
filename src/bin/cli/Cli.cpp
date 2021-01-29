// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
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
#include <pwd.h>

#include "GetOpt.h"
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
#include "usr/Users.h"
#include "common/Version.h"
#include "util/Archive.h"
#include "sys/XLog.h"
#include "common/InitTeardown.h"
#include "StrlFunc.h"
#include "libapis/LibApisSend.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "xcalar/compute/localtypes/Version.pb.h"
#include "xcalar/compute/localtypes/Dataflow.pb.h"

// ----- Function prototypes for builtins -----
static void cliHelpMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
static void cliHelpHelp(int argc, char *argv[]);

static void cliQuitMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
static void cliQuitHelp(int argc, char *argv[]);

static void cliVersionMain(int argc,
                           char *argv[],
                           XcalarWorkItem *workItemIn,
                           bool prettyPrint,
                           bool interactive);
static void cliVersionHelp(int argc, char *argv[]);

static void cliConnectMain(int argc,
                           char *argv[],
                           XcalarWorkItem *workItemIn,
                           bool prettyPrint,
                           bool interactive);
static void cliConnectHelp(int argc, char *argv[]);

static void cliEchoMain(int argc,
                        char *argv[],
                        XcalarWorkItem *workItemIn,
                        bool prettyPrint,
                        bool interactive);
static void cliEchoHelp(int argc, char *argv[]);

static void cliWhoamIMain(int argc,
                          char *argv[],
                          XcalarWorkItem *workItemIn,
                          bool prettyPrint,
                          bool interactive);
static void cliWhoamIHelp(int argc, char *argv[]);

// ----- Global variables declaration -----
char cliDestIpBuf[255];

static CmdMode cmdMode = CmdInteractiveMode;
static bool CliExitCli = false;
#ifdef MEMORY_TRACKING  // This avoids an 'unused-variable'
static const char *moduleName = "cli";
#endif  // MEMORY_TRACKING

// Install your program here
const ExecCommandMapping builtInCommandMappings[] = {
    // Builtins - functions that touches the CLI env vars go here
    {.commandString = "help",
     .commandDesc = "Show list of commands",
     .main = cliHelpMain,
     .usage = cliHelpHelp},
    {.commandString = "quit",
     .commandDesc = "Exit Xcalar shell",
     .main = cliQuitMain,
     .usage = cliQuitHelp},
    {.commandString = "version",
     .commandDesc = "Get version of CLI and backend",
     .main = cliVersionMain,
     .usage = cliVersionHelp},
    {.commandString = "connect",
     .commandDesc = "Reconnect to a different node",
     .main = cliConnectMain,
     .usage = cliConnectHelp},
    {
        .commandString = "echo",
        .commandDesc = "Echoes whatever you type",
        .main = cliEchoMain,
        .usage = cliEchoHelp,
    },
    {
        .commandString = "whoami",
        .commandDesc = "Prints the username associated with this session",
        .main = cliWhoamIMain,
        .usage = cliWhoamIHelp,
    },

};

const unsigned CliNumBuiltInCommandMappings =
    (unsigned) ArrayLen(builtInCommandMappings);
unsigned CliNumCommandMappings;
ExecCommandMapping *commandMappings;

static void
initCommandMappings(void)
{
    CliNumCommandMappings =
        CliNumBuiltInCommandMappings + execNumCoreCommandMappings;

    commandMappings = (ExecCommandMapping *) memAlloc(
        sizeof(ExecCommandMapping) * CliNumCommandMappings);

    memcpy(commandMappings,
           builtInCommandMappings,
           sizeof(ExecCommandMapping) * CliNumBuiltInCommandMappings);
    memcpy(&commandMappings[CliNumBuiltInCommandMappings],
           execCoreCommandMappings,
           sizeof(ExecCommandMapping) * execNumCoreCommandMappings);
}

// ----- Start of CLI Builtins -----
// whoami
static void
cliWhoamIMain(int argc,
              char *argv[],
              XcalarWorkItem *workItemIn,
              bool prettyPrint,
              bool interactive)
{
    printf("%s (%d %s)\n",
           cliUsername,
           cliUserIdUnique,
           (cliUserIdUniqueUserSpecified) ? "user-provided" : "default");
}

static void
cliWhoamIHelp(int argc, char *argv[])
{
    printf("To figure out who you are, just ask\n");
}

// connect
static void
cliConnectMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    char tmpIpBuf[sizeof(cliDestIpBuf)];
    uint16_t tmpPort;
    size_t ret;

    Status status = StatusUnknown;
    xcalar::compute::localtypes::Version::GetVersionResponse resp;
    ProtoRequestMsg requestMsg;
    ProtoResponseMsg responseMsg;
    google::protobuf::Empty empty;
    bool success;

    ServiceSocket socket;

    if (argc < 2 || argc > 3) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    ret = strlcpy(tmpIpBuf, argv[1], sizeof(tmpIpBuf));
    if (ret >= sizeof(tmpIpBuf)) {
        fprintf(stderr, "Error: Ip address too long\n");
        status = StatusOverflow;
        goto CommonExit;
    }

    tmpPort = (argc > 2) ? (uint16_t) atoi(argv[2]) : cliDestPort;

    // Attempt to connect
    try {
        requestMsg.mutable_servic()->set_servicename("Version");
        requestMsg.mutable_servic()->set_methodname("GetVersion");
        requestMsg.set_requestid(0);
        requestMsg.set_target(ProtoMsgTargetService);
        requestMsg.mutable_servic()->mutable_body()->PackFrom(empty);
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }
    status = socket.init();

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = socket.sendRequest(&requestMsg, &responseMsg);
    if (status != StatusOk) {
        goto CommonExit;
    }

    success = responseMsg.servic().body().UnpackTo(&resp);
    if (!success) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

    strlcpy(cliDestIpBuf, tmpIpBuf, sizeof(cliDestIpBuf));
    cliDestIp = cliDestIpBuf;
    cliDestPort = tmpPort;

    fprintf(stdout,
            "Connected to %s:%d successfully\n",
            cliDestIp,
            cliDestPort);
CommonExit:

    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
        if (status == StatusCliParseError) {
            cliConnectHelp(argc, argv);
        }
    }
}

static void
cliConnectHelp(int argc, char *argv[])
{
    printf("Usage: %s <destination hostname / IP> <destination port>\n",
           argv[0]);
    printf("You are currently connected to %s:%d\n", cliDestIp, cliDestPort);
}

// help
static void
cliHelpHelp(int argc, char *argv[])
{
    printf("If you need help with help, you definitely need help.\n");
}

static void
cliHelpMain(int argc,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    unsigned ii;

    if (argc > 1) {
        for (ii = 0; ii < CliNumCommandMappings; ii++) {
            if (strcmp(argv[1], commandMappings[ii].commandString) == 0) {
                if (commandMappings[ii].usage != NULL) {
                    commandMappings[ii].usage(argc - 1, &argv[1]);
                } else {
                    printf("No help found for %s\n",
                           commandMappings[ii].commandString);
                }
                return;
            }
        }

        printf("Unknown command %s\n", argv[1]);
    }
    printf("List of commands available:\n");
    for (ii = 0; ii < CliNumCommandMappings; ii++) {
        printf("  %-12s - %s\n",
               commandMappings[ii].commandString,
               commandMappings[ii].commandDesc);
    }
}

// quit
static void
cliQuitHelp(int argc, char *argv[])
{
    printf("It is what it is. %s exits the shell\n", argv[0]);
}

static void
cliQuitMain(int arg64,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    CliExitCli = true;
}

static void
cliVersionMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    Status status = StatusUnknown;

    xcalar::compute::localtypes::Version::GetVersionResponse resp;
    ProtoRequestMsg requestMsg;
    ProtoResponseMsg responseMsg;
    google::protobuf::Empty empty;

    ServiceSocket socket;
    bool success = false;

    printf("CLI Version: %s\n", versionGetFullStr());
    printf("CLI Xcalar API Version Signature: %s (%u)\n",
           versionGetApiStr(versionGetApiSig()),
           versionGetApiSig());
    printf("CLI xcrpc Version Signature: %s (%u)\n",
           versionGetXcrpcStr(versionGetXcrpcSig()),
           versionGetXcrpcSig());

    printf("\nAttempting to get version from backend\n");
    printf("----------------------------------------\n");

    // TODO will change to xc2 version
    try {
        requestMsg.mutable_servic()->set_servicename("Version");
        requestMsg.mutable_servic()->set_methodname("GetVersion");
        requestMsg.set_requestid(0);
        requestMsg.set_target(ProtoMsgTargetService);
        requestMsg.mutable_servic()->mutable_body()->PackFrom(empty);
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }
    status = socket.init();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = socket.sendRequest(&requestMsg, &responseMsg);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (responseMsg.status() != StatusOk.code()) {
        status.fromStatusCode((StatusCode) responseMsg.status());
        goto CommonExit;
    }

    success = responseMsg.servic().body().UnpackTo(&resp);
    if (!success) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

CommonExit:
    workItemIn->status = status.code();
    if (status == StatusOk) {
        printf("Backend Version: %s\n", resp.version().c_str());
        printf("Backend Xcalar API Version Signature: %s (%u)\n",
               resp.thrift_version_signature_full().c_str(),
               resp.thrift_version_signature_short());
        printf("Backend xcrpc Version Signature: %s (%u)\n",
               resp.xcrpc_version_signature_full().c_str(),
               resp.xcrpc_version_signature_short());
    } else {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}

static void
cliVersionHelp(int argc, char *argv[])
{
    printf("Usage: %s\n", argv[0]);
    printf("This gets both the CLI version and the backend version\n");
}

static void
cliEchoMain(int argc,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    if (argc > 1) {
        printf("%s\n", argv[1]);
    }
}

static void
cliEchoHelp(int argc, char *argv[])
{
    printf(
        "Echoes whatever you type. E.g. %s \"Hello World\" outputs"
        " \"Hello World\"\n",
        argv[0]);
}

// Each string the generator function returns as a match must be
// allocated with malloc. See http://web.mit.edu/gnu/doc/html/rlman_2.html
char *
cliCompletionMatches(const char *text, int state)
{
    static unsigned listIndex;
    static size_t length;
    size_t commandStringLength;
    char *matchedString;
    const char *commandString;

    if (state == 0) {
        listIndex = 0;
        length = strlen(text);
    }

    while (listIndex < CliNumCommandMappings) {
        commandString = commandMappings[listIndex].commandString;
        listIndex++;
        if (strncmp(commandString, text, length) == 0) {
            commandStringLength = strlen(commandString) + 1;
            // @SymbolCheckIgnore
            matchedString = (char *) malloc(commandStringLength);
            if (matchedString) {
                strlcpy(matchedString, commandString, commandStringLength);
            }
            return matchedString;
        }
    }

    return NULL;
}

static Status
issueCliCmd(int argc, char *argv[], bool prettyPrint)
{
    unsigned ii;
    QueryParserEnum qp;
    XcalarWorkItem *workItemIn = NULL;
    Status status = StatusCliUnknownCmd;

    assert(argc > 0);
    assert(argv != NULL);

    for (ii = 0; ii < CliNumCommandMappings; ii++) {
        if (strcmp(argv[0], commandMappings[ii].commandString) == 0) {
            assert(commandMappings[ii].main != NULL);
            qp = strToQueryParserEnum(commandMappings[ii].commandString);
            if ((int) qp < QueryParserEnumLen) {
                QueryCmdParser *cmdParser;
                cmdParser = QueryParser::get()->getCmdParser(qp);
                if (cmdParser == NULL) {
                    break;
                }

                status = cmdParser->parse(argc, argv, &workItemIn);
                if (status != StatusOk) {
                    assert(workItemIn == NULL);
                    break;
                }
            } else {
                if (strcmp(argv[0], "version") == 0) {
                    workItemIn = xcalarApiMakeGetVersionWorkItem();
                    if (workItemIn == NULL) {
                        status = StatusNoMem;
                        break;
                    }
                }
            }

            if (workItemIn) {
                // Mark this as coming from legacy/frozen infrastructure
                workItemIn->legacyClient = true;
            }

            commandMappings[ii].main(argc,
                                     argv,
                                     workItemIn,
                                     prettyPrint,
                                     cmdMode == CmdInteractiveMode);

            if (workItemIn != NULL) {
                if (workItemIn->output != NULL) {
                    status.fromStatusCode(workItemIn->output->hdr.status);
                } else {
                    status.fromStatusCode(workItemIn->status);
                }
                xcalarApiFreeWorkItem(workItemIn);
            } else {
                status = StatusUnknown;
                // unknown status since commandMappings[ii].main() is void and
                // workItemIn is NULL so no way to get status of its execution.
            }
            return status;
        }
    }

    fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    if (status == StatusCliParseError) {
        commandMappings[ii].usage(argc, argv);
    }
    return status;
}

static void
printUsage(void)
{
    fprintf(stderr,
            "Usage:\n\txccli [--command <commandToRun>] "
            "[--noprettyprint]\n\t"
            "            [--ip <hostname>] [--port <port>] "
            "[--username <username>] [--userIdUnique <cookie>]\n");
}

int
main(int argc, char *argv[])
{
    const char *fileName = NULL;
    int cliArgc;
    char **cliArgv = NULL;
    char *optArgCmd = NULL;
    FILE *fp = NULL;
    bool prettyPrint = true;
    bool usernameSpecified = false;
    CmdParserCursor *cmdParserCursor;
    Status status;
    size_t ret;
    bool cliCmdFailed = false;

    status = InitTeardown::init(InitLevel::Cli,
                                SyslogFacilityCli,
                                NULL,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                Config::ConfigUnspecifiedNodeId,
                                1 /* Num active */,
                                Config::ConfigUnspecifiedNumActiveOnPhysical,
                                BufferCacheMgr::TypeXccli);
    if (status != StatusOk) {
        fprintf(stderr, "Could not initialize common libraries\n");
        goto CommonExit;
    }

    initCommandMappings();

    if (argc > 1) {
        int flag;
        struct option longOptions[] = {
            {"command", required_argument, 0, 'c'},
            {"noprettyprint", no_argument, 0, 'p'},
            {"ip", required_argument, 0, 'i'},
            {"port", required_argument, 0, 'o'},
            {"username", required_argument, 0, 'u'},
            {"userIdUnique", required_argument, 0, 'k'},
            {0, 0, 0, 0},
        };

        while (
            (flag = getopt_long(argc, argv, "c:i:o:u:k:", longOptions, NULL)) !=
            -1) {
            switch (flag) {
            case 'c':
                cmdMode = CmdReadFromOptArgMode;
                optArgCmd = optarg;
                break;
            case 'p':
                prettyPrint = false;
                break;
            case 'i':
                ret = strlcpy(cliDestIpBuf, optarg, sizeof(cliDestIpBuf));
                if (ret >= sizeof(cliDestIpBuf)) {
                    fprintf(stderr, "Ip address too long\n");
                    // @SymbolCheckIgnore
                    exit(1);
                }
                cliDestIp = cliDestIpBuf;
                break;
            case 'o':
                cliDestPort = (uint16_t) atoi(optarg);
                break;
            case 'u':
                usernameSpecified = true;
                ret = strlcpy(cliUsername, optarg, sizeof(cliUsername));
                if (ret >= sizeof(cliUsername)) {
                    fprintf(stderr, "Username too long\n");
                    // @SymbolCheckIgnore
                    exit(1);
                }
                break;
            case 'k':
                cliUserIdUniqueUserSpecified = true;
                cliUserIdUnique = atoi(optarg);
                if (cliUserIdUnique == SessionMgr::InvalidUserCookie) {
                    fprintf(stderr, "Invalid user cookie specified\n");
                    // @SymbolCheckIgnore
                    exit(1);
                }
                break;
            default:
                printUsage();
                // @SymbolCheckIgnore
                exit(1);
            }
        }

        if (optind < argc && cmdMode != CmdReadFromOptArgMode) {
            cmdMode = CmdReadFromFileMode;
            fileName = argv[optind];
            // @SymbolCheckIgnore
            fp = fopen(fileName, "r");
            if (fp == NULL) {
                fprintf(stderr, "Could not open %s\n", fileName);
                // @SymbolCheckIgnore
                exit(1);
            }
        }
    }

    if (!cliUserIdUniqueUserSpecified) {
        cliUserIdUnique = getpid();
    }

    if (!usernameSpecified) {
        int getloginRc = 0;
        uid_t uid;
        char *returnedPwd = NULL;
        int64_t returnedPwdSize;
        struct passwd pwd;
        struct passwd *returnedPwdStruct;

        uid = getuid();
        returnedPwdSize = sysconf(_SC_GETPW_R_SIZE_MAX);
        if (returnedPwdSize < 0) {   /* Value was indeterminate */
            returnedPwdSize = 16384; /* Should be more than enough */
        }

        returnedPwd =
            (char *) memAllocExt((size_t) returnedPwdSize, moduleName);
        if (returnedPwd == NULL) {
            fprintf(stderr, "Could not malloc for returnedPwd\n");
            // @SymbolCheckIgnore
            exit(1);
        }

        getloginRc = getpwuid_r(uid,
                                &pwd,
                                returnedPwd,
                                (size_t) returnedPwdSize,
                                &returnedPwdStruct);
        if (getloginRc != 0 || returnedPwdStruct == NULL) {
            fprintf(stderr,
                    "Error: %s\n",
                    strGetFromStatus(sysErrnoToStatus(getloginRc)));
            // @SymbolCheckIgnore
            exit(1);
        }
        ret = strlcpy(cliUsername, pwd.pw_name, sizeof(cliUsername));
        if (ret >= sizeof(cliUsername)) {
            fprintf(stderr, "Username too long\n");
            // @SymbolCheckIgnore
            exit(1);
        }

        assert(returnedPwd != NULL);
        memFree(returnedPwd);
        returnedPwd = NULL;
    }

    if (cmdMode == CmdInteractiveMode) {
        prettyPrint = true;
        printf(
            "Xccli has been decomissioned. Please use Xcalar Design "
            "instead\n\n");
        /*
                printf("____  ___           .__  .__ \n");
                printf("\\   \\/  /____  ____ |  | |__|\n");
                printf(" \\     // ___\\/ ___\\|  | |  |\n");
                printf(" /     \\  \\__\\  \\___|  |_|  |\n");
                printf("/___/\\  \\___  >___  >____/__|\n");
                printf("      \\_/   \\/    \\/         \n");
                printf("Welcome, %s! Type \"help\" to see list of "
                       "commands.\n\n", cliUsername);
        */
        CliExitCli = true;
    }

    status = cmdInitParseCmdCursor(&cmdParserCursor);
    if (status != StatusOk) {
        printf("Cli error: %s\n", strGetFromStatus(status));
        CliExitCli = true;
    }

    while (!CliExitCli) {
        status = cmdParseCmd(optArgCmd,
                             fp,
                             &cliArgc,
                             &cliArgv,
                             cmdParserCursor,
                             cmdMode,
                             true);
        if (status == StatusNoData) {
            CliExitCli = true;
            break;
        } else if (status != StatusOk && status != StatusMore) {
            printf("Cli error: %s\n", strGetFromStatus(status));
            CliExitCli = true;
            break;
        }

        if (cliArgc == 0) {
            assert(cliArgv == NULL);
        } else {
            assert(cliArgv != NULL);

            optind = 0;
            status = issueCliCmd(cliArgc, cliArgv, prettyPrint);
            for (int ii = 0; ii < cliArgc; ii++) {
                assert(cliArgv[ii] != NULL);
                memFree(cliArgv[ii]);
                cliArgv[ii] = NULL;
            }
            memFree(cliArgv);
            cliArgv = NULL;
            if (status != StatusOk && status != StatusUnknown) {
                cliCmdFailed = true;
                // continue to next command but report that at least one failed
            }
        }
    }
    cmdFreeParseCmdCursor(&cmdParserCursor);
    memFree(commandMappings);

    /*
        if (cmdMode == CmdInteractiveMode) {
            printf("\n\nGoodbye!\n\n");
        }
    */

    if (fp != NULL) {
        assert(cmdMode == CmdReadFromFileMode);
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }
CommonExit:
    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    if (cliCmdFailed) {  // return non-zero if at least one cli command failed
        return 1;
    } else {
        return 0;
    }
}
