// Copyright 2018-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>

#include "config/Config.h"
#include "common/InitTeardown.h"
#include "log/Log.h"
#include "queryparser/QueryParser.h"
#include "dag/DagLib.h"
#include "common/Version.h"
#include "LibDagConstants.h"
#include "runtime/Runtime.h"
#include "Primitives.h"
#include "util/MemTrack.h"
#include "msg/Xid.h"
#include "kvstore/KvStore.h"
#include "usr/Users.h"
#include "session/Sessions.h"
#include "log/Log.h"
#include "util/FileUtils.h"
#include "app/AppMgr.h"
#include "export/DataTarget.h"
#include "udf/UserDefinedFunction.h"

#include "XcUpgradeTool.h"

// This tool upgrades Chronos to Dionysus:
// * Query graphs are deserialized and converted to dataflow 2.0 version.
// * Batch dataflows are uploaded as shared dataflows.
//
// The uses of this tool must ensure that a copy of XLRROOT is made before doing
// the upgrade.

static constexpr unsigned MaxCmdLength = 2048;
static constexpr const char *QgraphPrefix = "Xcalar.qgraph.";
static constexpr const char *SessionPrefix = "Xcalar.session.";
static constexpr const char *DataflowPrefix = "Xcalar.Retina.";
static constexpr const char *MetaPostfix = "-meta";
static constexpr const char *ZeroPostfix = "-0";
static constexpr const char *OnePostfix = "-1";
static constexpr const char *BackupDir = "backupCP1";
static constexpr const char *SessionDir = "sessions";
static constexpr const char *DataflowDir = "dataflows";
static constexpr const char *KvsDir = "kvs";
static constexpr const char *WorkbookDir = "workbooks";
static constexpr const char *DataflowUserName = ".xcalar.published.df";
static constexpr const char *TarGzPostfix = ".tar.gz";

// Chatty outout
static bool verbose = false;

// Hash table to keep track of users with bad sessions and the session
// names that are bad.
UserSessionHashTable badUserHt;
UserSessionHashTable upgradedUserHt;
UserSessionHashTable badDataflowHt;
UserSessionHashTable upgradedDataflowHt;

void
printUpgradeUsage(const char *name)
{
    fprintf(stdout,
            "\nUsage: %s\n"
            "\t--cfg <absolute path to config file>\n"
            "\t[--verbose]\n"
            "\t[--force]\n",
            name);
}

Status
parseUpgradeOptions(int argc, char *argv[], UpgradeToolOptions *options)
{
    int optionIndex = 0;
    int flag;
    bool cfgSet = false;

    struct option longOptions[] = {
        {"cfg", required_argument, 0, 'c'},
        {"verbose", no_argument, 0, 'v'},
        {"force", no_argument, 0, 'f'},
        {0, 0, 0, 0},
    };

    options->cfgFile[0] = '\0';
    options->force = false;
    verbose = false;

    while (
        (flag =
             getopt_long(argc, argv, "c:pvj:Jf", longOptions, &optionIndex)) !=
        -1) {
        switch (flag) {
        case 'c':
            strlcpy(options->cfgFile, optarg, sizeof(options->cfgFile));
            cfgSet = true;
            break;
        case 'v':
            verbose = true;
            break;
        case 'f':
            options->force = true;
            break;
        default:
            fprintf(stderr, "Unknown arg %c\n", flag);
            printUpgradeUsage(argv[0]);
            return StatusInval;
            break;
        }
    }

    if (!cfgSet) {
        fprintf(stderr, "**ERROR**: The --cfg argument is required\n");
        printUpgradeUsage(argv[0]);
        return StatusInval;
    }

    return StatusOk;
}

void
logger(const char *loggerMsg)
{
    fprintf(stdout, "xc2 logger %s\n", loggerMsg);
}

Status
insertUserSession(UserSessionHashTable *userHt,
                  const char *userName,
                  const char *sessionName)
{
    Status status = StatusOk;
    UserSessionEntry *userEntry;
    SessionEntry *sessionEntry;

    // Create user if doesn't exist.
    userEntry = userHt->find(userName);
    if (!userEntry) {
        userEntry = (UserSessionEntry *) memAlloc(sizeof(*userEntry));
        BailIfNull(userEntry);
        memset(userEntry, 0, sizeof(*userEntry));
        strlcpy(userEntry->userName, userName, sizeof(userEntry->userName));
        status = userHt->insert(userEntry);
        BailIfFailed(status);
    }

    if (!sessionName) {
        goto CommonExit;
    }

    // Add session if doesn't exist.
    sessionEntry = userEntry->sessionHt.find(sessionName);
    if (!sessionEntry) {
        sessionEntry = (SessionEntry *) memAlloc(sizeof(*sessionEntry));
        BailIfNull(sessionEntry);
        strlcpy(sessionEntry->sessionName,
                sessionName,
                sizeof(sessionEntry->sessionName));
        status = userEntry->sessionHt.insert(sessionEntry);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

// Deserialize the on disk protobuf files to json and convert them to dataflow
// 2.0 format. The KvStore entries are added by the converter.
Status
upgradeQueryGraph(Dag *qgraph,
                  const char *usrName,
                  const char *sessionName,
                  const uint64_t sessionId)
{
    Status status = StatusOk;
    UserMgr *userMgr = UserMgr::get();
    XcalarApiUserId userId;
    XcalarApiSessionInfoInput sessionInput;

    verify((size_t) snprintf(userId.userIdName,
                             sizeof(userId.userIdName),
                             "%s",
                             usrName) < sizeof(userId.userIdName));
    userId.userIdUnique = (uint32_t) hashStringFast(userId.userIdName);
    sessionInput.sessionId = sessionId;
    strlcpy(sessionInput.sessionName,
            sessionName,
            sizeof(sessionInput.sessionName));
    status = userMgr->queryToDF2Upgrade(qgraph, &userId, &sessionInput);
    BailIfFailed(status);

CommonExit:
    return status;
}

// List and upgrade all the workbooks of a user.
Status
upgradeWorkbooks(const char *userName)
{
    Status status = StatusOk;
    UserMgr *userMgr = UserMgr::get();
    XcalarApiUserId user;
    XcalarApiOutput *outputOut = NULL;
    XcalarApiSessionListOutput *sessionListOutput = NULL;
    size_t outputSize = 0;
    char *sessionName = NULL;
    char sessionIdStr[SessionMgr::MaxTextHexIdSize];
    uint64_t sessionId = 0;
    char qgraphFilePrefix[XcalarApiMaxPathLen + 1];
    Dag *qgraph = NULL;
    XcalarApiSessionDeleteInput sessInput;
    XcalarApiOutput *persistOutputOut = NULL;
    size_t persistOutputSize;

    // Get list of sessions for the user
    strlcpy(user.userIdName, userName, sizeof(user.userIdName));
    user.userIdUnique = 0;
    status = userMgr->list(&user, "*", &outputOut, &outputSize);
    if (status != StatusOk) {
        insertUserSession(&badUserHt, userName, NULL);
        goto CommonExit;
    }
    sessionListOutput = &outputOut->outputResult.sessionListOutput;

    for (size_t jj = 0; jj < sessionListOutput->numSessions; jj++) {
        sessionName = sessionListOutput->sessions[jj].name;
        sessionId = sessionListOutput->sessions[jj].sessionId;
        snprintf(sessionIdStr, sizeof(sessionIdStr), "%lX", sessionId);
        fprintf(stdout,
                "Upgrading session '%s' (Id '%s') for user '%s'\n",
                sessionName,
                sessionIdStr,
                userName);
        snprintf(qgraphFilePrefix,
                 sizeof(qgraphFilePrefix),
                 "%s%s.%s",
                 QgraphPrefix,
                 userName,
                 sessionIdStr);
        status = StatusInval;
        if (status != StatusOk) {
            if (sessionIdStr[0] == '5' && sessionIdStr[1] <= '9') {
                fprintf(stdout, "Unsupported query graph version. \n");
            } else {
                if (sessionName[0] == '\0') {
                    strlcpy(sessionName, sessionIdStr, strlen(sessionIdStr));
                }
                insertUserSession(&badUserHt, userName, sessionName);
            }

            status = StatusOk;
            continue;
        }

        // Dag will be destroyed by it.
        status = upgradeQueryGraph(qgraph, userName, sessionName, sessionId);
        if (status != StatusOk) {
            insertUserSession(&badUserHt, userName, sessionName);
            continue;
        }

        // persist the session metadata (it's already been de-serialized via
        // the call to list above)

        memZero(&sessInput, sizeof(sessInput));
        sessInput.sessionId = sessionId;
        sessInput.noCleanup = false;

        status = userMgr->persist(&user,
                                  &sessInput,
                                  &persistOutputOut,
                                  &persistOutputSize);
        if (status != StatusOk) {
            fprintf(stdout,
                    "Failed to persist session '%s' (Id '%s') for user '%s'\n",
                    sessionName,
                    sessionIdStr,
                    userName);
            insertUserSession(&badUserHt, userName, sessionName);
            continue;
        }
        if (verbose) {
            fprintf(stdout, "Successfully upgraded %s\n", qgraphFilePrefix);
        }
        insertUserSession(&upgradedUserHt, userName, sessionIdStr);
    }

CommonExit:
    if (outputOut) {
        memFree(outputOut);
        outputOut = NULL;
    }

    status = StatusOk;  // Continue to the next user.
    return status;
}

bool
stringStartsWith(const char *s, const char *t)
{
    size_t ls = strlen(s);
    size_t lt = strlen(t);
    return ls < lt ? false : strncmp(s, t, lt) == 0;
}

bool
stringEndsWith(const char *s, const char *t)
{
    size_t ls = strlen(s);  // find length of s
    size_t lt = strlen(t);  // find length of t
    return ls < lt ? false : memcmp(t, s + (ls - lt), lt) == 0;
}

// List the session directory to find all usernames, and upgrade the uesrs'
// workbooks.
Status
upgradeUsers()
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    char sessionDir[XcalarApiMaxPathLen + 1];
    char *curFilename;
    char curUserName[LOGIN_NAME_MAX + 1];
    size_t curUserNameStartQgraph = strlen(QgraphPrefix);
    size_t curUserNameStartSession = strlen(SessionPrefix);
    size_t curUserNameLen = 0;
    UserSessionHashTable upgradedUsers;

    logger("Upgrading workbooks");

    snprintf(sessionDir,
             sizeof(sessionDir),
             "%s/%s",
             XcalarConfig::get()->xcalarRootCompletePath_,
             SessionDir);
    dirIter = opendir(sessionDir);
    if (!dirIter) {
        status = sysErrnoToStatus(errno);
        fprintf(stderr,
                "Failed to open session directory '%s': %s\n",
                sessionDir,
                strGetFromStatus(status));
    }

    while (true) {
        errno = 0;
        dirent *curFile = readdir(dirIter);
        if (curFile == NULL) {
            // The only error is EBADF, which should exclusively be a
            // program error
            assert(errno == 0);
            // End of directory
            break;
        }

        curFilename = curFile->d_name;

        if (!(stringStartsWith(curFilename, QgraphPrefix) ||
              stringStartsWith(curFilename, SessionPrefix)) ||
            !stringEndsWith(curFilename, MetaPostfix)) {
            continue;
        }

        if (stringStartsWith(curFilename, QgraphPrefix)) {
            curUserNameLen = strlen(curFilename) - strlen(QgraphPrefix) -
                             strlen(MetaPostfix) - 16;
            snprintf(curUserName,
                     curUserNameLen,
                     "%s",
                     curFilename + curUserNameStartQgraph);
        } else {
            curUserNameLen = strlen(curFilename) - strlen(SessionPrefix) -
                             strlen(MetaPostfix) - 16;
            snprintf(curUserName,
                     curUserNameLen,
                     "%s",
                     curFilename + curUserNameStartSession);
        }

        if (!upgradedUsers.find(curUserName)) {
            status = upgradeWorkbooks(curUserName);
            BailIfFailed(status);
            status = insertUserSession(&upgradedUsers, curUserName, NULL);
            BailIfFailed(status);
        }
    }

CommonExit:
    if (dirIter) {
        closedir(dirIter);
        dirIter = NULL;
    }

    return status;
}

Status
readFile(const char *filePath, uint8_t **fileContents, size_t *fileSize)
{
    Status status = StatusOk;
    struct stat fileStat;
    int ret = 0;
    int fd = 0;
    size_t bytesRead = 0;

    ret = stat(filePath, &fileStat);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        fprintf(stderr,
                "Failed to stat '%s': %s\n",
                filePath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    fd = open(filePath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        fprintf(stderr,
                "Failed to open '%s': %s\n",
                filePath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    *fileSize = fileStat.st_size;
    *fileContents = (uint8_t *) memAlloc(*fileSize + 1);
    if (*fileContents == NULL) {
        status = StatusNoMem;
        fprintf(stderr,
                "Failed to allocate '%lu' bytes for '%s'\n",
                *fileSize + 1,
                filePath);
        goto CommonExit;
    }

    status =
        FileUtils::convergentRead(fd, *fileContents, *fileSize, &bytesRead);

    if (status != StatusOk) {
        fprintf(stderr,
                "Failed to read '%s': %s\n",
                filePath,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(bytesRead == *fileSize);

CommonExit:
    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }

    return status;
}

// Upgrade on-disk dataflows.
Status
upgradeDataflows()
{
    Status status = StatusOk;
    char dataflowDir[XcalarApiMaxPathLen + 1];
    DIR *dirIter = NULL;
    XcalarApiUserId userId;
    XcalarApiSessionUploadInput *sessionInput = NULL;
    XcalarApiSessionNewOutput sessionNewOutput;
    uint8_t *fileContents = NULL;
    size_t fileSize = 0;

    logger("Upgrading dataflows");

    verify((size_t) snprintf(userId.userIdName,
                             sizeof(userId.userIdName),
                             "%s",
                             DataflowUserName) < sizeof(userId.userIdName));
    userId.userIdUnique = (uint32_t) hashStringFast(userId.userIdName);

    snprintf(dataflowDir,
             sizeof(dataflowDir),
             "%s/%s",
             XcalarConfig::get()->xcalarRootCompletePath_,
             DataflowDir);
    dirIter = opendir(dataflowDir);
    if (!dirIter) {
        status = sysErrnoToStatus(errno);
        fprintf(stderr,
                "Failed to open dataflow directory '%s': %s\n",
                dataflowDir,
                strGetFromStatus(status));
    }

    while (true) {
        errno = 0;
        dirent *curFile = readdir(dirIter);
        if (curFile == NULL) {
            // The only error is EBADF, which should exclusively be a
            // program error
            assert(errno == 0);
            // End of directory
            break;
        }

        char *curFilename = curFile->d_name;
        char curDataflowName[XcalarApiMaxPathLen + 1];
        if (!stringEndsWith(curFilename, TarGzPostfix)) {
            continue;
        }

        snprintf(curDataflowName,
                 strlen(curFilename) - strlen(DataflowPrefix) -
                     strlen(TarGzPostfix) + 1,
                 "%s",
                 curFilename + strlen(DataflowPrefix));

        fprintf(stdout, "Upgrading batch dataflow %s\n", curDataflowName);

        char curFilePath[XcalarApiMaxPathLen + 1];
        snprintf(curFilePath,
                 sizeof(curFilePath),
                 "%s/%s",
                 dataflowDir,
                 curFilename);

        status = readFile(curFilePath, &fileContents, &fileSize);
        BailIfFailed(status);

        size_t sessionInputSize = sizeof(*sessionInput) + fileSize;
        sessionInput =
            (XcalarApiSessionUploadInput *) memAlloc(sessionInputSize);
        BailIfNull(sessionInput);

        verify(strlcpy(sessionInput->sessionName,
                       curDataflowName,
                       sizeof(sessionInput->sessionName)) <
               sizeof(sessionInput->sessionName));
        sessionInput->sessionNameLength = strlen(sessionInput->sessionName);
        sessionInput->pathToAdditionalFiles[0] = '\0';
        sessionInput->sessionContentCount = fileSize;
        memcpy(&sessionInput->sessionContent, fileContents, fileSize);

        status =
            UserMgr::get()->upload(&userId, sessionInput, &sessionNewOutput);
        if (status != StatusOk) {
            insertUserSession(&badDataflowHt,
                              DataflowUserName,
                              curDataflowName);
            status = StatusOk;
            continue;
        }

        if (verbose) {
            fprintf(stdout, "Successfully upgraded %s\n", curDataflowName);
        }
        insertUserSession(&upgradedDataflowHt,
                          DataflowUserName,
                          curDataflowName);
    }

CommonExit:
    if (dirIter != NULL) {
        closedir(dirIter);
        dirIter = NULL;
    }

    if (fileContents != NULL) {
        memFree(fileContents);
        fileContents = NULL;
    }

    if (sessionInput != NULL) {
        memFree(sessionInput);
        sessionInput = NULL;
    }

    return status;
}

// Print the users and sessions that are bad.
void
printUserSession(UserSessionHashTable *userHt, bool isDataflow)
{
    UserSessionEntry *userEntry;
    SessionEntry *sessionEntry;
    size_t ii;

    if (!isDataflow) {
        logger("Listing bad sessions");
    } else {
        logger("Listing bad batch dataflows");
    }

    for (auto it = userHt->begin(); (userEntry = it.get()) != NULL; it.next()) {
        if (!isDataflow) {
            fprintf(stdout,
                    "User '%s' has these bad sessions:\n",
                    userEntry->getUserName());

            if (userEntry->sessionHt.getSize() == 0) {
                fprintf(stdout, "\t%s\n", "Unable to get session list");
                continue;
            }
        } else {
            fprintf(stdout, "Bad batch dataflows:\n");
        }

        ii = 1;
        for (auto it2 = userEntry->sessionHt.begin();
             (sessionEntry = it2.get()) != NULL;
             it2.next()) {
            fprintf(stdout, "\t%lu)  %s\n", ii, sessionEntry->getSessionName());
            ii++;
        }
    }
}

// Update the last write version in the global KvStore so that booting XCE
// can tell that no upgrade is needed.
Status
finalizeUpgrade()
{
    Status status = StatusOk;
    KvStoreLib *kvs = KvStoreLib::get();
    char kvKey[] = "UpgradedByVersion";
    const char *kvValue = versionGetFullStr();

    // Write something to the global kvs so that the last write version
    // gets updated.
    status = kvs->addOrReplace(XidMgr::XidGlobalKvStore,
                               kvKey,
                               strlen(kvKey) + 1,
                               kvValue,
                               strlen(kvValue) + 1,
                               true,
                               KvStoreOptSync);
    if (status != StatusOk) {
        fprintf(stderr,
                "Failed to write upgrade version to global "
                "KvStore: %s\n",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

// Delete legacy query graph and sessions protobuf files.  Will not delete the
// file(s) if upgrade failed.  Will delete the sessions dir itself if it's
// empty (normal case).

Status
cleanupLegacyQgraphsAndSessions()
{
    Status status = StatusOk;
    UserSessionEntry *userEntry;
    SessionEntry *sessionEntry;
    int ret = 0;
    char *xlrRoot = XcalarConfig::get()->xcalarRootCompletePath_;
    char queryFilename[XcalarApiMaxPathLen + 1];
    char sessionsFilename[XcalarApiMaxPathLen + 1];
    const char *const sessionsDirFilePostFixes[3] = {MetaPostfix,
                                                     ZeroPostfix,
                                                     OnePostfix};
    char queryFileAbsolutePath[XcalarApiMaxPathLen + 1];
    char sessionsFileAbsolutePath[XcalarApiMaxPathLen + 1];
    char sessionsDirAbsolutePath[XcalarApiMaxPathLen + 1];

    logger("Removing legacy query graphs and sessions metadata");

    snprintf(sessionsDirAbsolutePath,
             sizeof(sessionsDirAbsolutePath),
             "%s/%s",
             xlrRoot,
             SessionDir);

    for (auto it = upgradedUserHt.begin(); it.get(); it.next()) {
        userEntry = it.get();

        for (auto it2 = userEntry->sessionHt.begin(); it2.get(); it2.next()) {
            sessionEntry = it2.get();

            for (size_t ii = 0; ii < 3; ++ii) {
                snprintf(queryFilename,
                         sizeof(queryFilename),
                         "%s%s.%s%s",
                         QgraphPrefix,
                         userEntry->getUserName(),
                         sessionEntry->getSessionName(),
                         sessionsDirFilePostFixes[ii]);
                snprintf(queryFileAbsolutePath,
                         sizeof(queryFileAbsolutePath),
                         "%s/%s",
                         sessionsDirAbsolutePath,
                         queryFilename);
                if (verbose) {
                    fprintf(stdout, "Removing %s\n", queryFileAbsolutePath);
                }
                ret = remove(queryFileAbsolutePath);
                if (ret != 0) {
                    status = sysErrnoToStatus(errno);
                }
                snprintf(sessionsFilename,
                         sizeof(sessionsFilename),
                         "%s%s.%s%s",
                         SessionPrefix,
                         userEntry->getUserName(),
                         sessionEntry->getSessionName(),
                         sessionsDirFilePostFixes[ii]);
                snprintf(sessionsFileAbsolutePath,
                         sizeof(sessionsFileAbsolutePath),
                         "%s/%s",
                         sessionsDirAbsolutePath,
                         sessionsFilename);
                if (verbose) {
                    fprintf(stdout, "Removing %s\n", sessionsFileAbsolutePath);
                }
                ret = remove(sessionsFileAbsolutePath);
                if (ret != 0) {
                    status = sysErrnoToStatus(errno);
                }
            }
        }
    }

    if (FileUtils::isDirectoryEmpty(sessionsDirAbsolutePath) == true) {
        if (verbose) {
            fprintf(stdout, "Removing %s\n", sessionsDirAbsolutePath);
        }
        ret = remove(sessionsDirAbsolutePath);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
        }
    }

    return status;
}

Status
cleanupLegacyDataflows()
{
    Status status = StatusOk;
    UserSessionEntry *userEntry;
    SessionEntry *sessionEntry;
    int ret = 0;
    char *xlrRoot = XcalarConfig::get()->xcalarRootCompletePath_;

    logger("Removing legacy batch dataflows");

    if (upgradedDataflowHt.getSize() == 0) {
        goto CommonExit;
    }

    userEntry = upgradedDataflowHt.begin().get();

    for (auto it2 = userEntry->sessionHt.begin(); it2.get(); it2.next()) {
        sessionEntry = it2.get();
        char dataflowFileAbsolutePath[XcalarApiMaxPathLen + 1];
        char dataflowFileAbsolutePathExtension[XcalarApiMaxPathLen + 1];
        char dataflowFilename[XcalarApiMaxPathLen + 1];
        snprintf(dataflowFilename,
                 sizeof(dataflowFilename),
                 "%s%s",
                 DataflowPrefix,
                 sessionEntry->getSessionName());
        snprintf(dataflowFileAbsolutePath,
                 sizeof(dataflowFileAbsolutePath),
                 "%s/%s/%s",
                 xlrRoot,
                 DataflowDir,
                 dataflowFilename);
        snprintf(dataflowFilename,
                 sizeof(dataflowFilename),
                 "%s%s%s",
                 DataflowPrefix,
                 sessionEntry->getSessionName(),
                 TarGzPostfix);
        snprintf(dataflowFileAbsolutePathExtension,
                 sizeof(dataflowFileAbsolutePathExtension),
                 "%s/%s/%s",
                 xlrRoot,
                 DataflowDir,
                 dataflowFilename);
        if (verbose) {
            fprintf(stdout, "Removing %s\n", dataflowFileAbsolutePath);
        }
        ret = remove(dataflowFileAbsolutePath);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
        }

        if (verbose) {
            fprintf(stdout, "Removing %s\n", dataflowFileAbsolutePathExtension);
        }
        ret = remove(dataflowFileAbsolutePathExtension);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
        }
    }

CommonExit:
    return status;
}

// Delete legacy query graphs in workbooks and batch dataflows.
Status
cleanupLegacyFiles()
{
    Status status = StatusOk;

    status = cleanupLegacyQgraphsAndSessions();
    status = cleanupLegacyDataflows();

    return status;
}

void *
doUpgradeWork(void *args)
{
    Status status = StatusOk;
    UpgradeToolArgs *upgradeArgs = (UpgradeToolArgs *) args;
    KvStoreLib *kvs = KvStoreLib::get();
    bool kvsOpened = false;

    status = kvs->open(XidMgr::XidGlobalKvStore,
                       KvStoreLib::KvGlobalStoreName,
                       KvStoreGlobal);
    if (status != StatusOk) {
        fprintf(stderr,
                "Failed to open global KvStore: %s\n",
                strGetFromStatus(status));
        goto CommonExit;
    }
    kvsOpened = true;

    status = AppMgr::get()->addBuildInApps();
    BailIfFailed(status);

    logger("Upgrading UDFs");
    status = UserDefinedFunction::get()->addDefaultUdfs();
    BailIfFailed(status);
    status = UserDefinedFunction::get()->unpersistAll(true);
    BailIfFailed(status);

    status = upgradeUsers();
    BailIfFailed(status);

    status = upgradeDataflows();
    BailIfFailed(status);

    status = cleanupLegacyFiles();
    // We don't bail if clean up failed. Error info is printed by the function.

    status = finalizeUpgrade();
    BailIfFailed(status);

    printUserSession(&badUserHt, false);
    printUserSession(&badDataflowHt, true);

CommonExit:
    if (kvsOpened) {
        kvs->close(XidMgr::XidGlobalKvStore, KvStoreCloseOnly);
        kvsOpened = false;
    }

    // Session persistence.
    UserMgr::get()->shutdown();

    upgradeArgs->status = status;

    return NULL;
}

Status
startUpgradeWorker(UpgradeToolOptions *options)
{
    Status status = StatusOk;
    pthread_t threadHandle;
    UpgradeToolArgs upgradeArgs;
    upgradeArgs.options = options;

    // The work must be done using a separate thread as the main thread
    // cannot call into the runtime.
    status = Runtime::get()->createBlockableThread(&threadHandle,
                                                   NULL,
                                                   doUpgradeWork,
                                                   &upgradeArgs);
    if (status != StatusOk) {
        fprintf(stderr, "Unable to create worker thread\n");
        goto CommonExit;
    }

    sysThreadJoin(threadHandle, NULL);

    status = upgradeArgs.status;

CommonExit:

    return status;
}

Status
doCopyOrMove(const char *cmd, const char *srcDir, const char *dstDir)
{
    Status status = StatusOk;
    char cmdStr[MaxCmdLength];
    char line[MaxCmdLength];
    int ret;
    FILE *file;
    bool outputReceived = false;

    // Command to make the destination directory and the do the copy/move of
    // the files
    ret = snprintf(cmdStr,
                   sizeof(cmdStr),
                   "mkdir -p %s && %s %s %s 2>&1",
                   dstDir,
                   cmd,
                   srcDir,
                   dstDir);
    assert(ret >= 0 && ret < (int) sizeof(cmdStr));

    errno = 0;
    file = popen(cmdStr, "r");
    if (file == NULL) {
        status = sysErrnoToStatus(errno);
        fprintf(stderr,
                "Failed to execute '%s': %s\n",
                cmdStr,
                strGetFromStatus(status));
        goto CommonExit;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        outputReceived = true;
        fprintf(stderr, "\tResponse: %s\n", line);
    }
    pclose(file);

    // Any output received constitutes an error
    if (outputReceived) {
        status = StatusInval;
        fprintf(stderr, "Error occurred running '%s'\n", cmdStr);
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
backupApplicableData(const char *dirName)
{
    Status status = StatusOk;
    int ret;
    char *xlrRoot = XcalarConfig::get()->xcalarRootCompletePath_;
    char srcDir[XcalarApiMaxPathLen + 1];
    char dstDir[XcalarApiMaxPathLen + 1];

    ret = snprintf(srcDir, sizeof(srcDir), "%s/%s", xlrRoot, dirName);
    assert(ret >= 0 && ret < (int) sizeof(srcDir));
    ret = snprintf(dstDir, sizeof(dstDir), "%s/%s/", xlrRoot, BackupDir);
    assert(ret >= 0 && ret < (int) sizeof(dstDir));

    fprintf(stdout, "Copying '%s' to '%s'\n", srcDir, dstDir);

    // In a dev env, this can possibly fail if there's no `session` dir which
    // doesn't exit in 2.0.
    status = doCopyOrMove("cp -Lr", srcDir, dstDir);

    // We ignore the bad status. Any related info should be already printed out.
    // This backup is just a backup of users manual backup, so it's fine to
    // continue when the auto backup somehow failed.
    status = StatusOk;

    return status;
}

int
main(int argc, char *argv[])
{
    Status status = StatusOk;
    UpgradeToolOptions upgradeOptions;
    Txn txn;

    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    status = parseUpgradeOptions(argc, argv, &upgradeOptions);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityUsrNode,
                                (char *) upgradeOptions.cfgFile,
                                NULL,
                                NULL,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeUsrnode);
    if (status != StatusOk) {
        fprintf(stderr, "Failed to init: %s\n", strGetFromStatus(status));
        goto CommonExit;
    }

    // Set transaction ID so that we don't hit asserts in message layer as
    // all processing must occur in the context of a transaction.
    txn = Txn::newTxn(Txn::Mode::NonLRQ);
    Txn::setTxn(txn);

    logger("Backing up directories modified during upgrade");
    status = backupApplicableData(SessionDir);
    BailIfFailed(status);
    status = backupApplicableData(DataflowDir);
    BailIfFailed(status);
    status = backupApplicableData(KvsDir);
    BailIfFailed(status);
    status = backupApplicableData(WorkbookDir);
    BailIfFailed(status);

    status = startUpgradeWorker(&upgradeOptions);
    if (status != StatusOk) {
        fprintf(stderr, "Upgrade failed: %s\n", strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error %s\n", strGetFromStatus(status));
        return status.code();
    } else {
        return badUserHt.empty() && badDataflowHt.empty() ? 0 : 1;
    }
}
