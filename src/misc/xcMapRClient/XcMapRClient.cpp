// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <libgen.h>
#include <dlfcn.h>

#include "XcMapRClient.h"

/* ******************************** MapRLib ********************************* */

int
MapRLib::init()
{
    bool success = false;
    char *error;

    // Clear dlerrors
    dlerror();

    libHandle_ = dlopen(MaprLibName, RTLD_LAZY);
    if (libHandle_ == NULL) {
        error = dlerror();
        XcMapRClient::err("Failed to load '%s': \"%s\".\nIs MapR installed?\n",
                          MaprLibName,
                          error);
        goto CommonExit;
    }

    newBuilder_ = reinterpret_cast<struct hdfsBuilder *(*) (void)>(
        dlsym(libHandle_, "hdfsNewBuilder"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "hdfsNewBuilder", error);
        goto CommonExit;
    }

    freeBuilder_ = reinterpret_cast<void (*)(struct hdfsBuilder *)>(
        dlsym(libHandle_, "hdfsFreeBuilder"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "freeBuilder_", error);
        goto CommonExit;
    }

    builderSetNameNode_ =
        reinterpret_cast<void (*)(struct hdfsBuilder *, const char *)>(
            dlsym(libHandle_, "hdfsBuilderSetNameNode"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsBuilderSetNameNode",
                          error);
        goto CommonExit;
    }

    builderSetNameNodePort_ =
        reinterpret_cast<void (*)(struct hdfsBuilder *, tPort)>(
            dlsym(libHandle_, "hdfsBuilderSetNameNodePort"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsBuilderSetNameNodePort",
                          error);
        goto CommonExit;
    }

    builderSetUserName_ =
        reinterpret_cast<void (*)(struct hdfsBuilder *, const char *)>(
            dlsym(libHandle_, "hdfsBuilderSetUserName"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsBuilderSetUserName",
                          error);
        goto CommonExit;
    }

    // Connection
    connectAsUser_ =
        reinterpret_cast<hdfsFS (*)(const char *, tPort, const char *)>(
            dlsym(libHandle_, "hdfsConnectAsUser"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsConnectAsUser",
                          error);
        goto CommonExit;
    }

    builderConnect_ = reinterpret_cast<hdfsFS (*)(struct hdfsBuilder *)>(
        dlsym(libHandle_, "hdfsBuilderConnect"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsBuilderConnect",
                          error);
        goto CommonExit;
    }
    disconnect_ = reinterpret_cast<int (*)(hdfsFS fs)>(
        dlsym(libHandle_, "hdfsDisconnect"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "hdfsDisconnect", error);
        goto CommonExit;
    }

    // Directory ops
    listDirectory_ =
        reinterpret_cast<hdfsFileInfo *(*) (hdfsFS, const char *, int *)>(
            dlsym(libHandle_, "hdfsListDirectory"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsListDirectory",
                          error);
        goto CommonExit;
    }
    freeFileInfo_ = reinterpret_cast<void (*)(hdfsFileInfo *, int)>(
        dlsym(libHandle_, "hdfsFreeFileInfo"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n",
                          "hdfsFreeFileInfo",
                          error);
        goto CommonExit;
    }

    // File ops
    openFile_ = reinterpret_cast<
        hdfsFile (*)(hdfsFS fs, const char *, int, int, short, tSize)>(
        dlsym(libHandle_, "hdfsOpenFile"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "hdfsOpenFile", error);
        goto CommonExit;
    }

    closeFile_ = reinterpret_cast<int (*)(hdfsFS, hdfsFile)>(
        dlsym(libHandle_, "hdfsCloseFile"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "hdfsCloseFile", error);
        goto CommonExit;
    }

    seek_ = reinterpret_cast<int (*)(hdfsFS, hdfsFile, tOffset)>(
        dlsym(libHandle_, "hdfsSeek"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "hdfsSeek", error);
        goto CommonExit;
    }

    read_ = reinterpret_cast<tSize (*)(hdfsFS, hdfsFile, void *, tSize)>(
        dlsym(libHandle_, "hdfsRead"));
    if ((error = dlerror()) != NULL) {
        XcMapRClient::err("Failed to find '%s': %s\n", "hdfsRead", error);
        goto CommonExit;
    }

    success = true;
CommonExit:
    if (success) {
        return 0;
    } else {
        return -1;
    }
}

MapRLib::~MapRLib() {}

struct hdfsBuilder *
MapRLib::newBuilder(void) const
{
    return (*newBuilder_)();
}

void
MapRLib::freeBuilder(struct hdfsBuilder *bld) const
{
    return (*freeBuilder_)(bld);
}

void
MapRLib::builderSetNameNode(struct hdfsBuilder *bld, const char *nn) const
{
    return (*builderSetNameNode_)(bld, nn);
}

void
MapRLib::builderSetNameNodePort(struct hdfsBuilder *bld, tPort port) const
{
    return (*builderSetNameNodePort_)(bld, port);
}
void
MapRLib::builderSetUserName(struct hdfsBuilder *bld, const char *userName) const
{
    return (*builderSetUserName_)(bld, userName);
}

// Connection
hdfsFS
MapRLib::connectAsUser(const char *nn, tPort port, const char *user) const
{
    return (*connectAsUser_)(nn, port, user);
}

hdfsFS
MapRLib::builderConnect(struct hdfsBuilder *bld) const
{
    return (*builderConnect_)(bld);
}

int
MapRLib::disconnect(hdfsFS fs) const
{
    return (*disconnect_)(fs);
}

// Directory ops
hdfsFileInfo *
MapRLib::listDirectory(hdfsFS fs, const char *path, int *numEntries) const
{
    return (*listDirectory_)(fs, path, numEntries);
}

void
MapRLib::freeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries) const
{
    return (*freeFileInfo_)(hdfsFileInfo, numEntries);
}

// File ops
hdfsFile
MapRLib::openFile(hdfsFS fs,
                  const char *path,
                  int flags,
                  int bufferSize,
                  short replication,
                  tSize blocksize) const
{
    return (*openFile_)(fs, path, flags, bufferSize, replication, blocksize);
}

int
MapRLib::closeFile(hdfsFS fs, hdfsFile file) const
{
    return (*closeFile_)(fs, file);
}

int
MapRLib::seek(hdfsFS fs, hdfsFile file, tOffset desiredPos) const
{
    return (*seek_)(fs, file, desiredPos);
}

tSize
MapRLib::read(hdfsFS fs, hdfsFile file, void *buffer, tSize length) const
{
    return (*read_)(fs, file, buffer, length);
}

/* ****************************** XcMapRClient ****************************** */

XcMapRClient::~XcMapRClient()
{
    if (hdfsConn_) {
        int ret = maprLib_.disconnect(hdfsConn_);
        if (ret == -1) {
            err("disconnect error");
        }
        hdfsConn_ = NULL;
    }
}

int
XcMapRClient::init(const char *host, int port, const char *user)
{
    bool success = false;
    struct hdfsBuilder *builder = NULL;
    int ret;

    ret = maprLib_.init();
    if (ret != 0) {
        goto CommonExit;
    }

    hdfsConn_ = maprLib_.connectAsUser(host, (tPort) port, user);
    if (hdfsConn_ == NULL) {
        err("failed to connect to %s:%i as %s\n", host, port, user ? user : "");
        goto CommonExit;
    }

    success = true;
CommonExit:
    if (builder) {
        maprLib_.freeBuilder(builder);
        builder = NULL;
    }
    if (success) {
        return 0;
    } else {
        return -1;
    }
}

int
XcMapRClient::lsToStdout(const char *path, bool recursive)
{
    int numInfos;
    json_t *fileArray = NULL;
    bool success = false;
    char *jsonFileStr = NULL;

    fileArray = ls(path, recursive, &numInfos);
    if (fileArray == NULL) {
        goto CommonExit;
    }

    jsonFileStr = json_dumps(fileArray, 0);
    if (jsonFileStr == NULL) {
        err("failed to allocate json file string");
        goto CommonExit;
    }

    printf("%s\n", jsonFileStr);

    success = true;
CommonExit:
    if (fileArray) {
        json_decref(fileArray);
        fileArray = NULL;
    }
    if (jsonFileStr) {
        // @SymbolCheckIgnore
        free(jsonFileStr);
        jsonFileStr = NULL;
    }
    if (success) {
        return 0;
    } else {
        return -1;
    }
}

int
XcMapRClient::readToStdout(const char *path, int64_t offset, int64_t numBytes)
{
    bool success = false;
    hdfsFile file = NULL;
    char *buffer = NULL;
    int ret;
    int64_t totalRead;
    int64_t totalWritten;

    // @SymbolCheckIgnore
    buffer = static_cast<char *>(malloc(numBytes));
    if (buffer == NULL) {
        err("failed to allocate read buffer (%lu bytes) for '%s'\n",
            numBytes,
            path);
        goto CommonExit;
    }

    // Use default values for bufferSize, replication, blocksize
    file = maprLib_.openFile(hdfsConn_, path, O_RDONLY, 0, 0, 0);
    if (file == NULL) {
        goto CommonExit;
    }

    if (offset != 0) {
        ret = maprLib_.seek(hdfsConn_, file, offset);
        if (ret != 0) {
            err("failed to seek to %i in '%s'\n", offset, path);
            goto CommonExit;
        }
    }

    totalRead = 0;
    while (true) {
        int64_t bytesRead = maprLib_.read(hdfsConn_,
                                          file,
                                          buffer + totalRead,
                                          numBytes - totalRead);
        if (bytesRead > 0) {
            totalRead += bytesRead;
        } else if (bytesRead == -1 && errno == EINTR) {
            continue;
        } else if (bytesRead == 0) {
            // We have reached the end of the file
            break;
        } else {
            assert(bytesRead < 0);
            err("failed to read '%s': errno=%i\n", path, errno);
            goto CommonExit;
        }
    }

    totalWritten = 0;
    while (true) {
        if (totalWritten == totalRead) {
            break;
        }
        int64_t bytesWritten = write(STDOUT_FILENO,
                                     &buffer[totalWritten],
                                     totalRead - totalWritten);
        assert(bytesWritten != 0);
        if (bytesWritten > 0) {
            totalWritten += bytesWritten;
        } else if (bytesWritten == -1 && errno == EINTR) {
            continue;
        } else {
            assert(bytesWritten < 0);
            err("failed to report read from '%s': errno=%i\n", path, errno);
            goto CommonExit;
        }
    }
    assert(totalWritten == totalRead);

    success = true;
CommonExit:
    if (file != NULL) {
        ret = maprLib_.closeFile(hdfsConn_, file);
        if (ret != 0) {
            err("error closing '%s': errno=%i\n", path, errno);
            // XXX - since this is harmless, we can maybe let it slip by...
            success = false;
        }
        file = NULL;
    }
    if (buffer != NULL) {
        // @SymbolCheckIgnore
        free(buffer);
        buffer = NULL;
    }
    if (success) {
        return 0;
    } else {
        return -1;
    }
}

json_t *
XcMapRClient::convertInfosToList(const hdfsFileInfo *infos, int numInfos)
{
    json_t *fileArray = NULL;
    int ret;
    bool success = false;

    fileArray = json_array();
    if (fileArray == NULL) {
        err("failed to allocate json file array");
        goto CommonExit;
    }

    for (int ii = 0; ii < numInfos; ii++) {
        json_t *infoDict = convertInfoToDict(&infos[ii]);
        ret = json_array_append_new(fileArray, infoDict);
        if (ret == -1) {
            err("failed to append file info to json array");
            goto CommonExit;
        }
    }

    success = true;
CommonExit:
    if (!success) {
        if (fileArray) {
            json_decref(fileArray);
            fileArray = NULL;
        }
    }
    return fileArray;
}

json_t *
XcMapRClient::convertInfoToDict(const hdfsFileInfo *info)
{
    json_t *fileDict = NULL;
    json_t *fieldValue = NULL;
    int ret;
    bool success = false;

    fileDict = json_object();
    if (fileDict == NULL) {
        goto CommonExit;
    }

    // mKind
    if (info->mKind == kObjectKindFile) {
        fieldValue = json_string("F");
    } else if (info->mKind == kObjectKindDirectory) {
        fieldValue = json_string("D");
    } else {
        err("unrecognized MapR file kind '%c'", info->mKind);
        goto CommonExit;
    }
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mKind", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mName
    fieldValue = json_string(info->mName);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mName", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mLastMod
    fieldValue = json_integer(info->mLastMod);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mLastMod", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mSize
    fieldValue = json_integer(info->mSize);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mSize", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mReplication
    fieldValue = json_integer(info->mReplication);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mReplication", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mBlockSize
    fieldValue = json_integer(info->mBlockSize);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mBlockSize", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mOwner
    fieldValue = json_string(info->mOwner);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mOwner", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mGroup
    fieldValue = json_string(info->mGroup);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mGroup", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mPermissions
    fieldValue = json_integer(info->mPermissions);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mPermissions", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    // mLastAccess
    fieldValue = json_integer(info->mLastAccess);
    if (fieldValue == NULL) {
        goto CommonExit;
    }
    ret = json_object_set_new(fileDict, "mLastAccess", fieldValue);
    if (ret == -1) {
        goto CommonExit;
    }
    fieldValue = NULL;

    success = true;
CommonExit:
    if (!success) {
        err("failed to allocated fileDict for MapR file '%s'", info->mName);
        if (fileDict) {
            json_decref(fileDict);
            fileDict = NULL;
        }
    }
    if (fieldValue) {
        json_decref(fieldValue);
        fieldValue = NULL;
    }
    return fileDict;
}

json_t *
XcMapRClient::ls(const char *path, bool recursive, int *numInfos)
{
    json_t *fileArray = NULL;
    json_t *subDirArray = NULL;
    int subDirNumInfos;
    hdfsFileInfo *infos = NULL;
    bool success = false;
    int listErrno;

    infos = maprLib_.listDirectory(hdfsConn_, path, numInfos);
    listErrno = errno;
    if (infos != NULL) {
        // We have successfully listed
        fileArray = convertInfosToList(infos, *numInfos);
        if (!fileArray) {
            goto CommonExit;
        }

        if (recursive) {
            // Recurse into all subdirectories
            for (int ii = 0; ii < *numInfos; ii++) {
                const hdfsFileInfo *info = &infos[ii];
                if (info->mKind != kObjectKindDirectory) {
                    continue;
                }
                subDirArray = ls(info->mName, true, &subDirNumInfos);
                if (subDirArray == NULL) {
                    goto CommonExit;
                }

                // Add the subdirectory files to our list
                int ret = json_array_extend(fileArray, subDirArray);
                if (ret != 0) {
                    goto CommonExit;
                }
                json_array_clear(subDirArray);
                json_decref(subDirArray);
                subDirArray = NULL;
            }
        }
    } else if (listErrno == ENOTDIR) {
        // This path refers to a specific file, not a directory. Try again
        // for the parent directory and filter down to the single file
        int pathLen = strlen(path);
        char tmpPath[pathLen + 1];
        memcpy(tmpPath, path, pathLen);
        tmpPath[pathLen] = '\0';
        const char *pathDirname = dirname(tmpPath);

        infos = maprLib_.listDirectory(hdfsConn_, pathDirname, numInfos);
        if (infos == NULL) {
            goto CommonExit;
        }
        const hdfsFileInfo *singleFile = NULL;
        for (int ii = 0; ii < *numInfos; ii++) {
            if (strcmp(infos[ii].mName, path) == 0) {
                singleFile = &infos[ii];
            }
        }
        if (singleFile == NULL) {
            err("directory '%s' changed during operation "
                "while looking for '%s'\n",
                pathDirname,
                path);
            goto CommonExit;
        }
        fileArray = convertInfosToList(singleFile, 1);
        if (!fileArray) {
            goto CommonExit;
        }
    } else {
        // Catch all other hdfsListDirectory errors
        char errnoStrBuf[ErrStrBufSize];
        err("Error while reading directory '%s' - %s\n",
            path,
            strerror_r(listErrno, errnoStrBuf, sizeof(errnoStrBuf)));
        goto CommonExit;
    }

    success = true;
CommonExit:
    if (infos != NULL) {
        maprLib_.freeFileInfo(infos, *numInfos);
        infos = NULL;
    }
    if (subDirArray != NULL) {
        json_array_clear(subDirArray);
        json_decref(subDirArray);
        subDirArray = NULL;
    }
    if (!success) {
        if (fileArray) {
            json_decref(fileArray);
            fileArray = NULL;
        }
    }
    return fileArray;
}

void
XcMapRClient::err(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
}

int
main(int argc, char *argv[])
{
    return XcMapRClient::main(argc, argv);
}

int
XcMapRClient::main(int argc, char *argv[])
{
    static struct option long_options[] = {
        {"cldbNode", required_argument, 0, 'c'},
        {"port", required_argument, 0, 'p'},
        {"user", required_argument, 0, 'u'},
        {"path", required_argument, 0, 'a'},
        // ls specific
        {"recursive", no_argument, 0, 'r'},
        // cat specific
        {"offset", required_argument, 0, 'o'},
        {"numBytes", required_argument, 0, 'n'},
        {0, 0, 0, 0},
    };

    bool success = false;
    XcMapRClient client;
    const char *cldbNode = "default";
    const char *user = NULL;
    int port = 7222;
    const char *path = NULL;
    bool recursive = false;
    int64_t offset = 0;
    int64_t numBytes = 4 * 1 << 10;  // 4 KB

    int flag;
    int optionIndex = 0;
    char *endptr;
    int ret;

    int remainingArgs;
    const char *command;

    // The ":" follows required args.
    while ((flag = getopt_long(argc,
                               argv,
                               "c:p:u:a:ro:n:b:",
                               long_options,
                               &optionIndex)) != -1) {
        switch (flag) {
        case 'c':
            cldbNode = optarg;
            break;
        case 'p': {
            int maybePort = strtoul(optarg, &endptr, 10);
            if (*endptr != '\0' || maybePort < 0 || maybePort > 65535) {
                err("invalid port '%s'.\n", optarg);
                goto CommonExit;
            }
            port = maybePort;
            break;
        }
        case 'u':
            user = optarg;
            break;
        case 'a':
            path = optarg;
            break;
        case 'r':
            recursive = true;
            break;
        case 'o': {
            int64_t maybeOffset = strtoull(optarg, &endptr, 10);
            if (*endptr != '\0') {
                err("invalid offset '%s'.\n", optarg);
                goto CommonExit;
            }
            offset = maybeOffset;
            break;
        }
        case 'n': {
            int64_t maybeNumBytes = strtoull(optarg, &endptr, 10);
            if (*endptr != '\0') {
                err("invalid numBytes '%s'.\n", optarg);
                goto CommonExit;
            }
            numBytes = maybeNumBytes;
            break;
        }
        default:
            break;
        }
    }

    if (path == NULL) {
        err(HelpStr);
        err("--path is required\n");
        goto CommonExit;
    }

    // Initialize client
    ret = client.init(cldbNode, port, user);
    if (ret != 0) {
        err("failed to initialize MapR client\n");
        goto CommonExit;
    }

    // Now we process the position arguments (commands)
    remainingArgs = argc - optind;

    if (remainingArgs != 1) {
        err(HelpStr);
        err("exactly 1 command must be supplied. %i commands supplied:",
            remainingArgs);
        for (int ii = 0; ii < remainingArgs; ii++) {
            err(" '%s'", argv[optind + ii]);
        }
        err("\n");
        goto CommonExit;
    }
    command = argv[optind];

    if (strcmp(command, LsCommand) == 0) {
        ret = client.lsToStdout(path, recursive);
        if (ret != 0) {
            goto CommonExit;
        }
    } else if (strcmp(command, ReadCommand) == 0) {
        ret = client.readToStdout(path, offset, numBytes);
        if (ret != 0) {
            goto CommonExit;
        }
    } else {
        err(HelpStr);
        err("command %s not supported\n", command);
        goto CommonExit;
    }

    success = true;
CommonExit:
    if (success) {
        return 0;
    } else {
        return -1;
    }
}
