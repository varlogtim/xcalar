// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// Set preprocessor variables so compilation doesn't fail
#define LIBHDFS_DLL_IMPORT 0
#include "hdfs.h"

#include <jansson.h>

class MapRLib {
public:
    MapRLib() = default;
    ~MapRLib();

    int init();

    // Connection builder
    struct hdfsBuilder *newBuilder(void) const;
    void freeBuilder(struct hdfsBuilder *bld) const;
    void builderSetNameNode(struct hdfsBuilder *bld, const char *nn) const;
    void builderSetNameNodePort(struct hdfsBuilder *bld, tPort port) const;
    void builderSetUserName(struct hdfsBuilder *bld,
                            const char *userName) const;

    // Connection
    hdfsFS connectAsUser(const char *nn, tPort port, const char *user) const;
    hdfsFS builderConnect(struct hdfsBuilder *bld) const;
    int disconnect(hdfsFS fs) const;

    // Directory ops
    hdfsFileInfo *listDirectory(hdfsFS fs,
                                const char *path,
                                int *numEntries) const;
    void freeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries) const;

    // File ops
    hdfsFile openFile(hdfsFS fs,
                      const char *path,
                      int flags,
                      int bufferSize,
                      short replication,
                      tSize blocksize) const;
    int closeFile(hdfsFS fs, hdfsFile file) const;
    int seek(hdfsFS fs, hdfsFile file, tOffset desiredPos) const;
    tSize read(hdfsFS fs, hdfsFile file, void *buffer, tSize length) const;

private:
    static constexpr const char *MaprLibName = "libMapRClient.so";
    void *libHandle_ = NULL;

    struct hdfsBuilder *(*newBuilder_)(void);
    void (*freeBuilder_)(struct hdfsBuilder *bld);
    void (*builderSetNameNode_)(struct hdfsBuilder *bld, const char *nn);
    void (*builderSetNameNodePort_)(struct hdfsBuilder *bld, tPort port);
    void (*builderSetUserName_)(struct hdfsBuilder *bld, const char *userName);

    // Connection
    hdfsFS (*connectAsUser_)(const char* nn, tPort port, const char *user);
    hdfsFS (*builderConnect_)(struct hdfsBuilder *bld);
    int (*disconnect_)(hdfsFS fs);

    // Directory ops
    hdfsFileInfo *(*listDirectory_)(hdfsFS fs,
                                const char *path,
                                int *numEntries);
    void (*freeFileInfo_)(hdfsFileInfo *hdfsFileInfo, int numEntries);

    // File ops
    hdfsFile (*openFile_)(hdfsFS fs,
                      const char *path,
                      int flags,
                      int bufferSize,
                      short replication,
                      tSize blocksize);
    int (*closeFile_)(hdfsFS fs, hdfsFile file);
    int (*seek_)(hdfsFS fs, hdfsFile file, tOffset desiredPos);
    tSize (*read_)(hdfsFS fs, hdfsFile file, void *buffer, tSize length);
};

class XcMapRClient {
public:
    static void err(const char *format, ...);

    XcMapRClient() = default;
    ~XcMapRClient();

    int init(const char *host, int port, const char *user);

    static int main(int argc, char *argv[]);

    int lsToStdout(const char *path, bool recursive);
    int readToStdout(const char *path, int64_t offset, int64_t numBytes);

private:
    static constexpr const char *HelpStr =
        "Usage: xcMapRClient [optional flags] <command>\n"
        "  command may be:\n"
        "    ls --path PATH [--recursive]\n"
        "    read --path PATH [--offset OFFSET] [--numBytes NUMBYTES]\n"
        "\n"
        "  general options:\n"
        "    --cldbNode CLDBNODE\n"
        "    --port PORT\n"
        "    --user USER\n"
        "";
    static constexpr const size_t ErrStrBufSize = 128;
    static constexpr const char *LsCommand = "ls";
    static constexpr const char *ReadCommand = "read";
    static json_t *convertInfosToList(const hdfsFileInfo *infos, int numInfos);
    static json_t *convertInfoToDict(const hdfsFileInfo *info);

    // Returns a json_t array of dicts representing the file infos
    json_t *ls(const char *path, bool recursive, int *numInfos);

    hdfsFS hdfsConn_ = NULL;
    MapRLib maprLib_;
};
