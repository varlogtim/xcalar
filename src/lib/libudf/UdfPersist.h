// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFPERSIST_H_
#define _UDFPERSIST_H_

#include <dirent.h>

#include "primitives/Primitives.h"
#include "UdfTypeEnums.h"
#include "udf/UdfTypes.h"
#include "util/StringHashTable.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "libapis/LibApisCommon.h"
#include "LibUdfConstants.h"

//
// This class persists UDFs to disk and reads them back in. To save space,
// persistence is not log based and information can be lost in the event of a
// crash. Error detection mechanisms are used to prevent this from resulting
// in corrupted UDF modules.
//

class UdfPersist final
{
    friend class TwoPcMsg2pcAddOrUpdateUdfDlm1;

  public:
    UdfPersist();
    ~UdfPersist();

    // Assume that the max name length of a udf dir's child is the name length
    // of a python dir (that's the only lang supported) == sizeof(sourcesPyName)

    static constexpr unsigned MaxUdfDirChildPathLen =
        MaxUdfPathLen + 1 + sizeof(udf::sourcesPyName);

    MustCheck Status init(bool skipRestore);
    void destroy();
    MustCheck Status update(const char *moduleName,
                            XcalarApiUdfContainer *udfContainer,
                            UdfType type,
                            const char *source);

    void del(const char *moduleName,
             XcalarApiUdfContainer *udfContainer,
             UdfType type);

    // legacyDel used to look for and delete the legacy old-style persistent
    // UDF metafiles and versioned UDF python files - needed only to support
    // upgrade
    void legacyDel(const char *moduleName,
                   XcalarApiUdfContainer *udfContainer,
                   UdfType type);
    MustCheck Status addAll(bool upgradeToDionysus);
    MustCheck Status deleteWorkbookDirectory(const char *userName,
                                             const uint64_t sessionId);
    MustCheck Status getUdfOnDiskPath(const char *udfLibNsPath,
                                      char **udfOnDiskPath);
    MustCheck bool finishedRestoring() { return finishedRestore_; }
    // This is only for use by the XcUpgrade tool.
    void setFinishedRestoring(bool newValue) { finishedRestore_ = newValue; }
    void processUdfsDir(DIR *udfDir,
                        char *dirPath,
                        size_t maxDirNameLen,
                        XcalarApiUdfContainer *udfContainer,
                        bool upgradeToDionysus);

    MustCheck bool needsPersist(UdfModuleSrc *input,
                                XcalarApiUdfContainer *udfContainer);

  private:
    static constexpr const char *ModuleName = "libudf";
    static constexpr const char *EyeCatcherBegin = "UDFX";
    static constexpr const char *EyeCatcherEnd = "UDFY";
    static constexpr const size_t ModuleNameSlotCount = 61;  // prime number

    struct ModuleNameEntry {
        char moduleName[UdfVersionedModuleName + 1];
        uint64_t curVersion;
        StringHashTableHook hook;
        const char *getModuleName() const { return moduleName; };
        void del() { memFree(this); }
    };

    typedef StringHashTable<ModuleNameEntry,
                            &ModuleNameEntry::hook,
                            &ModuleNameEntry::getModuleName,
                            ModuleNameSlotCount,
                            hashStringFast>
        ModuleNameHashTable;
    ModuleNameHashTable moduleNameHT_;

    bool finishedRestore_;

    // Persisted UDF info persisted as follows:
    // $xcalarDir/udf         Contains metadata about each module.
    // $xcalarDir/udf/python  Contains source of each python module.
    // $xcalarDir/udf is UdfPath
    char *sourcesPyPath_;

    // Disallow.
    UdfPersist(const UdfPersist &) = delete;
    UdfPersist &operator=(const UdfPersist &) = delete;

    MustCheck Status createOrTruncFile(const char *filePath,
                                       int *fd,
                                       int rwFlag);
    MustCheck Status fileNames(const char *moduleName,
                               UdfType type,
                               XcalarApiUdfContainer *udfContainer,
                               char *fileNameSourceTmp,
                               size_t *fileNameSourceTmpSize,
                               char *fileNameSource,
                               size_t *fileNameSourceSize);
    MustCheck Status addModule(const char *fileNameSource,
                               UdfType udfType,
                               XcalarApiUdfContainer *udfContainer);
    void processUdfsPyDir(DIR *udfPyDir,
                          char *dirPath,
                          XcalarApiUdfContainer *udfContainer);
    void processUdfsDirPostUpgrade(DIR *udfDir,
                                   char *dirPath,
                                   size_t maxDirNameLen,
                                   XcalarApiUdfContainer *udfContainer);
    // Return StatusOk if fileNameSource contents are identical to sourceCheck
    MustCheck Status bytesCmp(char *fileNameSource, const char *sourceCheck);

    // Below are routines for upgrade to Dionysus. The main difference is that
    // the legacy format had an additional metafile for each UDF, and the UDF
    // python source file had a version number suffixed to the name. The new
    // layout has no metafile (it wasn't necessary) and there's no version
    // number suffixed to the UDF source file.

    // Following routine allows code re-factoring between the legacy,
    // pre-Dionysus code to read and add a Python UDF module, and the new
    // code for Dionysus and beyond.
    MustCheck Status readSourceAndAdd(const char *fileNameSource,
                                      uint64_t moduleVersion,  // for legacy use
                                      UdfType udfType,
                                      XcalarApiUdfContainer *udfContainer,
                                      void *udfMeta,  // for legacy use
                                      char *moduleName);

    MustCheck Status deleteOldUdfFiles(XcalarApiUdfContainer *udfContainer);
    MustCheck Status copyOldSourcesToNew(DIR *udfDir, char *udfDirPath);
    void processUdfsDirUpgrade(DIR *udfDir,
                               char *dirPath,
                               XcalarApiUdfContainer *udfContainer);
    MustCheck Status legacyFileNames(const char *moduleName,
                                     UdfType type,
                                     XcalarApiUdfContainer *udfContainer,
                                     char *fileNameMeta,
                                     size_t *fileNameMetaSize,
                                     char *fileNameSource,
                                     size_t *fileNameSourceSize);
    MustCheck Status legacyAdd(const char *fileNameMeta,
                               XcalarApiUdfContainer *udfContainer);
};

#endif  // _UDFPERSIST_H_
