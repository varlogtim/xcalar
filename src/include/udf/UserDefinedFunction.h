// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _USERDEFINEDFUNCTION_H_
#define _USERDEFINEDFUNCTION_H_

#include <dirent.h>

#include "primitives/Primitives.h"
#include "msg/MessageTypes.h"
#include "udf/UdfTypes.h"
#include "udf/UdfError.h"
#include "operators/XcalarEvalTypes.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "gvm/Gvm.h"

struct XcalarApiOutput;
struct XcalarApiUdfGetInput;
struct XcalarApiUdfDeleteInput;
struct XcalarApiUdfContainer;
struct XcalarApiUserId;
struct XcalarApiSessionInfoInput;

class UdfLocal;
class UdfPersist;
class UdfOperation;
class UdfParent;
class ParentChild;

//
// Module for managing user defined functions (UDFs). UDFs are used to provide
// extensibility in many different operations. libudf sits in usrnodes and deals
// with propagation and persistence of all UDF types.
//

class UserDefinedFunction final
{
    friend class LibUdfGvm;
    friend class UdfLocal;
    friend class UdfPersist;
    friend class UdfOperation;
    friend class TwoPcMsg2pcAddOrUpdateUdfDlm1;

  public:
    // XXX May be a poor choice. But don't care for now, since it is good
    // enough. However, UdfVersionDelim and UdfRetinaPrefixDelim must be
    // different

    static constexpr const char *UdfVersionDelim = "#";
    static constexpr const char *UdfRetinaPrefixDelim = "$";
    static constexpr const char ModuleDelim = ':';
    static constexpr const char *DefaultModuleName = "default";
    static constexpr const char *CsvParserName = "parseCsv";
    static constexpr const char *JsonParserName = "parseJson";
    static constexpr const char *UdfWorkBookPrefix = "/workbook/";
    static constexpr const char *UdfWorkBookDir = "udf";

    // NOTE!! keep following name in sync with that used by XD for the fake user
    // being used for published dataflows

    static constexpr const char *PublishedDfUserName = ".xcalar.published.df";

    enum UdfPaths { CurDir = 0, Shared, Npaths };
    static constexpr const char *UdfCurrentDirPath = "./";  // Current Session
    // The shared UDF space also includes Xcalar's default module
    static constexpr const char *SharedUDFsDirPath = "/sharedUDFs";
    char *UdfPath[UdfPaths::Npaths];
    static constexpr const char *GlobalUDFDirPathForTests = "/udf";
    // The default user that UDFs get copied to during Artemis to Chronos
    // upgrade. We need this to find the UDF used in an export target given a
    // relative path.
    static constexpr const char *AdminUserName = "admin";
    static constexpr const char *DefaultUDFsPath = "scripts/default.py";

    static MustCheck UserDefinedFunction *get();
    static MustCheck Status init(bool skipRestore);
    void destroy();

    MustCheck Status bulkAddUdf(UdfModuleSrc *input[],
                                uint64_t numModules,
                                XcalarApiUdfContainer *udfContainer,
                                XcalarApiOutput **outputArrayOut[],
                                size_t *outputSizeArrayOut[]);
    MustCheck Status addUdf(UdfModuleSrc *input,
                            XcalarApiUdfContainer *udfContainer,
                            XcalarApiOutput **output,
                            size_t *outputSize);
    MustCheck Status addUdfIgnoreDup(UdfModuleSrc *input,
                                     XcalarApiUdfContainer *udfContainer,
                                     XcalarApiOutput **output,
                                     size_t *outputSize);
    MustCheck Status updateUdf(UdfModuleSrc *input,
                               XcalarApiUdfContainer *udfContainer,
                               XcalarApiOutput **output,
                               size_t *outputSize);
    MustCheck Status getUdf(XcalarApiUdfGetInput *input,
                            XcalarApiUdfContainer *udfContainer,
                            XcalarApiOutput **output,
                            size_t *outputSize);

    // Functions for new style APIs

    // Given a UDF module name, return a fully qualified path to module in the
    // UDF libNs namespace using the current UdfPath[]
    MustCheck Status getResolutionUdf(char *moduleName,
                                      XcalarApiUdfContainer *udfContainer,
                                      char **fullyQName,
                                      size_t *fullyQNameSize);

    MustCheck Status deleteUdf(XcalarApiUdfDeleteInput *input,
                               XcalarApiUdfContainer *udfContainer);
    MustCheck Status addDefaultUdfs();
    MustCheck Status deleteDefaultUdfs();
    MustCheck Status getUdfOnDiskPath(const char *udfLibNsPath,
                                      char **udfOnDiskPath);
    // XXX: upgradeToDionysus flag may not be needed - decision TBD until
    // upgrade milestone
    MustCheck Status unpersistAll(bool upgradeToDionysus);
    MustCheck Status transferModules(ParentChild *child,
                                     EvalUdfModuleSet *modules,
                                     const char *userIdName,
                                     uint64_t sessionId);
    MustCheck static size_t sizeOfAddUpdateOutput(size_t messageSize,
                                                  size_t tracebackSize);
    MustCheck static Status parseFunctionName(char *fullyQualifiedFnName,
                                              const char **moduleName,
                                              const char **moduleVersion,
                                              const char **functionName);
    static MustCheck Status
    initUdfContainer(XcalarApiUdfContainer *udfContainer,
                     XcalarApiUserId *userId,
                     XcalarApiSessionInfoInput *sessionInfo,
                     const char *retinaName);
    static MustCheck Status initUdfContainerFromScope(
        XcalarApiUdfContainer *udfContainer,
        const xcalar::compute::localtypes::Workbook::WorkbookScope *scope);

    MustCheck Status deleteWorkbookDirectory(const char *userName,
                                             const uint64_t sessionId);
    // XXX: upgradeToDionysus flag may not be needed - decision TBD until
    // upgrade milestone
    void processUdfsDir(DIR *udfDir,
                        char *dirPath,
                        size_t maxDirNameLen,
                        XcalarApiUdfContainer *udfContainer,
                        bool upgradeToDionysus);

    // Returns handle opened in Read only mode along with the versioned
    // fullyQualifiedFnName.
    MustCheck Status openUdfHandle(char *fullyQualifiedFnName,
                                   XcalarApiUdfContainer *udfContainer,
                                   LibNsTypes::NsHandle *retNsHandle,
                                   char **retFullyQualFnNameVersioned);
    MustCheck Status closeUdfHandle(char *fullyQualifiedFnName,
                                    LibNsTypes::NsHandle nsHandle);

    MustCheck bool absUDFinEvalStr(const char *evalStr);

    MustCheck Status getUdfName(char *fullyQName,
                                size_t fullyQnameSize,
                                char *moduleName,
                                XcalarApiUdfContainer *udfContainer,
                                bool parse);
    MustCheck Status copyUdfToWorkbook(const char *fullyQName,
                                       XcalarApiUdfContainer *udfContainer);
    // Copy the Udf parser (must be fully qualified name) to the shared UDF
    // space and return the relative name.
    MustCheck Status copyUdfParserToSharedSpace(const char *parserFnName,
                                                char **relativeNameOut);
    MustCheck Status copyUdfExportToSharedSpace(const char *exportFnName);

    // Check if the specified fully qualified UDF module name exists in the
    // namespace.
    MustCheck bool udfModuleExistsInNamespace(const char *fullyQualifiedName);

    // Caller must free memory returned in prefixedUdfName. Note that the
    // routine generates <retinaName>-<udfName> in prefixedUdfName, with
    // the retinaName fixed up if needed (to replace any Udf version delim
    // chars).
    MustCheck Status prefixUdfNameWithRetina(const char *retinaName,
                                             char *udfName,
                                             char **prefixedUdfName);
    static MustCheck Status generateUserWorkbookUdfName(const char *udfName,
                                                        char **newUdfName,
                                                        bool isFunction);
    // Only used by XcUpgrade tool
    void setFinishedRestoring(bool newValue);

    // XXX Move all container helper functions into a different library
    static MustCheck bool containersMatch(
        const XcalarApiUdfContainer *udfContainer1,
        const XcalarApiUdfContainer *udfContainer2);

    static void copyContainers(XcalarApiUdfContainer *dstUdfContainer,
                               const XcalarApiUdfContainer *srcUdfContainer);

    static MustCheck bool containerForDataflows(
        const XcalarApiUdfContainer *udfContainer);

    static MustCheck bool containerWithWbScope(
        const XcalarApiUdfContainer *udfContainer);

  private:
    static constexpr const char *UdfGlobalDataFlowPrefix = "/dataflow/";
    static constexpr uint64_t TimeoutUdfInitUsecs = 60 * USecsPerSec;
    // NwClientFlushSleepInterval * NwClientFlushMaxRetries > 60s since
    // typical max NFS client cache timeout period is 60s
    static constexpr uint64_t NwClientFlushMaxRetries = 10;
    static constexpr uint64_t NwClientFlushSleepInterval = 10;

    class UdfRecord final : public NsObject
    {
      public:
        static constexpr const uint64_t StartVersion = 1;
        static constexpr const uint64_t InvalidVersion = (uint64_t) -1;
        UdfRecord(uint64_t consistentVersion,
                  uint64_t nextVersion,
                  bool isCreated)
            : NsObject(sizeof(UdfRecord)),
              consistentVersion_(consistentVersion),
              nextVersion_(nextVersion),
              isCreated_(isCreated)
        {
        }

        ~UdfRecord() {}

        // Current consistent version number
        uint64_t consistentVersion_ = InvalidVersion;
        // Next available version number
        uint64_t nextVersion_ = InvalidVersion;
        bool isCreated_ = false;

      private:
        UdfRecord(const UdfRecord &) = delete;
        UdfRecord &operator=(const UdfRecord &) = delete;
    };

    static UserDefinedFunction *instance;
    UdfLocal *udfLocal_;
    UdfPersist *udfPersist_;
    UdfOperation *udfOperation_;
    UdfParent *udfParent_;

    UserDefinedFunction();
    ~UserDefinedFunction();

    // Disallow.
    UserDefinedFunction(const UserDefinedFunction &) = delete;
    UserDefinedFunction &operator=(const UserDefinedFunction &) = delete;

    MustCheck Status addUdfInternal(UdfModuleSrc *input,
                                    XcalarApiUdfContainer *udfContainer,
                                    uint64_t curVersion,
                                    XcalarApiOutput **output,
                                    size_t *outputSize);

    MustCheck Status addUpdateOutput(const char *moduleName,
                                     Status operationStatus,
                                     const UdfError *error,
                                     XcalarApiOutput **output,
                                     size_t *outputSize);

    // Wraps validation and staging of new module.
    MustCheck Status stageAndListFunctions(char *moduleNameVersioned,
                                           UdfModuleSrc *input,
                                           UdfError *error,
                                           XcalarEvalFnDesc **functions,
                                           size_t *functionCount);

    // NOTE: legacy flag needed to handle upgrade.
    MustCheck Status deleteUdfInternal(char *moduleName,
                                       XcalarApiUdfContainer *udfContainer,
                                       bool legacy);

    void cleanoutHelper(char *moduleName,
                        uint64_t consistentVersion,
                        uint64_t nextVersion,
                        XcalarApiUdfContainer *udfContainer);

    MustCheck Status nwClientFlushPath(char *udfOdPath,
                                       bool isCreate,
                                       Gvm::Payload **gFlushPayloadInOut);
};

class TwoPcMsg2pcAddOrUpdateUdfDlm1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcAddOrUpdateUdfDlm1() {}
    virtual ~TwoPcMsg2pcAddOrUpdateUdfDlm1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcAddOrUpdateUdfDlm1(const TwoPcMsg2pcAddOrUpdateUdfDlm1 &) =
        delete;
    TwoPcMsg2pcAddOrUpdateUdfDlm1(const TwoPcMsg2pcAddOrUpdateUdfDlm1 &&) =
        delete;
    TwoPcMsg2pcAddOrUpdateUdfDlm1 &operator=(
        const TwoPcMsg2pcAddOrUpdateUdfDlm1 &) = delete;
    TwoPcMsg2pcAddOrUpdateUdfDlm1 &operator=(
        const TwoPcMsg2pcAddOrUpdateUdfDlm1 &&) = delete;
};

#endif  // _USERDEFINEDFUNCTION_H_
