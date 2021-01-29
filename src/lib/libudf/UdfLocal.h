// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFLOCAL_H_
#define _UDFLOCAL_H_

#include "primitives/Primitives.h"
#include "udf/UdfError.h"
#include "operators/XcalarEvalTypes.h"
#include "stat/StatisticsTypes.h"
#include "LibUdfConstants.h"
#include "udf/UdfTypes.h"
#include "ns/LibNs.h"

struct XcalarApiOutput;
struct XcalarApiUdfGetInput;
struct XcalarApiUdfDeleteInput;

class UdfOperation;
class UdfParent;
class ParentChild;

using namespace udf;

//
// Maintains this usrnode's view of UDF modules.
//

class UdfLocal final
{
  public:
    UdfLocal();
    ~UdfLocal();

    MustCheck Status init(UdfOperation *udfOperation, UdfParent *udfParent);
    void destroy();

    MustCheck Status addUdf(UdfModuleSrc *input,
                            char *modulePrefix,
                            uint64_t consistentVersion);
    MustCheck Status updateUdf(UdfModuleSrc *input,
                               char *modulePrefix,
                               uint64_t consistentVersion,
                               uint64_t nextVersion);
    MustCheck Status getUdf(XcalarApiUdfGetInput *input,
                            char *moduleAbsPath,
                            uint64_t version,
                            XcalarApiOutput **output,
                            size_t *outputSize);
    MustCheck Status deleteUdf(XcalarApiUdfDeleteInput *input,
                               char *modulePrefix,
                               uint64_t consistentVersion,
                               uint64_t nextVersion);
    MustCheck Status validateUdf(const char *moduleName,
                                 const char *source,
                                 size_t sourceSize,
                                 UdfType type);
    MustCheck Status flushNwCacheUdf(UdfNwFlushPath *input);
    MustCheck Status loadModule(ParentChild *child, const char *moduleName);
    MustCheck Status loadModules(ParentChild *child, EvalUdfModuleSet *modules);
    MustCheck Status getModuleNameVersioned(char *moduleName,
                                            uint64_t version,
                                            char *fnName,
                                            char **retModuleNameVersioned);

  private:
    static constexpr const char *ModuleName = "libudf";
    static constexpr size_t UdfModulesSlots = 255;

    struct UdfModule {
        StringHashTableHook hook;
        char moduleFullNameVersioned[LibNsTypes::MaxPathNameLen + 1];
        UdfModuleSrc *input;
        const char *getModuleName() const { return moduleFullNameVersioned; }
        void del();
    };

    typedef StringHashTable<UdfModule,
                            &UdfModule::hook,
                            &UdfModule::getModuleName,
                            UdfModulesSlots,
                            hashStringFast>
        ModuleHashTable;

    UdfParent *udfParent_;
    UdfOperation *udfOperation_;
    ModuleHashTable modules_;
    Mutex modulesHTLock_;
    StatGroupId statGroupId_;
    StatHandle statModuleCount_;

    // Disallow.
    UdfLocal(const UdfLocal &) = delete;
    UdfLocal &operator=(const UdfLocal &) = delete;

    void logError(UdfError *error);
    MustCheck Status populateEval(UdfModule *module,
                                  XcalarEvalFnDesc *functions,
                                  size_t functionCount);
    void deleteUdfInternal(UdfModule *module);
    MustCheck Status deleteUdfByName(char *moduleName);
};

#endif  // _UDFLOCAL_H_
