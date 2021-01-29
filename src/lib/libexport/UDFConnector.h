// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFCONNECTOR_H_
#define _UDFCONNECTOR_H_

#include "export/DataTarget.h"
#include "df/DataFormat.h"
#include "ns/LibNsTypes.h"

enum {
    NumUDFTargetHashSlots = 17,
};

class UDFConnector;

class UDFConnector final : public ExportTargetMgrIface
{
  public:
    struct UdfTargetDirectory {
        StringHashTableHook hook;
        const char *getName() const { return target.name; };
        ExExportTargetHdr target;
        ExAddTargetUDFInput input;
        void del();
    };

    static MustCheck Status init();
    static void destroy();

    static MustCheck UDFConnector &getRef();

    // Management functions
    MustCheck Status getDefaultTargets(int *numTargets,
                                       ExExportTarget **targets);
    MustCheck Status shouldPersist(const ExExportTarget *target, bool *persist);
    MustCheck Status verifyTarget(const ExExportTarget *target);
    MustCheck Status addTargetLocal(const ExExportTarget *target);
    MustCheck Status removeTargetLocal(const ExExportTargetHdr *hdr);
    MustCheck int countTargets(const char *namePattern);
    MustCheck Status listTargets(ExExportTarget *targets,
                                 int *ii,
                                 int targetsLength,
                                 const char *pattern);

  private:
    typedef StringHashTable<UdfTargetDirectory,
                            &UdfTargetDirectory::hook,
                            &UdfTargetDirectory::getName,
                            NumUDFTargetHashSlots,
                            hashStringFast>
        TargetHashTable;

    UDFConnector(){};
    virtual ~UDFConnector();
    UDFConnector(const UDFConnector &) = delete;
    UDFConnector &operator=(const UDFConnector &) = delete;

    MustCheck Status initInternal();

    static constexpr const char *defaultTargetDir = "/export";
    static UDFConnector *udfConnector;
    TargetHashTable targetHashTable;
    Mutex targetHashTableLock;
};

#endif  // _UDFCONNECTOR_H_
