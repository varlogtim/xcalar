// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SOURCEFORMATCONNECTOR_H_
#define _SOURCEFORMATCONNECTOR_H_

#include "export/DataTarget.h"
#include "df/DataFormat.h"

enum {
    NumTargetHashSlots = 17,
};

class SourceFormatConnector final : public ExportTargetMgrIface
{
  public:
    struct SfTargetDirectory {
        StringHashTableHook hook;
        const char *getName() const { return target.name; };
        ExExportTargetHdr target;
        ExAddTargetSFInput input;
        void del();
    };

    // constants
    static constexpr const char *defaultTargetName = "Default";
    static constexpr const int defaultTargetNameLen = 8;

    static MustCheck Status init();
    static void destroy();

    static SourceFormatConnector &getRef();

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
    typedef StringHashTable<SfTargetDirectory,
                            &SfTargetDirectory::hook,
                            &SfTargetDirectory::getName,
                            NumTargetHashSlots,
                            hashStringFast>
        TargetHashTable;

    SourceFormatConnector(){};
    virtual ~SourceFormatConnector();
    SourceFormatConnector(const SourceFormatConnector &) = delete;
    SourceFormatConnector &operator=(const SourceFormatConnector &) = delete;

    MustCheck Status initInternal();
    // caller owns dir
    MustCheck Status getLowestExistentDir(const char *path, char **directory);

    static constexpr const char *defaultTargetDir = "/export";
    static SourceFormatConnector *sfConnector;
    TargetHashTable targetHashTable;
    Mutex targetHashTableLock;
};

#endif  // _SOURCEFORMATCONNECTOR_H_
