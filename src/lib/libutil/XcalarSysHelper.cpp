// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <unistd.h>
#include <new>

#include "sys/XLog.h"
#include "util/System.h"
#include "util/XcalarSysHelper.h"
#include "constants/XcalarConfig.h"

XcSysHelper *XcSysHelper::instance = NULL;

XcSysHelper *
XcSysHelper::get()
{
    assert(instance != NULL);

    return instance;
}

XcSysHelper::XcSysHelper()
{
    physicalMemorySize_ = 0;

    numCores_ = -1;
    numJiffiesPerSecond_ = -1;

    procFsHelper_ = NULL;
}

XcSysHelper::~XcSysHelper()
{
    assert(procFsHelper_ == NULL);
}

Status
XcSysHelper::getSysTotalResourceSizeInBytes(uint64_t *sysTotalResSizeInBytes)
{
    Status status = StatusOk;
    status = procFsHelper_->getSysTotalResourceSize(sysTotalResSizeInBytes);
    BailIfFailed(status);

CommonExit:

    return status;
}

long
XcSysHelper::getNumOnlineCores()
{
    if (!artificialNumCoresLimit) {
        // If an artifical limit is not set, we always want to
        // report the current number of online cores. We will update
        // numCores_ everytime getNumOnlineCores is called.
        numCores_ = sysconf(_SC_NPROCESSORS_ONLN);
    }
    assert(numCores_ != -1);
    return numCores_;
}

long
XcSysHelper::getNumJiffiesPerSecond()
{
    assert(numJiffiesPerSecond_ != -1);

    return numJiffiesPerSecond_;
}

uint64_t
XcSysHelper::getPhysicalMemorySizeInBytes()
{
    return physicalMemorySize_;
}

XcProcFsHelper *
XcSysHelper::getProcFsHelper()
{
    assert(procFsHelper_ != NULL);

    return procFsHelper_;
}

void
XcSysHelper::tearDownSysHelperInternal()
{
    assert(procFsHelper_ != NULL);

    delete procFsHelper_;
    procFsHelper_ = NULL;
}

void
XcSysHelper::tearDownSysHelper()
{
    if (instance == NULL) {
        return;
    }

    instance->tearDownSysHelperInternal();
    delete instance;
    instance = NULL;
}

Status
XcSysHelper::initSysHelperInternal()
{
    Status status = StatusOk;
    char *endptr = NULL;
    long userSetNumCores = -1;
    char *sameHwContainer = NULL;

    numCores_ = sysconf(_SC_NPROCESSORS_ONLN);
    if (numCores_ == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    numJiffiesPerSecond_ = sysconf(_SC_CLK_TCK);
    if (numJiffiesPerSecond_ == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    procFsHelper_ = new (std::nothrow) XcProcFsHelper();
    BailIfNull(procFsHelper_);

    status = procFsHelper_->initProcFsHelper();
    BailIfFailed(status);

    status = procFsHelper_->getTotalMemoryInBytes(&physicalMemorySize_);
    BailIfFailed(status);

    // NUMCPU contains the maximum number of cpus that this cluster node has
    // access to. It should only be set if all cluster nodes exist on the same
    // physical machine (same HW).
    sameHwContainer = getenv("CLUSTER_SAME_PHYSICAL_NODE");
    if (sameHwContainer && strcmp(sameHwContainer, "true") == 0) {
        if (char *env = getenv("NUMCPU")) {
            userSetNumCores = strtoul(env, &endptr, 10);
            if (*endptr != '\0' || userSetNumCores <= 0) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Invalid Number of cores '%s'",
                        env);
            } else {
                if (userSetNumCores > numCores_) {
                    xSyslog(moduleName,
                            XlogWarn,
                            "User supplied number of cores (%ld) exceeds "
                            "actual number of physical cores (%ld)",
                            userSetNumCores,
                            numCores_);
                }
                numCores_ = userSetNumCores;
                artificialNumCoresLimit = true;
                xSyslog(moduleName,
                        XlogInfo,
                        "Number of cores available to this cluster node set to "
                        "user supplied value: %ld",
                        numCores_);
            }
        }
    }

CommonExit:

    if (status != StatusOk) {
        if (procFsHelper_ != NULL) {
            delete procFsHelper_;
            procFsHelper_ = NULL;
        }
    }

    return status;
}

Status
XcSysHelper::initSysHelper()
{
    Status status = StatusOk;

    assert(instance == NULL);

    instance = new (std::nothrow) XcSysHelper();
    BailIfNull(instance);

    status = instance->initSysHelperInternal();
    BailIfFailed(status);

CommonExit:

    if (status != StatusOk) {
        if (instance != NULL) {
            delete instance;
            instance = NULL;
        }
    }

    return status;
}
