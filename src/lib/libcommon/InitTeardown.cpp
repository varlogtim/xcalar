// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "common/InitTeardown.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "bc/BufferCache.h"
#include "log/Log.h"
#include "stat/Statistics.h"
#include "usrnode/UsrNode.h"
#include "xdb/Xdb.h"
#include "operators/Operators.h"
#include "dataset/Dataset.h"
#include "export/DataTarget.h"
#include "table/ResultSet.h"
#include "util/WorkQueue.h"
#include "operators/XcalarEval.h"
#include "querymanager/QueryManager.h"
#include "scalars/Scalars.h"
#include "dag/DagLib.h"
#include "udf/UserDefinedFunction.h"
#include "util/MemTrack.h"
#include "kvstore/KvStore.h"
#include "usr/Users.h"
#include "optimizer/Optimizer.h"
#include "sys/XLog.h"
#include "util/License.h"
#include "callout/Callout.h"
#include "parent/Parent.h"
#include "usrnode/UsrNodeMonInterface.h"
#include "common/Version.h"
#include "util/Archive.h"
#include "runtime/Runtime.h"
#include "test/FuncTests/FuncTestDriver.h"
#include "runtime/ShimSchedulable.h"
#include "runtime/Spinlock.h"
#include "queryeval/QueryEvaluate.h"
#include "constants/XcalarConfig.h"
#include "queryparser/QueryParser.h"
#include "support/SupportBundle.h"
#include "gvm/Gvm.h"
#include "app/AppMgr.h"
#include "shmsg/SharedMemory.h"
#include "localmsg/LocalMsg.h"
#include "udf/UdfPython.h"
#include "msgstream/MsgStream.h"
#include "cursor/Cursor.h"
#include "demystify/Demystify.h"
#include "ns/LibNs.h"
#include "durable/Durable.h"
#include "transport/TransportPage.h"
#include "child/Child.h"
#include "usr/Users.h"
#include "xdb/HashTree.h"
#include "service/ServiceMgr.h"
#include "util/DFPUtils.h"
#include "util/SaveTrace.h"
#include "libapis/LibApisRecv.h"

static const char logDirPathDefault[XcalarApiMaxPathLen + 1] =
    "/var/log/xcalar";

InitTeardown *InitTeardown::instance = NULL;

InitTeardown::InitTeardown()
    : level_(InitLevel::Invalid),
      loadedConfigFiles_(false),
      dataTargetInitialized_(false)
{
}

InitTeardown::~InitTeardown() {}

Status  // static
InitTeardown::init(InitLevel level,
                   SyslogFacility syslogFacility,
                   const char *configFilePath,
                   const char *pubSigningKeyFilePath,
                   const char *argv0,
                   InitFlags flags,
                   NodeId myNodeId,
                   int numActiveNodes,
                   int numActiveOnPhysical,
                   BufferCacheMgr::Type bcType)
{
    Status status;

    assert(instance == NULL);
    instance = new (std::nothrow) InitTeardown;
    if (instance == NULL) {
        status = StatusNoMem;
        fprintf(stderr,
                "Failed InitTeardown::init: %s\n",
                strGetFromStatus(status));
        return status;
    }
    instance->level_ = level;
    status = instance->initInternal(syslogFacility,
                                    configFilePath,
                                    pubSigningKeyFilePath,
                                    argv0,
                                    flags,
                                    myNodeId,
                                    numActiveNodes,
                                    numActiveOnPhysical,
                                    bcType);
    if (status != StatusOk) {
        fprintf(stderr,
                "Failed InitTeardown::init: %s\n",
                strGetFromStatus(status));
        delete instance;
        instance = NULL;
    }
    return status;
}

void
InitTeardown::teardown()
{
    assert(instance != NULL);
    instance->teardownInternal();
    delete instance;
    instance = NULL;
}

Status
InitTeardown::initInternal(SyslogFacility syslogFacility,
                           const char *configFilePath,
                           const char *pubSigningKeyFilePath,
                           const char *argv0,
                           InitFlags flags,
                           NodeId myNodeId,
                           int numActiveNodes,
                           int numActiveOnPhysical,
                           BufferCacheMgr::Type bcType)
{
    Status status;
    unsigned bcNumLocalNodes = 1;
    size_t bcBufSize = 0;
    const char *logdirPath = NULL;
    XcalarConfig *xcalarConfig = NULL;

    Config *config = NULL;
    assert(level_ >= InitLevel::Cli);

    versionInit();

    status = memTrackInit();
    if (status != StatusOk) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "memTrack initialization failed: %s\n",
                strGetFromStatus(status));
        goto CommonExit;
    }

    srand(time(NULL));

    status = Config::init();
    if (status != StatusOk) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "Config initialization failed: %s\n",
                strGetFromStatus(status));
        goto CommonExit;
    }
    config = Config::get();

    config->setMyNodeId(myNodeId);
    status = config->setNumActiveNodes(numActiveNodes);
    if (status != StatusOk) {
        fprintf(stderr,
                "Config setNumActiveNodes %u failed: %s\n",
                numActiveNodes,
                strGetFromStatus(status));
        goto CommonExit;
    }
    config->setNumActiveNodesOnMyPhysicalNode(numActiveOnPhysical);

    if (level_ >= InitLevel::Config) {
        status = config->loadLogInfoFromConfig(configFilePath);
        if (status != StatusOk) {
            fprintf(stderr,
                    "Error: Module: %s: initInternal failed: "
                    "no log info in '%s': %s\n",
                    ModuleName,
                    configFilePath,
                    strGetFromStatus(status));
            fflush(stderr);
            goto CommonExit;
        }
        logdirPath = config->getLogDirPath();
    }

    // Initialize Xcalar system log (syslog) support.  It would be
    // preferable to do this right at the beginning, but we need the
    // node ID to be set in the usrnode case.
    status = xsyslogInit(syslogFacility, logdirPath);
    if (status != StatusOk) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "syslog initialization(fac=%d,logdir=%s) failed:%s\n",
                syslogFacility,
                logdirPath,
                strGetFromStatus(status));
        fflush(stderr);
        goto CommonExit;
    }

    if (syslogFacility == SyslogFacilityUsrNode) {
        // XXX: when XlogNote has been resolved, convert these to that level
        xSyslog(ModuleName, XlogWarn, "Version: %s", versionGetFullStr());
        xSyslog(ModuleName,
                XlogWarn,
                "ApiVersion: %s",
                versionGetApiStr(versionGetApiSig()));
    }

    status = XcSysHelper::initSysHelper();
    BailIfFailedMsg(ModuleName,
                    status,
                    "sysHelper init failed %s",
                    strGetFromStatus(status));

    status = StatsLib::createSingleton();
    BailIfFailedMsg(ModuleName,
                    status,
                    "stat initialization failed: %s",
                    strGetFromStatus(status));

    status = config->loadBinaryInfo();
    BailIfFailedMsg(ModuleName,
                    status,
                    "Config loadBinaryInfo failed: %s",
                    strGetFromStatus(status));

    config->setNumActiveNodesOnMyPhysicalNode(1);

    if (level_ >= InitLevel::Config) {
        status = config->loadConfigFiles(configFilePath);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed to load config files: %s",
                        strGetFromStatus(status));

        loadedConfigFiles_ = true;
        if (level_ >= InitLevel::UsrNode) {
            // XXX Should be done via node discovery instead
            status = config->loadNodeInfo();
            BailIfFailedMsg(ModuleName,
                            status,
                            "Failed to populate node information. "
                            "Please check your config file.: %s",
                            strGetFromStatus(status));
            bcNumLocalNodes = config->getActiveNodesOnMyPhysicalNode();
            // Check/set if user changed log level in the config file
            setLogLevelViaConfigFile();
        }
    }

    if (!loadedConfigFiles_) {
        // Not loading config file but still need to setup Xcalar "knobs"
        status = XcalarConfig::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Xcalar Config init failed: %s",
                        strGetFromStatus(status));
    }

    status = saveTraceInit(true);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to initialize debug trace infra %s",
                    strGetFromStatus(status));

    struct rlimit rl;
    xcalarConfig = XcalarConfig::get();
    if (getrlimit(RLIMIT_STACK, &rl) != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed getrlimit RLIMIT_STACK: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (rl.rlim_cur < xcalarConfig->thrStackSize_) {
        status = StatusStackSizeTooSmall;
        xSyslog(ModuleName,
                XlogErr,
                "Failed as configured stack size %lu is larger than "
                "getrlimit stack size %lu",
                xcalarConfig->thrStackSize_,
                rl.rlim_cur);
        goto CommonExit;
    }

    status = hashInit();
    BailIfFailedMsg(ModuleName,
                    status,
                    "SSE4.2 CRC32C initialization failed: %s",
                    strGetFromStatus(status));

    status = SharedMemoryMgr::init(level_);
    BailIfFailedMsg(ModuleName,
                    status,
                    "SharedMemoryMgr initialization failed: %s",
                    strGetFromStatus(status));

    bcBufSize = 0;
    if (bcType == BufferCacheMgr::TypeUsrnode) {
        bcBufSize = BufferCacheMgr::computeSize(bcNumLocalNodes);
    }

    status = BufferCacheMgr::init(bcType, bcBufSize);
    if (status == StatusNoMem) {
        BailIfFailedMsg(ModuleName,
                        status,
                        "bc initialization failed: out of resources condition."
                        " Inspect and remove all files in /dev/shm to release"
                        " memory.");
    } else {
        BailIfFailedMsg(ModuleName,
                        status,
                        "bc initialization failed: %s",
                        strGetFromStatus(status));
    }

    status = MemoryPile::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "Memory pile initialization failed: %s",
                    strGetFromStatus(status));

    if (level_ >= InitLevel::UsrNode) {
        status = Gvm::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "GVM initialization failed: %s",
                        strGetFromStatus(status));
    }

    status = CursorManager::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "CursorManager initialization failed: %s",
                    strGetFromStatus(status));

    if (level_ >= InitLevel::Basic) {
        status = Runtime::get()->init(level_);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Runtime initialization failed: %s",
                        strGetFromStatus(status));

        status = CalloutQueue::createSingleton();
        BailIfFailedMsg(ModuleName,
                        status,
                        "callout queue initialization failed: %s",
                        strGetFromStatus(status));

        status = config->loadBinaryInfo();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Config loadBinaryInfo failed: %s",
                        strGetFromStatus(status));
    }

    status = DataFormat::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "Df initialization failed: %s",
                    strGetFromStatus(status));

    status = QueryParser::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "Query parser initialization failed: %s",
                    strGetFromStatus(status));

    status = DFPUtils::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "Decimal floating point initialization failed: %s",
                    strGetFromStatus(status));

    if (level_ >= InitLevel::UsrNode || level_ == InitLevel::Cli) {
        status = archiveInit();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Archive library initialization failed: %s",
                        strGetFromStatus(status));
    }

    if (level_ < InitLevel::Config) {
        status = StatusOk;
        goto CommonExit;
    }

    if (level_ >= InitLevel::UsrNode) {
        status = DemystifyMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Demystify initialization failed: %s",
                        strGetFromStatus(status));

        status = LibNs::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "ns initialization failed: %s",
                        strGetFromStatus(status));

        status = ResultSetMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "result set initialization failed: %s",
                        strGetFromStatus(status));

        status = LicenseMgr::init(pubSigningKeyFilePath);
        BailIfFailedMsg(ModuleName,
                        status,
                        "license initialization failed: %s",
                        strGetFromStatus(status));

        status = usrNodeNew();
        BailIfFailedMsg(ModuleName,
                        status,
                        "libusrnode initialization failed: %s",
                        strGetFromStatus(status));

        // Create new message obj instance. msgNew() internally creates a
        // new buffer cache instance and associate it with message obj. It also
        // creates a new process message instance and associate it with message
        // obj
        status = MsgMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "msg initialization failed: %s",
                        strGetFromStatus(status));

        status = MsgStreamMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "MsgStreamMgr initialization failed: %s",
                        strGetFromStatus(status));

        status = WorkQueueMod::getInstance()->init(level_);
        BailIfFailedMsg(ModuleName,
                        status,
                        "work queue initialization failed: %s",
                        strGetFromStatus(status));

        status = Dataset::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "DS initialization failed: %s",
                        strGetFromStatus(status));
    }

    // Initialize logging
    status = LogLib::createSingleton();
    BailIfFailedMsg(ModuleName,
                    status,
                    "log initialization failed: %s",
                    strGetFromStatus(status));

    status = LibDurable::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "libdurable initialization failed: %s",
                    strGetFromStatus(status));

    if (level_ >= InitLevel::UsrNode) {
        status = XdbMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "XDB initialization failed: %s",
                        strGetFromStatus(status));

        status = TableMgr::init();
        BailIfFailedMsg(ModuleName, status, "Table initialization failed");

        status = HashTreeMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "HashTree initialization failed: %s",
                        strGetFromStatus(status));

        status = TransportPageMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Transport initialization failed: %s",
                        strGetFromStatus(status));

        status = KvStoreLib::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "KvStore initialization failed: %s",
                        strGetFromStatus(status));

        // Create the structures for sessions (must be after logging init)
        status = SessionMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "session initialization failed: %s",
                        strGetFromStatus(status));

        status = UserMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "user initialization failed: %s",
                        strGetFromStatus(status));

        status = QueryManager::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Query manager initialization failed: %s",
                        strGetFromStatus(status));
    }

    if (level_ >= InitLevel::UsrNode) {
        status = DhtMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "DHT initialization failed: %s",
                        strGetFromStatus(status));
    }

    if (level_ >= InitLevel::UsrNodeWithChildNode) {
        status = Parent::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Child node initialization failed: %s",
                        strGetFromStatus(status));
    }

    status = XcalarEval::init();
    BailIfFailedMsg(ModuleName,
                    status,
                    "Xcalar Eval initialization failed: %s",
                    strGetFromStatus(status));

    if (level_ >= InitLevel::UsrNode) {
        status = Operators::operatorsInit(level_);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Operators initialization failed: %s",
                        strGetFromStatus(status));

        status = DagLib::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "DAG initialization failed: %s",
                        strGetFromStatus(status));

        status = Optimizer::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Query optimizer initialization failed: %s",
                        strGetFromStatus(status));

        status = QueryEvaluate::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Query evaluator initialization failed: %s",
                        strGetFromStatus(status));

        status = UserDefinedFunction::init(
            flags & InitFlagSkipRestoreUdfs ? true : false);
        BailIfFailedMsg(ModuleName,
                        status,
                        "UDF initialization failed: %s",
                        strGetFromStatus(status));

        status = DataTargetManager::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Data target initialization failed: %s",
                        strGetFromStatus(status));
        dataTargetInitialized_ = true;

        status = SupportBundle::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Support bundle initialization failed: %s",
                        strGetFromStatus(status));

#ifndef DISABLE_FUNC_TESTS
        status = FuncTestDriver::createSingleton();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Functional test driver initialization failed: %s",
                        strGetFromStatus(status));
#endif

        status = AppMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed to initialize AppMgr: %s",
                        strGetFromStatus(status));

        // Initialize the interface to the cluster monitor.  StatusInval is
        // returned if the monitor port is not specified or invalid.  This is a
        // fatal condition.
        //
        // XXX StatusMonPortInvalid is not a fatal condition until monitor is
        // fully enabled. Currently, the monitorPort is stripped from the config
        // on the cloud. So it'll get StatusMonPortInvalid.  However, in all
        // other environments, even though monitor isn't running,
        // usrNodeMonitorNew() returns StatusOk typically, since the config
        // still continues to have the monitor ports. The monitor listener
        // thread is still created, and it keeps trying to connect to a monitor
        // from the usrnode, every 5 seconds, but this is harmless, and
        // eventually, this will go away when xcmonitor is running. At that
        // point we should make StatusMonPortInvalid a fatal condition.

        status = usrNodeMonitorNew();
        if (status == StatusMonPortInvalid) {
            xSyslog(ModuleName,
                    XlogWarn,
                    "Warning: Monitor port invalid, connection not "
                    "established: %s",
                    strGetFromStatus(status));
            status = StatusOk;
        }
        BailIfFailedMsg(ModuleName,
                        status,
                        "Usrnode/monitor interface intitialization failed: %s",
                        strGetFromStatus(status));

        status = ServiceMgr::init();
        BailIfFailedMsg(ModuleName,
                        status,
                        "Service manager init failed: %s",
                        strGetFromStatus(status));
    }

    if (level_ >= InitLevel::ChildNode) {
        status = LocalMsg::init(level_ >= InitLevel::UsrNode);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Local message init failed: %s",
                        strGetFromStatus(status));
    }

CommonExit:
    // Caller is responsible for calling commonTeardown on failure.
    return status;
}

void *
InitTeardown::earlyTeardown()
{
    Runtime *runtime = Runtime::get();

    if (Child::get() != NULL) {
        xSyslog(ModuleName, XlogInfo, "Child::destroy");
        Child::get()->destroy();
    }

    // We need to stop handling XcRpc requests as soon as we can. At this point,
    // App's have been cancelled
    if (LocalMsg::get() != NULL) {
        xSyslog(ModuleName, XlogInfo, "LocalMsg::stopListening");
        LocalMsg::get()->stopListening();
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
    }

    if (level_ >= InitLevel::UsrNode) {
        // Terminate the connection with the cluster monitor
        {
            // The monitor connection (if one exists) should always be
            // terminated first. For almost everyone, it's practically a no-op.
            // If active, it's still very quick. The idea is to let other nodes
            // as soon as possible that this node is shutting down. The monitor
            // does the communication async. We simply disconnect. So I don't
            // think you need drainAllRunnables() here.
            usrNodeMonitorDestroy();
        }

        if (AppMgr::get() != NULL) {
            xSyslog(ModuleName, XlogInfo, "AppMgr::destroy");
            AppMgr::get()->destroy();
        }

#ifndef DISABLE_FUNC_TESTS
        FuncTestDriver *funcTestDriver = NULL;
        funcTestDriver = FuncTestDriver::get();
        if (funcTestDriver != NULL) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            funcTestDriver->deleteSingleton();
            funcTestDriver = NULL;
        }
#endif

        {
            if (SupportBundle::get()) {
                if (runtime != NULL) {
                    runtime->drainAllRunnables();
                }
                xSyslog(ModuleName, XlogInfo, "SupportBundle::destroy");
                SupportBundle::get()->destroy();
            }
        }

        if (dataTargetInitialized_) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "DataTargetManager::destroy");
            DataTargetManager::destroy();
        }

        // Must be before xcalarEvalDestroy
        {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "UDF::destroy");
            UserDefinedFunction::get()->destroy();
        }

        if (QueryManager::get() != NULL) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "QM::destroy");
            QueryManager::get()->destroy();
        }

        if (QueryEvaluate::get() != NULL) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "QE::destroy");
            QueryEvaluate::get()->destroy();
        }

        if (Optimizer::get() != NULL) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "optimizer::destroy");
            Optimizer::get()->destroy();
        }

        if (DagLib::get() != NULL) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "Daglib::destroy");
            DagLib::get()->destroy();
        }

        {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "Operators::destroy");
            Operators::get()->operatorsDestroy();
        }
    }

    {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        xSyslog(ModuleName, XlogInfo, "XcalarEval::destroy");
        XcalarEval::destroy();
    }

    if (level_ >= InitLevel::UsrNodeWithChildNode) {
        // Must wait to destroyed Parent after all consumers of Parent have been
        // destroyed.
        if (Parent::get()) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "Parent::destroy");
            Parent::get()->destroy();
        }
    }

    if (level_ >= InitLevel::UsrNode) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        xSyslog(ModuleName, XlogInfo, "Dht::destroy");
        DhtMgr::get()->destroy();
    }

    // this gets destroyed in usrnodeRuntimeTeardown for usrnodes, here
    // for everything else
    if (QueryManager::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        xSyslog(ModuleName, XlogInfo, "QM::destroy");
        QueryManager::get()->destroy();
    }

    if (Dataset::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }

        xSyslog(ModuleName, XlogInfo, "Dataset::destroy");
        Dataset::get()->destroy();
    }

    if (DataFormat::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        xSyslog(ModuleName, XlogInfo, "Dataformat::destroy");
        DataFormat::get()->destroy();
    }

    if (level_ >= InitLevel::UsrNode) {
        // Free session hash table other structs, externalize any
        // sessions and DAGs that still need to be written
        {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            if (UserMgr::get() != NULL) {
                xSyslog(ModuleName, XlogInfo, "UserMgr::destroy");
                UserMgr::get()->destroy();
            }

            // XXX: following shouldn't occur since UserMgr's destroy
            // should take care of this...
            if (SessionMgr::get() != NULL) {
                xSyslog(ModuleName, XlogInfo, "SessionMgr::destroy");
                SessionMgr::get()->destroy();
            }
        }

        {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "KVS::destroy");
            KvStoreLib::destroy();
        }

        if (HashTreeMgr::get()) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "HashTreeMgr::destroy");
            HashTreeMgr::get()->destroy();
        }

        if (TableMgr::get()) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "TableMgr::destroy");
            TableMgr::get()->destroy();
        }

        if (TransportPageMgr::get() != NULL) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            TransportPageMgr::get()->destroy();
        }

        // xdbDelete() will wait for all xdb drops before returning.
        if (XdbMgr::get()) {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            xSyslog(ModuleName, XlogInfo, "Xdb::destroy");
            XdbMgr::get()->destroy();
        }
    }

    if (MsgStreamMgr::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        xSyslog(ModuleName, XlogInfo, "MsgStreamMgr::destroy");
        MsgStreamMgr::get()->destroy();
    }

    if (MsgMgr::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        xSyslog(ModuleName, XlogInfo, "MsgMgr::destroy");
        MsgMgr::get()->destroy();
    }

    if (UdfPython::get() != NULL) {
        xSyslog(ModuleName, XlogInfo, "UdfPython::destroy");
        UdfPython::get()->destroy();
    }

    if (LocalMsg::get() != NULL) {
        xSyslog(ModuleName, XlogInfo, "LocalMsg::destroy");
        LocalMsg::get()->destroy();
    }

    return NULL;
}

// As part of the Teardown sequence, we close the Apis Receiver and quiesce the
// Runtime. But this is not sufficient, since the down stream teardown could
// generate more activity on the Runtime.
// XXX Until we figure out how to systematically account and quiesce all
// background activity, we shall drain all Runtime objects prior to each
// sub-system teardown. This way if a dependent sub-system gets task queued up
// on the Runtime, it gets chance to drain outstanding activity prior to it's
// teardown.
// XXX Ideally we should be draining the Runtime as part of a sub-system's
// singleton teardown. But this won't work because the unit tests issue teardown
// from the Runtime itself. So this is going to look ugly.
// Refer Bug#5434  Init/Teardown may need cluster lockstep
void
InitTeardown::teardownInternal()
{
    Runtime *runtime = Runtime::get();

    if (!usrNodeNormalShutdown()) {
        setUsrNodeForceShutdown();
    }

    // Much of teardown cannot happen on main thread.
    if (runtime == NULL) {
        earlyTeardown();
    } else {
        pthread_t teardownThread;
        Status status =
            runtime->createBlockableThread(&teardownThread,
                                           this,
                                           &InitTeardown::earlyTeardown);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogCrit,
                    "Failed to spawn teardown thread: %s. Exiting",
                    strGetFromStatus(status));
            xcalarExit(1);
        }

        verify(sysThreadJoin(teardownThread, NULL) == 0);
    }

    {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        usrNodeDestroy();
    }

    {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        LicenseMgr::get()->destroy();
    }

    if (ResultSetMgr::get()) {
        {
            if (runtime != NULL) {
                runtime->drainAllRunnables();
            }
            ResultSetMgr::get()->destroy();
        }
    }

    {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        archiveDestroy();
    }

    if (LibNs::get()) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        LibNs::get()->destroy();
    }

    if (DemystifyMgr::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        DemystifyMgr::get()->destroy();
    }

    {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        CalloutQueue::deleteSingleton();
    }

    if (WorkQueueMod::getInstance() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        WorkQueueMod::getInstance()->destroy();
    }

    if (CursorManager::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        CursorManager::get()->tearDown();
    }

    if (Gvm::get() != NULL) {
        if (runtime != NULL) {
            runtime->drainAllRunnables();
        }
        Gvm::get()->destroy();
    }

    // After this Runtime is expected to the Empty and will be dismantled.
    if (Runtime::get() != NULL) {
        Runtime::get()->destroy();
        runtime = NULL;
    }

    if (BufferCacheMgr::get()) {
        BufferCacheMgr::get()->destroy();
    }

    if (SharedMemoryMgr::get()) {
        SharedMemoryMgr::get()->destroy();
    }

    saveTraceDestroy();

    if (loadedConfigFiles_) {
        Config::get()->unloadConfigFiles();
    } else {
        if (XcalarConfig::get()) {
            XcalarConfig::get()->destroy();
        }
    }

    if (Config::get()) {
        Config::get()->destroy();
    }

    if (DFPUtils::get()) {
        xSyslog(ModuleName, XlogInfo, "DFPUtils::destroy");
        DFPUtils::get()->destroy();
    }

    if (QueryParser::get() != NULL) {
        QueryParser::get()->destroy();
    }

    if (LibDurable::get() != NULL) {
        LibDurable::get()->destroy();
    }

    // XXX Log shutdown completed before invoking logDelete
    LogLib::deleteSingleton();

    StatsLib::deleteSingleton();

    XcSysHelper::tearDownSysHelper();

    // Uses xSyslog.
    memTrackDestroy(true);

    xSyslog(ModuleName, XlogNote, "All local cleanup done");
}
