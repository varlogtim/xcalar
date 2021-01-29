// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPMGR_H
#define APPMGR_H

#include "primitives/Primitives.h"
#include "app/App.h"
#include "app/AppInstance.h"
#include "app/AppGroup.h"
#include "runtime/Spinlock.h"
#include "ns/LibNsTypes.h"

class AppGroupMgr;
class AppStats;
class XpuCommObjectAction;

//
// Global, persistent registry of available Apps.
//

class AppMgr final
{
    friend class AppGroupMgr;
    friend class AppMgrGvm;
    friend class AppGroup;
    friend class AppGroupMgrGvm;
    friend class AppInstance;
    friend class XpuCommObjectAction;

  public:
    static constexpr const char *TargetAppName = "XcalarTargetMgr";
    static constexpr const char *DriverAppName = "XcalarDriverMgr";
    static constexpr const char *ExportHostAppName = "XcalarExportHost";
    // XXX(dwillis) make this user parameterizable to replace 'udf on load'
    static constexpr const char *LoadAppName = "XcalarLoad";
    static constexpr const char *SingleFileLoadAppName = "XcalarSingleFileLoad";
    static constexpr const char *ListPreviewAppName = "XcalarListPreview";
    static constexpr const char *FileDeleteAppName = "XcalarFileDelete";
    static constexpr const char *SchemaLoadAppName = "XcalarSchemaLoad";
    static constexpr const char *ListFilesAppName = "XcalarListFiles";
    static constexpr const char *CgroupsAppName = "CgroupsAppMgr";
    static constexpr const char *VerifyAppName = "XcalarVerify";
    static constexpr const char *QueryToDF2UpgradeAppName = "XcalarQueryToDF2";
    static constexpr const char *CepdAppName = "XcalarCepd";
    static constexpr const char *WriteFileAllNodesAppName = "WriteFileAllNodes";
    static constexpr const char *DependsAppName = "XcalarDepends";
    static constexpr const char *MemAppName = "Mem";
    static constexpr const char *ScheduleCreateAppName = "ScheduleCreate";
    static constexpr const char *ScheduleDeleteAppName = "ScheduleDelete";
    static constexpr const char *ScheduleUpdateAppName = "ScheduleUpdate";
    static constexpr const char *ScheduleListAppName = "ScheduleList";
    static constexpr const char *ScheduleClearAppName = "ScheduleClear";
    static constexpr const char *SchedulePauseAppName = "SchedulePause";
    static constexpr const char *ScheduleResumeAppName = "ScheduleResume";
    static constexpr const char *VisualParserFormatterAppName =
        "VisualParserFormatter";
    static constexpr const char *VisualParserJsonAppName = "VisualParserJson";
    static constexpr const char *VisualParserXmlAppName = "VisualParserXml";
    static constexpr const char *XdbSerDesAppName = "XdbSerDes";
    static constexpr const char *ParquetAppName = "XcalarParquet";
    static constexpr const char *WriteToFileDataflowStatsAppName =
        "writeToFileDataflowStats";
    static constexpr const char *SystemStatsAppName = "SystemStats";
    static constexpr const char *SystemRuntimePerfAppName = "SystemPerf";
    static constexpr const char *GrpcServerAppName = "GrpcServer";
    static constexpr const char *ControllerAppName = "ControllerApp";
    static constexpr const char *NotebookAppName = "NotebookApp";

    // For the special case of app execution where we are executing with
    // scope local, and with an app that doCfailing if the above assumptions
    // are broken. Caller must memFree *onlyAppOut
    static Status extractSingleAppOut(const char *appOutBlob,
                                      char **onlyAppOut);

    static MustCheck Status init();

    Status tryAbortAllApps(Status abortReason);
    void destroy();

    MustCheck static AppMgr *get() { return instance; }

    bool isAppAlive(AppGroup::Id appGroupId);

    // Running an App Synchronously.
    MustCheck Status runMyApp(App *app,
                              AppGroup::Scope scope,
                              const char *userIdName,
                              uint64_t sessionId,
                              const char *inBlob,
                              uint64_t cookie,
                              char **outBlobOut,
                              char **errorBlobOut,
                              bool *retAppInternalError);

    // Running an App Asynchronously.
    MustCheck Status runMyAppAsync(App *app,
                                   AppGroup::Scope scope,
                                   const char *userIdName,
                                   uint64_t sessionId,
                                   const char *inBlob,
                                   uint64_t cookie,
                                   AppGroup::Id *retAppGroupId,
                                   char **errorBlobOut,
                                   bool *retAppInternalError);

    // Collect result for Asynchronously run App.
    MustCheck Status waitForMyAppResult(AppGroup::Id appGroupId,
                                        uint64_t usecsTimeout,
                                        char **outBlobOut,
                                        char **errorBlobOut,
                                        bool *retAppInternalError);

    // Abort a running App.
    MustCheck Status abortMyAppRun(AppGroup::Id appGroupId,
                                   Status abortReason,
                                   bool *retAppInternalError);

    MustCheck Status abortMyAppRun(AppGroup::Id appGroupId,
                                   Status abortReason,
                                   bool *retAppInternalError,
                                   bool reappFlag);

    // App Management, creates.
    MustCheck Status createApp(const char *name,
                               App::HostType hostType,
                               uint32_t flags,
                               const uint8_t *exec,
                               size_t execSize);

    // App Management, updates.
    MustCheck Status updateApp(const char *name,
                               App::HostType hostType,
                               uint32_t flags,
                               const uint8_t *exec,
                               size_t execSize);

    // App Management, removes.
    void removeApp(const char *name);

    // App access control, open.
    MustCheck App *openAppHandle(const char *name,
                                 LibNsTypes::NsHandle *retHandle);

    // App access control, close.
    void closeAppHandle(App *app, LibNsTypes::NsHandle handle);

    // Add build in Apps during bootstrapping.
    MustCheck Status addBuildInApps();

    // Remove built in Apps during teardown.
    void removeBuiltInApps();

    // Restore all user created Apps during bootstrapping.
    MustCheck Status restoreAll();

    void onRequestMsg(LocalMsgRequestHandler *reqHandler,
                      LocalConnection *connection,
                      const ProtoRequestMsg *request,
                      ProtoResponseMsg *response);

    AppStats *getStats() { return stats_; }

    MustCheck bool builtInApp(const char *appName);

    MustCheck Status setupMsgStreams();
    void teardownMsgStreams();

  private:
    static constexpr const char *ModuleName = "AppMgr";
    static constexpr const char *AppPrefix = "/app/";

    class AppRecord final : public NsObject
    {
      public:
        AppRecord(bool isCreated)
            : NsObject(sizeof(AppRecord)), isCreated_(isCreated)
        {
        }
        ~AppRecord() {}
        bool isCreated_ = false;

      private:
        AppRecord(const AppRecord &) = delete;
        AppRecord &operator=(const AppRecord &) = delete;
    };

    struct BuiltInApp {
        const char *appName;
        const char *appPath;
        uint32_t flags;
    };

    static constexpr const uint64_t AppAbortTimeoutDuringShutdownUsecs =
        USecsPerSec;

    // For now, mark build in Apps as App::FlagSysLevel, if it needs to be
    // scheduled differently from user parameterizable built in Apps.
    static constexpr const BuiltInApp BuiltInApps[] =
        {{TargetAppName,
          "scripts/targetMgr.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {DriverAppName,
          "scripts/driverMgr.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ExportHostAppName,
          "scripts/exportHost.py",
          // Export driver code can be user written.
          App::FlagBuiltIn},
         {LoadAppName,
          "scripts/load.py",
          // Load UDF may not be the one available in default.py
          App::FlagBuiltIn | App::FlagImport},
         {SingleFileLoadAppName,
          "scripts/load.py",
          // Load UDF may not be the one available in default.py
          App::FlagBuiltIn | App::FlagImport | App::FlagInstancePerNode},
         {ListPreviewAppName,  // Used for List files as well as Preview
          "scripts/load.py",
          App::FlagBuiltIn | App::FlagImport | App::FlagInstancePerNode},
         {FileDeleteAppName,
          "scripts/load.py",
          App::FlagBuiltIn | App::FlagImport | App::FlagInstancePerNode},
         {SchemaLoadAppName,
          "scripts/schema_discover_load.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {ListFilesAppName,
          "scripts/list_files_app.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {CgroupsAppName,
          "scripts/cgroupsMgr.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {VerifyAppName,
          "scripts/verify.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {CepdAppName,
          "scripts/cepudf.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {WriteFileAllNodesAppName,
          "scripts/writeFileAllNodes.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {DependsAppName,
          "scripts/Depends.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ScheduleCreateAppName,
          "scripts/scheduleRetinas/apps/createSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ScheduleDeleteAppName,
          "scripts/scheduleRetinas/apps/deleteSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ScheduleUpdateAppName,
          "scripts/scheduleRetinas/apps/updateSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ScheduleListAppName,
          "scripts/scheduleRetinas/apps/listSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ScheduleClearAppName,
          "scripts/scheduleRetinas/apps/clearSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {SchedulePauseAppName,
          "/scripts/scheduleRetinas/apps/pauseSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ScheduleResumeAppName,
          "/scripts/scheduleRetinas/apps/resumeSchedule.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {VisualParserFormatterAppName,
          "scripts/vpFormatter.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {VisualParserJsonAppName,
          "scripts/vpJsonSudfGen.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {VisualParserXmlAppName,
          "scripts/vpXmlSudfGen.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {XdbSerDesAppName,
          "scripts/XdbSerDes.py",
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {ParquetAppName,
          "scripts/parquetApp.py",
          // Parquet App is parameterizable by the user, so is not a system
          // level App.
          App::FlagBuiltIn | App::FlagInstancePerNode},
         {QueryToDF2UpgradeAppName,
          "scripts/queryToDF2Upgrade.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {WriteToFileDataflowStatsAppName,
          "scripts/dataflow_trace.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {SystemStatsAppName,
          "scripts/system_stats_app.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {GrpcServerAppName,
          "scripts/grpc_server_app.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {SystemRuntimePerfAppName,
          "scripts/system_perf_app.py",
          App::FlagBuiltIn | App::FlagInstancePerNode | App::FlagSysLevel},
         {ControllerAppName,
          "scripts/controller_app.py",
          App::FlagBuiltIn | App::FlagSysLevel},
         {NotebookAppName,
          "scripts/notebook_app.py",
          App::FlagBuiltIn | App::FlagSysLevel}};

    // Singleton instance of AppMgr.
    static AppMgr *instance;
    AppGroupMgr *appGroupMgr_;
    AppStats *stats_;
    App::MgrHashTable apps_;
    mutable Mutex appsLock_;
    AppInstance::AppMgrHashTable appInstances_;
    mutable Mutex appInstancesLock_;

    MustCheck App *findApp(const char *appName)
    {
        appsLock_.lock();
        App *app = apps_.find(appName);
        appsLock_.unlock();
        return app;
    }

    void removeAppInternal(const char *name, LibNsTypes::NsHandle appHandle);

    // GVM handlers for App Management.
    MustCheck Status createLocalHandler(const void *payload);
    MustCheck Status updateLocalHandler(const void *payload);
    void removeLocalHandler(const void *payload);

    // AppInstance tracking.
    void registerAppInstance(AppInstance *instance);
    void deregisterAppInstance(ParentChild::Id id);

    AppMgr();
    virtual ~AppMgr();

    // Disallow.
    AppMgr(const AppMgr &) = delete;
    AppMgr &operator=(const AppMgr &) = delete;
};

#endif  // APPMGR_H
