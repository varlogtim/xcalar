// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <time.h>
#include <string.h>

#include "StrlFunc.h"
#include "test/FuncTests/AppFuncTests.h"
#include "app/AppMgr.h"
#include "app/App.h"
#include "util/Random.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "LibAppFuncTestConfig.h"
#include "child/Child.h"

#include "test/QA.h"

static const char *moduleName = "libFuncTest::AppFuncTests";
static const char *userIdName = "AppFuncTestsUser";
constexpr AppFuncTests::Params AppFuncTests::paramsSanity;
constexpr AppFuncTests::Params AppFuncTests::paramsStress;
AppFuncTests::Params AppFuncTests::paramsCustom = paramsSanity;

AppFuncTests::AppName AppFuncTests::AppSanity[] = {
    AppFuncTests::AppName::BigOutput,
    AppFuncTests::AppName::Module,
    // XXX
    // Error handling for divide by zero is failing after the 3.6 conversion.
    // Trying to get a stacktrace for this exception throws another exception
    // for some reason
    // AppFuncTests::AppName::Errors,
    AppFuncTests::AppName::Pause,
    AppFuncTests::AppName::Echo,
    AppFuncTests::AppName::StartFailure,
    AppFuncTests::AppName::BarrierSi,
    AppFuncTests::AppName::BarrierSendRecvSi,
    AppFuncTests::AppName::BarrierSendRecvMi,
    AppFuncTests::AppName::BarrierMi,
    AppFuncTests::AppName::BarrierMiFailure,
    AppFuncTests::AppName::XpuSendRecvSiHello,
    AppFuncTests::AppName::XpuSendRecvMiHello,
    AppFuncTests::AppName::XpuSendRecvMiXnodeHello,
    AppFuncTests::AppName::XpuSendRecvListSiHello,
    AppFuncTests::AppName::XpuSendRecvListMiLarge,
    AppFuncTests::AppName::GetUserNameTest,
};

AppFuncTests::AppName AppFuncTests::AppStress[] = {
    AppFuncTests::AppName::BarrierSi,
    AppFuncTests::AppName::BarrierStressSi,
    AppFuncTests::AppName::BarrierStressMi,
    AppFuncTests::AppName::BarrierSendRecvSi,
    AppFuncTests::AppName::BarrierSendRecvMi,
    AppFuncTests::AppName::BarrierMi,
    AppFuncTests::AppName::BarrierSiRand,
    AppFuncTests::AppName::BarrierMiRand,
    AppFuncTests::AppName::BarrierMiFailure,
    AppFuncTests::AppName::XpuSendRecvSiHello,
    AppFuncTests::AppName::XpuSendRecvMiHello,
    AppFuncTests::AppName::XpuSendRecvMiXnodeHello,
    AppFuncTests::AppName::XpuSendRecvSiLarge,
    AppFuncTests::AppName::XpuSendRecvMiLarge,
    AppFuncTests::AppName::XpuSendRecvMiXnodeLarge,
    AppFuncTests::AppName::XpuSendRecvListSiHello,
    AppFuncTests::AppName::XpuSendRecvListMiLarge,
};

AppFuncTests::AppFuncTests(const AppFuncTests::Params &params) : p_(params)
{
    rndInitHandle(&rnd_, time(NULL));
    memZero(apps_, AppTestCaseCount * sizeof(TestApp *));
}

AppFuncTests::~AppFuncTests()
{
    for (uint32_t ii = 0; ii < AppTestCaseCount; ii++) {
        if (apps_[ii] != NULL) {
            delete apps_[ii];
            apps_[ii] = NULL;
        }
    }
}

// Save configured params into global static var for testCustom.
Status  // static
AppFuncTests::parseConfig(Config::Configuration *config,
                          char *key,
                          char *value,
                          bool stringentRules)
{
    if (strcasecmp(key,
                   strGetFromLibAppFuncTestConfig(
                       LibAppFuncTotalThreadCount)) == 0) {
        paramsCustom.totalThreadCount = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibAppFuncTestConfig(
                              LibAppFuncLittleTestIters)) == 0) {
        paramsCustom.littleTestIters = strtoll(value, NULL, 0);
    } else {
        return StatusUsrNodeIncorrectParams;
    }
    return StatusOk;
}

//
// Test entry points.
//

Status  // static
AppFuncTests::testSanity()
{
    return AppFuncTests(paramsSanity).test();
}

Status  // static
AppFuncTests::testStress()
{
    return AppFuncTests(paramsStress).test();
}

Status  // static
AppFuncTests::testCustom()
{
    return AppFuncTests(paramsCustom).test();
}

Status
AppFuncTests::test()
{
    Status status;

    for (uint32_t ii = 0; ii < AppTestCaseCount; ii++) {
        switch ((AppName) ii) {
        case AppName::BigOutput:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) BigOutput());
            break;

        case AppName::Module:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) Module());
            break;

        case AppName::Errors:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) Errors());
            break;

        case AppName::Pause:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) Pause());
            break;

        case AppName::Echo:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) Echo());
            break;

        case AppName::BarrierSi:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) BarrierSi());
            break;

        case AppName::BarrierStressSi:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierStressSi());
            break;

        case AppName::BarrierStressMi:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierStressMi());
            break;

        case AppName::BarrierSendRecvSi:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierSendRecvSi());
            break;

        case AppName::BarrierSendRecvMi:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierSendRecvMi());
            break;

        case AppName::BarrierMi:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow) BarrierMi());
            break;

        case AppName::BarrierSiRand:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierSiRand());
            break;

        case AppName::BarrierMiRand:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierMiRand());
            break;

        case AppName::BarrierMiFailure:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) BarrierMiFailure());
            break;

        case AppName::StartFailure:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) StartFailure());
            break;

        case AppName::XpuSendRecvSiHello:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvSiHello());
            break;

        case AppName::XpuSendRecvMiHello:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvMiHello());
            break;

        case AppName::XpuSendRecvMiXnodeHello:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvMiXnodeHello());
            break;

        case AppName::XpuSendRecvSiLarge:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvSiLarge());
            break;

        case AppName::XpuSendRecvMiLarge:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvMiLarge());
            break;

        case AppName::XpuSendRecvMiXnodeLarge:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvMiXnodeLarge());
            break;

        case AppName::XpuSendRecvListSiHello:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvListSiHello());
            break;

        case AppName::XpuSendRecvListMiLarge:
            apps_[ii] = dynamic_cast<TestApp *>(new (std::nothrow)
                                                    XpuSendRecvListMiLarge());
            break;

        case AppName::GetUserNameTest:
            apps_[ii] =
                dynamic_cast<TestApp *>(new (std::nothrow) GetUserNameTest());
            break;

        default:
            status = StatusUnimpl;
            goto CommonExit;
            break;  // Never reached
        }
        BailIfNull(apps_[ii]);
    }

    status = createTestApps();
    BailIfFailed(status);

    status = createUserThreads(std::string(
                                   "appFunc" +
                                   std::to_string(Config::get()->getMyNodeId()))
                                   .c_str(),
                               p_.totalThreadCount);
    BailIfFailed(status);

    status = joinUserThreads();
    BailIfFailed(status);

CommonExit:
    return StatusOk;
}

//
// Test utilities.
//

char *
TestApp::flattenOutput(const char *outBlob)
{
    char *out;
    size_t outSize = 100;
    json_error_t jsonError;
    json_t *outTotal = json_loads(outBlob, JSON_DECODE_ANY, &jsonError);
    assert(outTotal);

    out = (char *) memAlloc(outSize);
    assert(out);
    out[0] = '\0';

    int numNodes = json_array_size(outTotal);
    for (int ii = 0; ii < numNodes; ii++) {
        json_t *nodeOut = json_array_get(outTotal, ii);
        int numApps = json_array_size(nodeOut);
        for (int jj = 0; jj < numApps; jj++) {
            json_t *outStr = json_array_get(nodeOut, jj);
            const char *thisOut = json_string_value(outStr);
            size_t newLen = snprintf(NULL, 0, "%s%s", out, thisOut);
            if (newLen >= outSize) {
                outSize = newLen * 2;
                out = (char *) memRealloc(out, outSize);
                assert(out);
            }
            verify(strlcat(out, thisOut, outSize) < outSize);
        }
    }

    assert(out);
    json_decref(outTotal);
    outTotal = NULL;
    return out;
}

Status
AppFuncTests::createTestApps()
{
    AppMgr *mgr = AppMgr::get();
    for (size_t ii = 0; ii < ArrayLen(apps_); ii++) {
        Status status = mgr->createApp(apps_[ii]->name_,
                                       apps_[ii]->hostType_,
                                       apps_[ii]->flags_,
                                       (uint8_t *) apps_[ii]->source_,
                                       strlen(apps_[ii]->source_) + 1);
        if (status != StatusOk && status != StatusAppAlreadyExists) {
            return status;
        }
    }

    return StatusOk;
}

//
// Actual test logic.
//

// Called once per thread in p_.totalThreadCount.
Status
AppFuncTests::userMain(FuncTestBase::User *me)
{
    return littleTests(me);
}

// Randomly runs small edge and sanity tests.
Status
AppFuncTests::littleTests(FuncTestBase::User *me)
{
    Status status;
    AppMgr *mgr = AppMgr::get();
    App *app;
    uint8_t *exec;
    size_t execSize;
    uint8_t garbage[1024];
    uint8_t *str = (uint8_t *) "asdf";
    size_t strSize = strlen((char *) str) + 1;
    size_t ii;
    AppName *appNamesToTest = NULL;
    unsigned appNamesCount = 0;

    unsigned nodeId = Config::get()->getMyNodeId();
    LibNsTypes::NsHandle handle;
    char appName[1024];
    char *outBlob = NULL;
    char *errorBlob = NULL;
    snprintf(appName, sizeof(appName), "foo-%u-%d", nodeId, me->index_);

    //
    // Let's try a few basic management operations...
    //

    // ... like create, ...
    mgr->removeApp(appName);
    status = mgr->createApp(appName,
                            App::HostType::Python2,
                            App::FlagNone,
                            str,
                            strSize);
    assert(status == StatusOk);

    // ... get, ...
    app = mgr->openAppHandle("foobar", &handle);
    assert(app == NULL);

    app = mgr->openAppHandle(appName, &handle);
    assert(app != NULL);
    app->getExec((const uint8_t **) &exec, &execSize);
    assert(execSize == strSize);
    assert(strcmp((const char *) str, (const char *) exec) == 0);
    assert(app->getHostType() == App::HostType::Python2);
    mgr->closeAppHandle(app, handle);

    // ... update, ...
    verify(
        mgr->updateApp("bar", App::HostType::Python2, App::FlagNone, NULL, 0) ==
        StatusAppNotFound);
    verify(mgr->updateApp(appName,
                          App::HostType::Invalid,
                          App::FlagNone,
                          str,
                          strSize) == StatusAppHostTypeInvalid);

    verifyOk(mgr->createApp(appName,
                            App::HostType::Python2,
                            App::FlagNone,
                            str,
                            strSize));
    verifyOk(mgr->updateApp(appName,
                            App::HostType::Python2,
                            App::FlagNone,
                            garbage,
                            sizeof(garbage)));
    app = mgr->openAppHandle(appName, &handle);
    assert(app != NULL);
    app->getExec((const uint8_t **) &exec, &execSize);
    assert(execSize == sizeof(garbage));
    assert(memcmp(exec, garbage, sizeof(garbage)) == 0);
    assert(app->getHostType() == App::HostType::Python2);

    // ... and remove.
    mgr->closeAppHandle(app, handle);
    mgr->removeApp(appName);

    snprintf(appName, sizeof(appName), "py-%u-%d", nodeId, me->index_);

    static const char *names[] =
        {"Bristol", "Piper", "Track", "Willow", "Trig", "Flut"};

    for (size_t iter = 0; iter < p_.littleTestIters; iter++) {
        // Used in last few cases.
        const char *name = names[rndGenerate32(&rnd_) % ArrayLen(names)];

        switch ((AppMgmtOpTests)(rndGenerate32(&rnd_) %
                                 (uint32_t) AppMgmtOpTests::AppMgmtOpLast)) {
        case AppMgmtOpTests::AppOpen:
            app = mgr->openAppHandle(name, &handle);
            if (app != NULL) {
                assert(strcmp(name, app->getName()) == 0);
                mgr->closeAppHandle(app, handle);
            }
            break;

        case AppMgmtOpTests::AppCreate:
            status = mgr->createApp(name,
                                    App::HostType::Python2,
                                    App::FlagNone,
                                    (uint8_t *) "",
                                    1);
            assert(status == StatusOk || status == StatusAppAlreadyExists);
            if (status == StatusAppAlreadyExists) {
                status = StatusOk;
            }
            break;

        case AppMgmtOpTests::AppRemove:
            app = mgr->openAppHandle(name, &handle);
            if (app != NULL) {
                bool appInternalError = false;
                verify(AppMgr::get()->runMyApp(app,
                                               AppGroup::Scope::Local,
                                               userIdName,
                                               0,
                                               "asdf",
                                               0,
                                               &outBlob,
                                               &errorBlob,
                                               &appInternalError) != StatusOk);
                mgr->closeAppHandle(app, handle);
            }
            break;

        case AppMgmtOpTests::AppUpdate:
            status = mgr->updateApp(name,
                                    App::HostType::Python2,
                                    App::FlagNone,
                                    (uint8_t *) "",
                                    1);
            assert(status == StatusOk || status == StatusAppNotFound ||
                   status == StatusAppInUse);
            if (status == StatusAppNotFound || status == StatusAppInUse) {
                status = StatusOk;
            }
            break;

        default:
            assert(0);
            break;
        }

        if (outBlob != NULL) {
            memFree(outBlob);
            outBlob = NULL;
        }

        if (errorBlob != NULL) {
            memFree(errorBlob);
            errorBlob = NULL;
        }
        if (status != StatusOk) {
            break;
        }
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    switch (p_.testType) {
    case AppTestType::Sanity:
        appNamesToTest = AppSanity;
        appNamesCount = sizeof(AppSanity) / sizeof(AppName);
        break;
    case AppTestType::Stress:
        appNamesToTest = AppStress;
        appNamesCount = sizeof(AppStress) / sizeof(AppName);
        break;
    default:
        assert(0);
        break;
    }

    for (size_t iter = 0; iter < p_.littleTestIters; iter++) {
        uint32_t testCaseIdx = (uint32_t) rndGenerate32(&rnd_) % appNamesCount;
        AppName testApp = appNamesToTest[testCaseIdx];
        status = apps_[(uint32_t) testApp]->execute();
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

CommonExit:
    // Clear everything out to check for memory leaks.
    for (ii = 0; ii < ArrayLen(names); ii++) {
        mgr->removeApp(names[ii]);
    }
    return status;
}

Status
BigOutput::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBigOutputHandle;
    App *app = mgr->openAppHandle("bigOutput", &appBigOutputHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    // Test output propagation.
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Local,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        assert(out);
        unsigned ii = 0;
        for (ii = 0; out[ii] != '\0'; ii++) {
            assert(out[ii] == 'A');
        }
        assert(ii == 150);
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran local appBigOutput successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run local appBigOutput: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            // Could occur since there's a race in AppFuncTests::littleTests
            // (which runs per-thread, per-node) - if a thread is at the end,
            // deleting apps, while some other thread is still in the executing
            // phase, the app execution may try to run an app which has been
            // deleted. This comment isn't being repeated in other apps'
            // execute routines for brevity
            status = StatusOk;
        }
    }

    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = AppMgr::get()->runMyApp(app,
                                     AppGroup::Scope::Global,
                                     userIdName,
                                     0,
                                     "asdf",
                                     0,
                                     &outBlob,
                                     &errorBlob,
                                     &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        assert(out);
        unsigned ii = 0;
        for (ii = 0; out[ii] != '\0'; ii++) {
            assert(out[ii] == 'A');
        }
        assert(ii == 150 * Config::get()->getActiveNodes());
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran global appBigOutput successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run global appBigOutput: %s",
                strGetFromStatus(status));
    }

    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }

CommonExit:
    mgr->closeAppHandle(app, appBigOutputHandle);
    return status;
}

Status
Module::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appModuleHandle;
    App *app = mgr->openAppHandle("module", &appModuleHandle);
    assert(app != NULL);

    unsigned nodeCount = Config::get()->getActiveNodes();
    char *outBlob = NULL;
    char *errorBlob = NULL;
    // Test distributed use of xcalar Python module.
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        assert(out);
        unsigned ii = 0;
        for (ii = 0; ii < xcMin(nodeCount, (unsigned) 10); ii++) {
            assert(out[ii] == '0' + (char) ii);
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appModule successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appModule: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }

    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appModuleHandle);
    return status;
}

Status
Errors::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appErrorsHandle;
    App *app = mgr->openAppHandle("errors", &appErrorsHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    // Test error reporting.
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusUdfExecuteFailed) {
        char *err = flattenOutput(errorBlob);
        assert(strstr(err, "integer division") != NULL);
        memFree(err);
        xSyslog(moduleName, XlogDebug, "Ran appErrors successfully");
        status = StatusOk;  // deemed a success
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appErrors: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appErrorsHandle);
    return status;
}

Status
Pause::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appPauseHandle;
    App *app = mgr->openAppHandle("pause", &appPauseHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    // Test an App that takes some time to complete (and has no output).
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        assert(strncmp(out, "None", strlen("None")) == 0);
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appPause successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appPause: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    if (status != StatusOk) {
        goto CommonExit;
    }

    AppGroup::Id appGroupId;
    appInternalError = false;
    status = AppMgr::get()->runMyAppAsync(app,
                                          AppGroup::Scope::Global,
                                          userIdName,
                                          0,  // session ID
                                          "",
                                          0,
                                          &appGroupId,
                                          &errorBlob,
                                          &appInternalError);
    if (status == StatusOk) {
        assert(outBlob == NULL);
        assert(errorBlob == NULL);
        status = AppMgr::get()->abortMyAppRun(appGroupId,
                                              StatusNoTTY,
                                              &appInternalError);
        assert(status == StatusOk);
        status = AppMgr::get()->waitForMyAppResult(appGroupId,
                                                   0,
                                                   &outBlob,
                                                   &errorBlob,
                                                   &appInternalError);
        assert(status == StatusNoTTY);
        xSyslog(moduleName, XlogDebug, "Successfully aborted appPause");
        if (status == StatusNoTTY) {
            status = StatusOk;
        }
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to runAsync appPause: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
CommonExit:
    mgr->closeAppHandle(app, appPauseHandle);
    return status;
}

Status
Echo::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appEchoHandle;
    App *app = mgr->openAppHandle("echo", &appEchoHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    // Test an App with input.
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Local,
                                            userIdName,
                                            0,  // session ID
                                            "hello",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        assert(strcmp(out, "hello") == 0);
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appEcho successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run local appEcho: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    appInternalError = false;
    status = AppMgr::get()->runMyApp(app,
                                     AppGroup::Scope::Global,
                                     userIdName,
                                     0,
                                     "h",
                                     0,
                                     &outBlob,
                                     &errorBlob,
                                     &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        assert(out[0] == 'h');
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran global appEcho successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run global appEcho: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
CommonExit:
    mgr->closeAppHandle(app, appEchoHandle);
    return status;
}

Status
BarrierSi::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierSiHandle;
    App *app = mgr->openAppHandle("barrierSi", &appBarrierSiHandle);
    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        int numNodes = Config::get()->getActiveNodes();
        assert((int) strlen(out) == numNodes);
        for (int ii = 0; ii < numNodes; ii++) {
            assert(out[ii] == 'A');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierSi successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierSi: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierSiHandle);
    return status;
}

Status
BarrierStressSi::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierStressSiHandle;
    App *app = mgr->openAppHandle("barrierStressSi", &appBarrierStressSiHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        int numNodes = Config::get()->getActiveNodes();
        assert((int) strlen(out) == numNodes);
        for (int ii = 0; ii < numNodes; ii++) {
            assert(out[ii] == 'A');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierStressSi successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierStressSi: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }

    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierStressSiHandle);
    return status;
}

Status
BarrierSendRecvSi::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierSendRecvSiHandle;
    App *app =
        mgr->openAppHandle("barrierSendRecvSi", &appBarrierSendRecvSiHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        int numNodes = Config::get()->getActiveNodes();
        assert((int) strlen(out) == numNodes);
        for (int ii = 0; ii < numNodes; ii++) {
            assert(out[ii] == 'A');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierSendRecvSi successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierSendRecvSi: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }

    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierSendRecvSiHandle);
    return status;
}

Status
BarrierStressMi::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierStressMiHandle;
    App *app = mgr->openAppHandle("barrierStressMi", &appBarrierStressMiHandle);
    assert(app != NULL);
    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        unsigned numNodes = Config::get()->getActiveNodes();
        unsigned numCores = (unsigned) XcSysHelper::get()->getNumOnlineCores();
        // assume same cores on all nodes
        unsigned xpuClusterSize = numNodes * numCores;
        assert((unsigned) strlen(out) == xpuClusterSize);
        for (unsigned ii = 0; ii < xpuClusterSize; ii++) {
            assert(out[ii] == 'A');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierStressMi successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierStressMi: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierStressMiHandle);
    return status;
}

Status
BarrierSendRecvMi::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierSendRecvMiHandle;
    App *app =
        mgr->openAppHandle("barrierSendRecvMi", &appBarrierSendRecvMiHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        unsigned numNodes = Config::get()->getActiveNodes();
        unsigned numCores = (unsigned) XcSysHelper::get()->getNumOnlineCores();
        // assume same cores on all nodes
        unsigned xpuClusterSize = numNodes * numCores;
        assert((unsigned) strlen(out) == xpuClusterSize);
        for (unsigned ii = 0; ii < xpuClusterSize; ii++) {
            assert(out[ii] == 'A');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierSendRecvMi successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierSendRecvMi: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierSendRecvMiHandle);
    return status;
}

Status
BarrierSiRand::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierSiRandHandle;
    App *app = mgr->openAppHandle("barrierSiRand", &appBarrierSiRandHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        int numNodes = Config::get()->getActiveNodes();
        assert((int) strlen(out) == numNodes);
        for (int ii = 0; ii < numNodes; ii++) {
            assert(out[ii] == 'A');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierSiRand successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierSiRand: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierSiRandHandle);
    return status;
}

Status
BarrierMi::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierMiHandle;
    App *app = mgr->openAppHandle("barrierMi", &appBarrierMiHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        unsigned numNodes = Config::get()->getActiveNodes();
        unsigned numCores = (unsigned) XcSysHelper::get()->getNumOnlineCores();
        // assume same cores on all nodes
        unsigned xpuClusterSize = numNodes * numCores;
        assert((unsigned) strlen(out) == xpuClusterSize);
        for (unsigned ii = 0; ii < xpuClusterSize; ii++) {
            assert(out[ii] == 'B');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierMi successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierMi: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierMiHandle);
    return status;
}

Status
BarrierMiRand::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierMiRandHandle;
    App *app = mgr->openAppHandle("barrierMiRand", &appBarrierMiRandHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        unsigned numNodes = Config::get()->getActiveNodes();
        unsigned numCores = (unsigned) XcSysHelper::get()->getNumOnlineCores();
        // assume same cores on all nodes
        unsigned xpuClusterSize = numNodes * numCores;
        assert((unsigned) strlen(out) == xpuClusterSize);
        for (unsigned ii = 0; ii < xpuClusterSize; ii++) {
            assert(out[ii] == 'B');
        }
        memFree(out);
        xSyslog(moduleName, XlogDebug, "Ran appBarrierMiRand successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appBarrierMiRand: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierMiRandHandle);
    return status;
}

Status
BarrierMiFailure::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appBarrierMiFailureHandle;
    App *app =
        mgr->openAppHandle("barrierMiFailure", &appBarrierMiFailureHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusChildTerminated || status == StatusUdfExecuteFailed) {
        // A chosen XPU either raises an exception (resulting in
        // StatusUdfExecuteFailed) or kills itself with a -9 signal (resulting
        // in StatusChildTerminated). So these are expected failure modes.
        xSyslog(moduleName,
                XlogDebug,
                "appBarrierMiFailure failed as expected %s",
                strGetFromStatus(status));
        status = StatusOk;
    } else if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "appBarrierMiFailure failed in an unexpected manner %s",
                strGetFromStatus(status));
    } else {
        xSyslog(moduleName,
                XlogErr,
                "appBarrierMiFailure unexpectedly passed!");
        status = StatusFailed;
        assert(0);
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appBarrierMiFailureHandle);
    return status;
}

Status
StartFailure::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appStartFailureHandle;
    App *app = mgr->openAppHandle("startFailure", &appStartFailureHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "asdf",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusUdfModuleLoadFailed) {
        char *err = flattenOutput(errorBlob);
        assert(strstr(err, "No module named 'notamodule'") != NULL);
        memFree(err);
        xSyslog(moduleName, XlogDebug, "Successfully failed to run notamodule");
        status = StatusOk;
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appStartFailure: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appStartFailureHandle);
    return status;
}

Status
XpuSendRecvSiHello::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvSiHelloHandle;
    App *app =
        mgr->openAppHandle("xpuSendRecvSiHello", &appXpuSendRecvSiHelloHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvSiHello successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvSiHello: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvSiHelloHandle);
    return status;
}

Status
XpuSendRecvListSiHello::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvListSiHelloHandle;
    App *app = mgr->openAppHandle("xpuSendRecvListSiHello",
                                  &appXpuSendRecvListSiHelloHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvListSiHello successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvListSiHello: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvListSiHelloHandle);
    return status;
}

Status
XpuSendRecvListMiLarge::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvListMiLargeHandle;
    App *app = mgr->openAppHandle("xpuSendRecvListMiLarge",
                                  &appXpuSendRecvListMiLargeHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvListMiLarge successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvListMiLarge: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvListMiLargeHandle);
    return status;
}

Status
XpuSendRecvMiHello::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvMiHelloHandle;
    App *app =
        mgr->openAppHandle("xpuSendRecvMiHello", &appXpuSendRecvMiHelloHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvMiHello successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvMiHello: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvMiHelloHandle);
    return status;
}

Status
XpuSendRecvMiXnodeHello::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvMiXnodeHelloHandle;
    App *app = mgr->openAppHandle("xpuSendRecvMiXnodeHello",
                                  &appXpuSendRecvMiXnodeHelloHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvMiXnodeHello successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvMiXnodeHello: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvMiXnodeHelloHandle);
    return status;
}

Status
XpuSendRecvSiLarge::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvSiLargeHandle;
    App *app =
        mgr->openAppHandle("xpuSendRecvSiLarge", &appXpuSendRecvSiLargeHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvSiLarge successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvSiLarge: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvSiLargeHandle);
    return status;
}

Status
XpuSendRecvMiLarge::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvMiLargeHandle;
    App *app =
        mgr->openAppHandle("xpuSendRecvMiLarge", &appXpuSendRecvMiLargeHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvMiLarge successfully %s",
                outBlob);
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvMiLarge: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvMiLargeHandle);
    return status;
}

Status
GetUserNameTest::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appGetUserNameTestHandle;
    App *app = mgr->openAppHandle("getUserNameTest", &appGetUserNameTestHandle);
    assert(app != NULL);
    size_t unameSize;
    const char *userIdName = "GetUserNameTest";

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    unameSize = strlen(userIdName);
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        char *out = flattenOutput(outBlob);
        int numNodes = Config::get()->getActiveNodes();
        assert((size_t) strlen(out) == numNodes * unameSize);
        for (int ii = 0; ii < numNodes; ii++) {
            assert(!strncmp(userIdName, out, unameSize));
            out += unameSize;
        }
        xSyslog(moduleName,
                XlogDebug,
                "Ran appGetUserNameTest successfully %s",
                outBlob);
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appGetUserNameTest: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appGetUserNameTestHandle);
    return status;
}

Status
XpuSendRecvMiXnodeLarge::execute()
{
    AppMgr *mgr = AppMgr::get();
    LibNsTypes::NsHandle appXpuSendRecvMiXnodeLargeHandle;
    App *app = mgr->openAppHandle("xpuSendRecvMiXnodeLarge",
                                  &appXpuSendRecvMiXnodeLargeHandle);
    assert(app != NULL);

    char *outBlob = NULL;
    char *errorBlob = NULL;
    bool appInternalError = false;
    Status status = AppMgr::get()->runMyApp(app,
                                            AppGroup::Scope::Global,
                                            userIdName,
                                            0,
                                            "",
                                            0,
                                            &outBlob,
                                            &errorBlob,
                                            &appInternalError);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Ran appXpuSendRecvMiXnodeLarge successfully");
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Failed to run appXpuSendRecvMiXnodeLarge: %s",
                strGetFromStatus(status));
        if (status == StatusNoEnt) {
            status = StatusOk;
        }
    }
    if (outBlob != NULL) {
        memFree(outBlob);
        outBlob = NULL;
    }

    if (errorBlob != NULL) {
        memFree(errorBlob);
        errorBlob = NULL;
    }
    mgr->closeAppHandle(app, appXpuSendRecvMiXnodeLargeHandle);
    return status;
}
