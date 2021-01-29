// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "udf/UserDefinedFunction.h"
#include "udf/UdfError.h"
#include "common/InitTeardown.h"
#include "runtime/Tls.h"
#include "libapis/LibApisCommon.h"

#include "test/QA.h"  // Must be last

static Status udfTestSanity();
static Status udfTestUdfErrorSerialization();

static TestCase testCases[] = {
    {"libudf: sanity", udfTestSanity, TestCaseEnable, ""},
};

struct UdfModule {
    UdfType type;
    const char *moduleName;
    size_t sourceSize;
    const char *source;
};

#define makeUdfModule(udfType, name, src)                               \
    {                                                                   \
        .type = udfType, .moduleName = name, .sourceSize = sizeof(src), \
        .source = src                                                   \
    }

UdfModule udfModules[] = {
    makeUdfModule(UdfTypePython,
                  "HelloWorld",
                  "def helloWorld():\n"
                  "\treturn \"Hello World\""),
    makeUdfModule(UdfTypePython,
                  "GoodbyeWorld",
                  "def goodbyeWorld():\n"
                  "\treturn \"Goodbye world\""),
};

static Status
udfTestSanity()
{
    Status status = StatusOk;
    UdfModuleSrc *addUdfInput;
    XcalarApiOutput *output = NULL;
    size_t outputSize;

    assertStatic(ArrayLen(udfModules) >= 2);
    addUdfInput = (UdfModuleSrc *) memAlloc(sizeof(*addUdfInput) +
                                            udfModules[0].sourceSize);
    assert(addUdfInput != NULL);
    memZero(addUdfInput, sizeof(addUdfInput) + udfModules[0].sourceSize);

    addUdfInput->isBuiltin = false;
    addUdfInput->modulePath[0] = '\0';
    addUdfInput->type = udfModules[0].type;
    verify(strlcpy(addUdfInput->moduleName,
                   udfModules[0].moduleName,
                   sizeof(addUdfInput->moduleName)) <
           sizeof(addUdfInput->moduleName));
    addUdfInput->sourceSize = udfModules[0].sourceSize;
    memcpy(addUdfInput->source, udfModules[0].source, addUdfInput->sourceSize);

    // Add hello world program
    status = UserDefinedFunction::get()->addUdf(addUdfInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusOk);
    assert(output != NULL);
    assert(output->outputResult.udfAddUpdateOutput.status == StatusOk.code());

    memFree(output);

    // Try to add it again. Should fail
    status = UserDefinedFunction::get()->addUdf(addUdfInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusUdfModuleAlreadyExists);
    assert(output != NULL);
    assert(output->outputResult.udfAddUpdateOutput.status ==
           StatusUdfModuleAlreadyExists.code());

    memFree(output);

    XcalarApiUdfGetInput udfGetInput;
    UdfModuleSrc *getUdfOutput;

    strlcpy(udfGetInput.moduleName,
            addUdfInput->moduleName,
            sizeof(udfGetInput.moduleName));

    // Check to make sure we can retrieve our hello world program
    status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusOk);

    getUdfOutput = &output->outputResult.udfGetOutput;
    assert(getUdfOutput->sourceSize == addUdfInput->sourceSize);
    assert(strcmp(getUdfOutput->moduleName, addUdfInput->moduleName) == 0);
    assert(memcmp(getUdfOutput->source,
                  addUdfInput->source,
                  getUdfOutput->sourceSize) == 0);

    memFree(output);
    getUdfOutput = NULL;
    output = NULL;

    UdfModule modifiedModule = makeUdfModule(UdfTypePython,
                                             udfModules[0].moduleName,
                                             "def helloWorld():\n"
                                             "\treturn \"Hello again world!\"");

    UdfModuleSrc *updateUdfInput = (UdfModuleSrc *) memAlloc(
        sizeof(*updateUdfInput) + modifiedModule.sourceSize);
    assert(updateUdfInput != NULL);
    memZero(updateUdfInput,
            sizeof(*updateUdfInput) + modifiedModule.sourceSize);

    // Note that this is deliberately the wrong moduleName
    verify(strcmp(addUdfInput->moduleName, udfModules[1].moduleName) != 0);
    verify(strlcpy(updateUdfInput->moduleName,
                   udfModules[1].moduleName,
                   sizeof(updateUdfInput->moduleName)) <
           sizeof(updateUdfInput->moduleName));
    updateUdfInput->type = modifiedModule.type;
    updateUdfInput->isBuiltin = false;
    updateUdfInput->modulePath[0] = '\0';
    updateUdfInput->sourceSize = modifiedModule.sourceSize;
    memcpy(updateUdfInput->source,
           modifiedModule.source,
           updateUdfInput->sourceSize);

    // Ensure we cannot update a non-existent module
    status = UserDefinedFunction::get()->updateUdf(updateUdfInput,
                                                   NULL,
                                                   &output,
                                                   &outputSize);
    assert(status == StatusUdfModuleNotFound);
    assert(output != NULL);
    assert(output->outputResult.udfAddUpdateOutput.status ==
           StatusUdfModuleNotFound.code());

    memFree(output);

    // Now let's update the correct one, but with some errors to test rollback
    memFree(updateUdfInput);
    const char badSource[] = "def foo: return 6";
    assert(sizeof(badSource) <= modifiedModule.sourceSize);
    updateUdfInput =
        (UdfModuleSrc *) memAlloc(sizeof(*updateUdfInput) + sizeof(badSource));
    assert(updateUdfInput != NULL);
    memZero(updateUdfInput, sizeof(*updateUdfInput) + sizeof(badSource));
    updateUdfInput->type = UdfTypePython;
    updateUdfInput->isBuiltin = false;
    updateUdfInput->modulePath[0] = '\0';
    updateUdfInput->sourceSize = sizeof(badSource);
    verify(strlcpy(updateUdfInput->moduleName,
                   addUdfInput->moduleName,
                   sizeof(updateUdfInput->moduleName)) <
           sizeof(updateUdfInput->moduleName));
    memcpy(updateUdfInput->source, badSource, sizeof(badSource));

    // This should fail
    status = UserDefinedFunction::get()->updateUdf(updateUdfInput,
                                                   NULL,
                                                   &output,
                                                   &outputSize);
    assert(status == StatusUdfModuleLoadFailed);
    assert(output != NULL);
    assert(output->outputResult.udfAddUpdateOutput.status ==
           StatusUdfModuleLoadFailed.code());

    memFree(output);

    // Check to make sure we still have access to our old udf
    status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusOk);

    getUdfOutput = &output->outputResult.udfGetOutput;
    assert(getUdfOutput->sourceSize == addUdfInput->sourceSize);
    assert(strcmp(getUdfOutput->moduleName, addUdfInput->moduleName) == 0);
    assert(memcmp(getUdfOutput->source,
                  addUdfInput->source,
                  getUdfOutput->sourceSize) == 0);

    memFree(output);
    getUdfOutput = NULL;

    // Now we're going to test our update for real
    memFree(updateUdfInput);
    updateUdfInput = (UdfModuleSrc *) memAlloc(sizeof(*updateUdfInput) +
                                               modifiedModule.sourceSize);
    assert(updateUdfInput != NULL);
    memZero(updateUdfInput,
            sizeof(*updateUdfInput) + modifiedModule.sourceSize);
    updateUdfInput->type = UdfTypePython;
    updateUdfInput->isBuiltin = false;
    updateUdfInput->modulePath[0] = '\0';
    updateUdfInput->sourceSize = modifiedModule.sourceSize;
    verify(strlcpy(updateUdfInput->moduleName,
                   addUdfInput->moduleName,
                   sizeof(updateUdfInput->moduleName)) <
           sizeof(updateUdfInput->moduleName));
    memcpy(updateUdfInput->source,
           modifiedModule.source,
           updateUdfInput->sourceSize);
    status = UserDefinedFunction::get()->updateUdf(updateUdfInput,
                                                   NULL,
                                                   &output,
                                                   &outputSize);
    assert(status == StatusOk);
    assert(output != NULL);
    assert(output->outputResult.udfAddUpdateOutput.status == StatusOk.code());

    memFree(output);

    // Check to make sure we can access the updated module
    status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusOk);

    getUdfOutput = &output->outputResult.udfGetOutput;
    assert(getUdfOutput->sourceSize == updateUdfInput->sourceSize);
    assert(strcmp(getUdfOutput->moduleName, updateUdfInput->moduleName) == 0);
    assert(memcmp(getUdfOutput->source,
                  updateUdfInput->source,
                  getUdfOutput->sourceSize) == 0);

    memFree(output);
    getUdfOutput = NULL;

    // And now we're going to try to bulkAdd without overwriting existing module
    UdfModuleSrc *addUdfInput2;

    addUdfInput2 = (UdfModuleSrc *) memAlloc(sizeof(*addUdfInput2) +
                                             udfModules[1].sourceSize);
    assert(addUdfInput2 != NULL);
    memZero(addUdfInput2, sizeof(*addUdfInput2) + udfModules[1].sourceSize);
    addUdfInput2->type = udfModules[1].type;
    addUdfInput2->isBuiltin = false;
    updateUdfInput->modulePath[0] = '\0';
    verify(strlcpy(addUdfInput2->moduleName,
                   udfModules[1].moduleName,
                   sizeof(addUdfInput2->moduleName)) <
           sizeof(addUdfInput2->moduleName));
    addUdfInput2->sourceSize = udfModules[1].sourceSize;
    memcpy(addUdfInput2->source,
           udfModules[1].source,
           addUdfInput2->sourceSize);

    UdfModuleSrc *bulkAddUdfInput[] = {addUdfInput, addUdfInput2};
    XcalarApiOutput **outputArray = NULL;
    size_t *outputSizeArray = NULL;

    // Try bulk add now; first module update should fail since an updated
    // module 1 already exists. But second module doesn't and so should succeed
    status = UserDefinedFunction::get()->bulkAddUdf(bulkAddUdfInput,
                                                    ArrayLen(bulkAddUdfInput),
                                                    NULL,
                                                    &outputArray,
                                                    &outputSizeArray);
    // Module 1 should fail
    assert(outputArray[0]->outputResult.udfAddUpdateOutput.status ==
           StatusUdfModuleAlreadyExists.code());
    // Module 2 should succeed
    assert(outputArray[1]->outputResult.udfAddUpdateOutput.status ==
           StatusOk.code());
    memFree(outputArray[0]);
    memFree(outputArray[1]);
    memFree(outputArray);
    memFree(outputSizeArray);

    // Get the contents of module 1. Should be left untouched
    strlcpy(udfGetInput.moduleName,
            addUdfInput->moduleName,
            sizeof(udfGetInput.moduleName));
    status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusOk);

    getUdfOutput = &output->outputResult.udfGetOutput;
    assert(getUdfOutput->sourceSize == updateUdfInput->sourceSize);
    assert(strcmp(getUdfOutput->moduleName, updateUdfInput->moduleName) == 0);
    assert(memcmp(getUdfOutput->source,
                  updateUdfInput->source,
                  getUdfOutput->sourceSize) == 0);

    memFree(output);
    getUdfOutput = NULL;

    // Get the contents of module 2.
    strlcpy(udfGetInput.moduleName,
            addUdfInput2->moduleName,
            sizeof(udfGetInput.moduleName));
    status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                NULL,
                                                &output,
                                                &outputSize);
    assert(status == StatusOk);

    getUdfOutput = &output->outputResult.udfGetOutput;
    assert(getUdfOutput->sourceSize == addUdfInput2->sourceSize);
    assert(strcmp(getUdfOutput->moduleName, addUdfInput2->moduleName) == 0);
    assert(memcmp(getUdfOutput->source,
                  addUdfInput2->source,
                  getUdfOutput->sourceSize) == 0);

    memFree(output);
    getUdfOutput = NULL;

    // Now let's try bulk add again and check that both attempts fail including
    // module 2 which has just been added above via first bulk update and that
    // the overall bulkAddUdf returns StatusUdfModuleAlreadyExists
    status = UserDefinedFunction::get()->bulkAddUdf(bulkAddUdfInput,
                                                    ArrayLen(bulkAddUdfInput),
                                                    NULL,
                                                    &outputArray,
                                                    &outputSizeArray);
    assert(status == StatusUdfModuleAlreadyExists);

    uint64_t ii;
    for (ii = 0; ii < ArrayLen(bulkAddUdfInput); ii++) {
        assert(outputArray[ii]->outputResult.udfAddUpdateOutput.status ==
               StatusUdfModuleAlreadyExists.code());
        memFree(outputArray[ii]);
    }
    memFree(outputArray);
    memFree(outputSizeArray);

    // Now let's retrieve the contents of both modules
    for (ii = 0; ii < ArrayLen(bulkAddUdfInput); ii++) {
        strlcpy(udfGetInput.moduleName,
                bulkAddUdfInput[ii]->moduleName,
                sizeof(udfGetInput.moduleName));
        status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                    NULL,
                                                    &output,
                                                    &outputSize);
        assert(status == StatusOk);

        getUdfOutput = &output->outputResult.udfGetOutput;
        assert(strcmp(getUdfOutput->moduleName,
                      bulkAddUdfInput[ii]->moduleName) == 0);
        if (ii == 0) {
            // Remember that module 0 has been successfully updated - and
            // neither of the bulk updates should've been able to modify it.
            // So the contents should be the same as updated contents.
            assert(getUdfOutput->sourceSize == updateUdfInput->sourceSize);
            assert(memcmp(getUdfOutput->source,
                          updateUdfInput->source,
                          getUdfOutput->sourceSize) == 0);
        }
        memFree(output);
        getUdfOutput = NULL;
    }

    // Finally, let's get rid of both modules
    XcalarApiUdfDeleteInput deleteUdfInput;
    for (ii = 0; ii < ArrayLen(bulkAddUdfInput); ii++) {
        strlcpy(deleteUdfInput.moduleName,
                bulkAddUdfInput[ii]->moduleName,
                sizeof(deleteUdfInput.moduleName));
        status = UserDefinedFunction::get()->deleteUdf(&deleteUdfInput, NULL);
        assert(status == StatusOk);

        // Make sure we can't find it anymore
        strlcpy(udfGetInput.moduleName,
                bulkAddUdfInput[ii]->moduleName,
                sizeof(udfGetInput.moduleName));
        status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                    NULL,
                                                    &output,
                                                    &outputSize);
        assert(status == StatusUdfModuleNotFound);
    }

    memFree(addUdfInput);
    memFree(addUdfInput2);
    memFree(updateUdfInput);

    return StatusOk;
}

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];

    char *dirTest = dirname(argv[0]);
    char dirConfig[1024];
    char dirChildNode[1024];

    strlcpy(dirConfig, dirTest, sizeof(dirConfig));
    strlcpy(dirChildNode, dirTest, sizeof(dirChildNode));

    strlcat(dirConfig, "/../", sizeof(dirConfig));
    strlcat(dirChildNode, "/../../../bin/childnode/", sizeof(dirChildNode));

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s%s",
             dirConfig,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagSkipRestoreUdfs,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeUsrnode));

    int numTestsFailed =
        qaRunTestSuite(testCases,
                       ArrayLen(testCases),
                       (TestCaseOptionMask)(TestCaseOptDisableIsPass |
                                            TestCaseScheduleOnRuntime |
                                            TestCaseSetTxn));

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
