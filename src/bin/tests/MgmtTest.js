// Scroll all the way down to add test cases
// Or search for function addTestCase

// Hello backend team! This is a gift from the FS people. It makes handling
// promises much funner :D
// You probably will find the calls at the bottom the most helpful. Especially
// PromiseHelper.chain
PromiseHelper = (function(PromiseHelper, $) {
    /**
    oneIter: Function that returns a promise. It represents one iteration of the
    loop.
    args: Arguments to apply to oneIter. Must be in an array
    condition: This is what we are going to call eval on. So this is a string
    that can take in arguments as in put and do whatever it wants with it. For
    example, if oneIter returns an integer, and we want to terminate if the
    integer is < 0.01(opaqueArgs.threshold), then
    condition = "arguments[0] < opaqueArgs.threshold"
    opaqueArgs: User can choose to use this argument in the condition. This
    function will not touch this argument and will not use it unless the caller
    manipulates it inside condition
    */
    PromiseHelper.doWhile = function(oneIter, args, condition, opaqueArgs) {
        // XXX: Type check!
        function doWork() {
            return (oneIter.apply({}, args)
                    .then(function() {
                        if (!eval(condition.apply({}, opaqueArgs))) {
                            return doWork();
                        }
                    })
                );
        }
        return doWork();
    };

    /**
    Same thing as doWhile except that it checks for the condition first before
    kicking into doWhile loop
    */
    PromiseHelper.while = function(oneIter, args, condition, opaqueArgs) {
        if (!eval(condition.apply({}, opaqueArgs))) {
            return PromiseHelper.doWhile(oneIter, args, condition, opaqueArgs);
        } else {
            return PromiseHelper.resolve();
        }
    };

    /**
    Runs all promises in the argument in parallel and resolves when all of
    them are complete or fails
    */
    PromiseHelper.when = function() {
        var numProm = arguments.length;
        if (numProm === 0) {
            return PromiseHelper.resolve(null);
        }
        var mainDeferred = jQuery.Deferred();

        var numDone = 0;
        var returns = [];
        var argument = arguments;
        var hasFailures = false;

        for (var t = 0; t < numProm; t++) {
            whenCall(t);
        }

        function whenCall(i) {
            argument[i].then(function(ret) {
                if (!gMutePromises) {
                    console.log("Promise", i, "done!");
                }
                numDone++;
                returns[i] = ret;

                if (numDone === numProm) {
                    if (!gMutePromises) {
                        console.log("All done!");
                    }
                    if (hasFailures) {
                        mainDeferred.reject.apply($, returns);
                    } else {
                        mainDeferred.resolve.apply($, returns);
                    }
                }
            }, function(ret) {
                console.warn("Promise", i, "failed!");
                numDone++;
                returns[i] = ret;
                hasFailures = true;
                if (numDone === numProm) {
                    console.log("All done!");
                    mainDeferred.reject.apply($, returns);
                }

            });
        }

        return (mainDeferred.promise());
    };

    /**
    Chains the promises such that only after promiseArray[i] completes, then
    promiseArray[i+1] will start.

    Usage: for example, you have a = jQuery.deferred(); b = jQuery.deferred();
    You want to run a, then after it's done, run b.
    var promiseArray = [a,b];
    promiseArray
    .then(function(bRet) {
        // Execute after b is done running
    })
    */

    PromiseHelper.chain = function(promiseArray) {
        if (!promiseArray ||
            !Array.isArray(promiseArray) ||
            typeof promiseArray[0] !== "function") {
            return PromiseHelper.resolve(null);
        }
        var head = promiseArray[0]();
        for (var i = 1; i < promiseArray.length; i++) {
            head = head.then(promiseArray[i]);
        }
        return (head);
    };

    /* return a promise with resolved value */
    PromiseHelper.resolve = function() {
        var deferred = jQuery.Deferred();
        deferred.resolve.apply(this, arguments);
        return deferred.promise();
    };

    /* return a promise with rejected error */
    PromiseHelper.reject = function() {
        var deferred = jQuery.Deferred();
        deferred.reject.apply(this, arguments);
        return deferred.promise();
    };

    return (PromiseHelper);

}({}, jQuery));

window.Function.prototype.bind = function() {
    var fn = this;
    var args = Array.prototype.slice.call(arguments);
    var obj = args.shift();
    return (function() {
        return (fn.apply(obj,
                args.concat(Array.prototype.slice.call(arguments))));
    });
};

testSuiteFn = (function($, TestSuite) {
    "use strict";

    if (!jQuery || typeof jQuery.Deferred !== "function") {
        throw "Requires jQuery 1.5+ to use asynchronous requests.";
    }

    var TestCaseEnabled = true;
    var TestCaseDisabled = false;

    // Test related variables
    var datasetPrefix = ".XcalarDS.";
    var passes;
    var fails;
    var skips;
    var returnValue;
    var defaultTimeout;
    var disableIsPass;
    var testCases;

    var thriftHandle;
    var loadArgs;
    var loadOutput;
    var origDataset;
    var yelpUserDataset;
    var yelpReviewsDataset;
    var moviesDataset;
    var moviesDatasetSet = false;
    var queryName;
    var origTable;
    var origStrTable;
    var aggrTable;
    var queryTableName;

    var makeResultSetOutput1;
    var makeResultSetOutput2;
    var makeResultSetOutput3;
    var newTableOutput;

    var sessionName = ""
    var session1; // Inactive session after apiKeySession test
    var session2; // Active session after apiKeySession test
    var session3; // Session to use to upload workbook
    var session4; // Session to use to test inactivate
                  // Inactivating other sessions could result in errors
                  // given that inactivating sessions destroys its SQG
    var session2Id; // sessionId for session2 (needed for UDF naming)

    // For retina test
    var retinaName;
    var retinaFilterStr;
    var retinaExportFile;
    var paramInput;
    var retinaImportName;

    testCases = [];
   // For start nodes test
    var startNodesState;
    var system = require('system');
    var fs = require('fs');
    var qaTestDir = system.env.QATEST_DIR;
    var envLicensePath = system.env.XCE_QALICENSEDIR;
    var numUsrnodes = 3;
    var targetName = "Default Shared Root";

    // For dataset (dataflow 2.0) tests
    var dsTestDatasetName = "dsTestDatasetName";

    console.log("Qa test dir: " + qaTestDir);
    startNodesState = TestCaseEnabled;

    function TestObj(options) {
        this.deferred = options.deferred || jQuery.Deferred();
        if (options.hasOwnProperty("currentTestNumber")) {
            this.currentTestNumber = options.currentTestNumber;
        } else {
            this.currentTestNumber = -1;
        }
        this.testName = options.testName || "Unnamed test";
        this.testFn = options.testFn;
        this.timeout = options.timeout || defaultTimeout;
        if (options.hasOwnProperty("testCaseEnabled")) {
            this.testCaseEnabled = options.testCaseEnabled;
        } else {
            this.testCaseEnabled = TestCaseEnabled;
        }
        this.witness = options.witness;
        return this;
    }

    TestObj.prototype = {
        "pass": function() {
            if (this.deferred.state() == "pending") {
                passes++;
                console.log("ok " + this.currentTestNumber + " - Test \"" +
                            this.testName + "\" passed");
                this.deferred.resolve();
            }
        },
        "fail": function(reason) {
            if (this.deferred.state() == "pending") {
                fails++;
                console.log("Test " + this.testName + " failed -- " + reason);
                console.log("not ok " + this.currentTestNumber + " - Test \"" +
                            this.testName +
                            "\" failed (" + reason + ")");
                this.deferred.reject();
            }
        },
        "skip": function() {
            console.log("====== Skipping " + this.testName + " ======");
            console.log("ok " + this.currentTestNumber + " - Test \"" +
                        this.testName + "\" disabled # SKIP");
            skips++;
            if (disableIsPass) {
                this.deferred.resolve();
            } else {
                this.deferred.reject();
            }
        },
        "assert": function(statement, sucMsg, failMsg) {
            if (!statement) {
                var reason = "Assertion Failed!";
                if (failMsg) {
                    reason = "Assertion Failed! "+failMsg;
                }
                this.fail(reason);
            } else {
                if (sucMsg) {
                    console.log(sucMsg);
                }
            }
        },
        "trivial": function(deferred) {
            var self = this;
            deferred
            .then(function(retString) {
                printResult(retString);
                self.pass();
            })
            .fail(function(status) {
                self.fail(StatusTStr[status]);
            });
        }
    };

    function printResult(result) {
        if (result) {
            console.log(JSON.stringify(result));
        }
    }

    function getDatasetCount(datasetName) {
        var numRows = -1;
        var deferred = jQuery.Deferred();
        xcalarMakeResultSetFromDataset(thriftHandle, datasetPrefix+datasetName, false)
        .then(function(ret) {
            numRows = ret.numEntries;
            console.log(JSON.stringify(ret));
            return (xcalarFreeResultSet(thriftHandle, ret.resultSetId));
        })
        .then(function(ret) {
            deferred.resolve(numRows);
        })
        .fail(function() {
            deferred.reject("Failed to get dataset count");
        });
        return deferred.promise();
    }

    function addTestCase(testFn, testName, timeout, testCaseEnabled, witness)
    {
        testCases.push(new TestObj({
            "deferred": jQuery.Deferred(),
            "currentTestNumber": testCases.length + 1,
            "testName": testName,
            "testFn": testFn,
            "testCaseEnabled": testCaseEnabled,
            "timeout": timeout,
            "witness": witness
        }));
    }

    function runTestSuite(testCases)
    {
        var initialDeferred = $.Deferred();
        var ii;
        var deferred;
        deferred = initialDeferred;

        // Start chaining the callbacks
        for (ii = 0; ii < testCases.length; ii++) {
            deferred = deferred.then(
                // Need to trap the value of testCase and ii
                (function trapFn(testCase, currentTestNumber) {
                    return function() {
                        if (testCase.testCaseEnabled) {
                            console.log("====================Test " +  testCase.currentTestNumber + " Begin====================");
                            console.log("Testing: " + testCase.testName);
                            setTimeout(function() {
                                if (testCase.deferred.state() == "pending") {
                                    var reason = "Timed out after " +
                                                 (testCase.timeout / 1000) +
                                                 " seconds";
                                    testCase.fail(reason);
                                }
                            }, testCase.timeout);

                            testCase.testFn(testCase);
                        } else {
                            testCase.skip();
                        }

                        return testCase.deferred.promise();
                    };
                })(testCases[ii], ii + 1) // Invoking trapFn
            );
        }

        deferred.fail(function() {
            returnValue = 1;
        });

        deferred.always(function() {
            console.log("# pass " + passes);
            console.log("# fail " + fails);
            console.log("# skips " + skips);
            console.log("==========================================");
            console.log("1.." + testCases.length + "\n");
            phantom.exit(returnValue);
        });

        // This starts the entire chain
        initialDeferred.resolve();
    }

    function appReapRetryer(appRunOutput) {
        var deferred = jQuery.Deferred();
        var appGroupId = appRunOutput.appGroupId;
        console.log("App GroupId: " + appGroupId);
        setTimeout(function(){
            xcalarAppReap(thriftHandle, appGroupId)
            .then(function(output) {
                deferred.resolve(output);
            })
            .fail(function(error) {
                if ("xcalarStatus" in error &&
                    error.xcalarStatus == StatusT.StatusAppInProgress) {
                    appReapRetryer(appRunOutput)
                    .then(function(retryOutput) {
                        deferred.resolve(retryOutput);
                    })
                    .fail(function(retryError) {
                        deferred.reject(retryError);
                    })
                }
                else {
                    deferred.reject(error);
                }
            });
        }, 1000);
        return (deferred.promise());
    }

    function testGetNumNodes(test) {
        xcalarGetNumNodes(thriftHandle)
        .done(function(result) {
            test.assert(result.numNodes === numUsrnodes);
            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testGetVersion(test) {
        test.trivial(xcalarGetVersion(thriftHandle));
    }

    function testRuntimeParams(test) {
        var defaultRuntimeMixedModeMinCores = "";
        var numCores = 0;
        var paramName = "RuntimeMixedModeMinCores";
        var nameSc = ["Scheduler-0", "Scheduler-1", "Scheduler-2"];
        var cpuPctSc = [85, 90, 100];
        var rtTypeSc = [RuntimeTypeT.Throughput, RuntimeTypeT.Latency, RuntimeTypeT.Latency];

        setTimeout(function() {
            waitUntilCgSetup();
        }, 500);

        function waitUntilCgSetup() {
            xcalarCgroupListParams(thriftHandle)
            .then(function(result) {
                console.log("Cgroup set up succeeded!");
                rtTest();
            })
            .fail(function(result) {
                if (result.xcalarStatus === StatusT.StatusAgain) {
                    console.log("Cgroup set up in progess...");
                    waitUntilCgSetup();
                } else if (result.xcalarStatus === StatusT.StatusCgroupsDisabled) {
                    test.pass();
                } else {
                    test.fail(StatusTStr[result.xcalarStatus]);
                }
            });
        }

        function rtTest() {
        // Get default runtime params
        xcalarRuntimeGetParam(thriftHandle)
        .then(function(result) {
            var runtimeGetParamOutput = result;
            console.log("Get default Runtime params");
            for (var ii = 0; ii < runtimeGetParamOutput.schedParams.length; ii++) {
                if (runtimeGetParamOutput.schedParams[ii].schedName === nameSc[0]) {
                    test.assert(runtimeGetParamOutput.schedParams[ii].cpusReservedInPercent <= 100);
                    test.assert(runtimeGetParamOutput.schedParams[ii].runtimeType === RuntimeTypeT.Throughput);
                } else if (runtimeGetParamOutput.schedParams[ii].schedName === nameSc[1]) {
                    test.assert(runtimeGetParamOutput.schedParams[ii].cpusReservedInPercent <= 100);
                    test.assert(runtimeGetParamOutput.schedParams[ii].runtimeType === RuntimeTypeT.Throughput ||
                        runtimeGetParamOutput.schedParams[ii].runtimeType === RuntimeTypeT.Latency);
                } else if (runtimeGetParamOutput.schedParams[ii].schedName === nameSc[2]) {
                    test.assert(runtimeGetParamOutput.schedParams[ii].cpusReservedInPercent <= 100);
                    test.assert(runtimeGetParamOutput.schedParams[ii].runtimeType === RuntimeTypeT.Throughput ||
                        runtimeGetParamOutput.schedParams[ii].runtimeType === RuntimeTypeT.Latency);
                } else {
                    test.assert(0);
                }
            }

            // Get Top result
            return xcalarApiTop(thriftHandle, XcalarApisConstantsT.XcalarApiDefaultTopIntervalInMs,
                XcalarApisConstantsT.XcalarApiDefaultCacheValidityInMs);
        })
        .then(function(result) {
            // Get core count from Top output
            numCores = result.topOutputPerNode[0].numCores;

            // Get current config params
            return xcalarGetConfigParams(thriftHandle);
        })
        .then(function(result) {
            // Remember default RuntimeMixedModeMinCores value
            var getConfigParamsOutput = result;
            var found = "false";
            test.assert(getConfigParamsOutput.numParams > 0);
            for (var ii = 0; ii < getConfigParamsOutput.numParams; ii++) {
                if (getConfigParamsOutput.parameter[ii].paramName == paramName) {
                    defaultRuntimeMixedModeMinCores = getConfigParamsOutput.parameter[ii].paramValue;
                    found = "true";
                    break;
                }
            }
            test.assert(found === "true");

            // Tweak RuntimeMixedModeMinCores to coreCount
            return xcalarSetConfigParam(thriftHandle, paramName, numCores.toString());
        })
        .then(function(reason) {
            console.log("Change runtime params");
            var sc0 = new XcalarApiSchedParamT({schedName:nameSc[0], cpusReservedInPercent:cpuPctSc[0], runtimeType:rtTypeSc[0]});
            var sc1 = new XcalarApiSchedParamT({schedName:nameSc[1], cpusReservedInPercent:cpuPctSc[1], runtimeType:rtTypeSc[1]});
            var sc2 = new XcalarApiSchedParamT({schedName:nameSc[2], cpusReservedInPercent:cpuPctSc[2], runtimeType:rtTypeSc[2]});

            // Set runtime params
            return xcalarRuntimeSetParam(thriftHandle, [sc0, sc1, sc2]);
        })
        .then(function(reason) {
            // Get runtime params
            return xcalarRuntimeGetParam(thriftHandle);
        })
        .then(function(result) {
            // Validate the runtime params
            var runtimeGetParamOutput = result;
            console.log("Get new Runtime params");
            for (var ii = 0; ii < runtimeGetParamOutput.schedParams.length; ii++) {
                if (runtimeGetParamOutput.schedParams[ii].schedName === nameSc[0]) {
                    test.assert(runtimeGetParamOutput.schedParams[ii].runtimeType === rtTypeSc[0]);
                } else if (runtimeGetParamOutput.schedParams[ii].schedName === nameSc[1]) {
                    test.assert(runtimeGetParamOutput.schedParams[ii].runtimeType === rtTypeSc[1]);
                } else if (runtimeGetParamOutput.schedParams[ii].schedName === nameSc[2]) {
                    test.assert(runtimeGetParamOutput.schedParams[ii].runtimeType === rtTypeSc[2]);
                } else {
                    test.assert(false);
                }
            }

            // Revert to original runtime params
            var sc0 = new XcalarApiSchedParamT({schedName:nameSc[0], cpusReservedInPercent:100, runtimeType:rtTypeSc[0]});
            var sc1 = new XcalarApiSchedParamT({schedName:nameSc[1], cpusReservedInPercent:100, runtimeType:rtTypeSc[1]});
            var sc2 = new XcalarApiSchedParamT({schedName:nameSc[2], cpusReservedInPercent:100, runtimeType:rtTypeSc[2]});
            return xcalarRuntimeSetParam(thriftHandle, [sc0, sc1, sc2]);
        })
        .then(function(result) {
            return xcalarCgroupGetParams(thriftHandle, "xcalar_xce", "memory");
        })
        .then(function(result) {
            // Revert to default RuntimeMixedModeMinCores
            xcalarSetConfigParam(thriftHandle, paramName, defaultRuntimeMixedModeMinCores);
        })
        .then(function(reason) {
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
        }
    }

    function testGetConfigParams(test) {
        xcalarGetConfigParams(thriftHandle)
        .then(function(result) {
            var getConfigParamsOutput = result;
            test.assert(getConfigParamsOutput.numParams > 0);
            console.log("Number of parameters #" + getConfigParamsOutput.numParams);
            for (var ii = 0; ii < getConfigParamsOutput.numParams; ii++) {
                console.log(
                    "Name: " + getConfigParamsOutput.parameter[ii].paramName +
                    ", Value: " + getConfigParamsOutput.parameter[ii].paramValue +
                    ", Visible: " + getConfigParamsOutput.parameter[ii].visible +
                    ", Changeable: " + getConfigParamsOutput.parameter[ii].changeable +
                    ", Restart: " + getConfigParamsOutput.parameter[ii].restartRequired +
                    ", Default: " + getConfigParamsOutput.parameter[ii].defaultValue);
                // Check one of them.
                if (getConfigParamsOutput.parameter[ii].paramName == "TotalSystemMemory") {
                    test.assert(getConfigParamsOutput.parameter[ii].changeable === false);
                    test.assert(getConfigParamsOutput.parameter[ii].visible === false);
                }
            }
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testSetConfigParam(test) {
        xcalarGetConfigParams(thriftHandle)
        .then(function(result) {
            var getConfigParamsOutput = result;
            var paramName = "Minidump";
            var paramValueNew = "true";
            var paramValueOld = "true";
            var found = "false";

            // toggle param value.
            test.assert(getConfigParamsOutput.numParams > 0);
            for (var ii = 0; ii < getConfigParamsOutput.numParams; ii++) {
                if (getConfigParamsOutput.parameter[ii].paramName == paramName) {
                    paramValueOld = getConfigParamsOutput.parameter[ii].paramValue;
                    found = "true";
                    break;
                }
            }
            test.assert(found == "true");

            if (paramValueOld == "true") {
                paramValueNew = "false";
            } else {
                test.assert(paramValueOld == "false");
                paramValueNew = "true";
            }

            // Set new param value
            xcalarSetConfigParam(thriftHandle, paramName, paramValueNew)
            .then(function(result) {
                xcalarGetConfigParams(thriftHandle)
                .then(function(result) {
                    getConfigParamsOutput = result;
                    found = "false";
                    test.assert(getConfigParamsOutput.numParams > 0);
                    for (var ii = 0; ii < getConfigParamsOutput.numParams; ii++) {
                        if (getConfigParamsOutput.parameter[ii].paramName == paramName) {
                            // Hidden parameter should be visible once it is changed
                            // from its default value.
                            console.log(
                                "Set new value for Param Name: " + getConfigParamsOutput.parameter[ii].paramName +
                                ", Value: " + getConfigParamsOutput.parameter[ii].paramValue +
                                ", Visible: " + getConfigParamsOutput.parameter[ii].visible +
                                ", Changeable: " + getConfigParamsOutput.parameter[ii].changeable +
                                ", Restart: " + getConfigParamsOutput.parameter[ii].restartRequired +
                                ", Default: " + getConfigParamsOutput.parameter[ii].defaultValue);
                             test.assert(getConfigParamsOutput.parameter[ii].paramValue == paramValueNew);
                             found = "true";
                             break;
                        }
                    }
                    test.assert(found == "true");

                    // Reset to old param value
                    xcalarSetConfigParam(thriftHandle, paramName, paramValueOld)
                    .then(function(result) {
                        xcalarGetConfigParams(thriftHandle)
                        .then(function(result) {
                            getConfigParamsOutput = result;
                            test.assert(getConfigParamsOutput.numParams > 0);
                            for (var ii = 0; ii < getConfigParamsOutput.numParams; ii++) {
                                if (getConfigParamsOutput.parameter[ii].paramName == paramName) {
                                    console.log(
                                        "Reset new value for Param Name: " + getConfigParamsOutput.parameter[ii].paramName +
                                        ", Value: " + getConfigParamsOutput.parameter[ii].paramValue +
                                        ", Visible: " + getConfigParamsOutput.parameter[ii].visible +
                                        ", Changeable: " + getConfigParamsOutput.parameter[ii].changeable +
                                        ", Restart: " + getConfigParamsOutput.parameter[ii].restartRequired +
                                        ", Default: " + getConfigParamsOutput.parameter[ii].defaultValue);
                                    test.assert(getConfigParamsOutput.parameter[ii].paramValue == paramValueOld);
                                    test.pass();
                                }
                            }
                        });
                    });
                });
            });
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testApps(test) {
        var name = "mgmtTestPythonApp";
        var hostType = "Python";
        var duty = "Export";
        var execStr = "def main(inBlob): return 's' + inBlob";

        // Test either create or update.
        xcalarAppSet(thriftHandle, name, hostType, duty, execStr)
        .then(function(result) {
            return xcalarAppRun(thriftHandle, name, false, "hello");
        })
        .then(appReapRetryer)
        .then(function(result) {
            var outStr = result.outStr;
            var expectedStr = "[[\"shello\"]]";
            if (outStr == expectedStr) {
                test.pass();
            } else {
                test.fail("Output: expected '" + expectedStr + "' got '" + outStr + "'");
            }
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    // 2 = LOG_CRIT, 1 = FlushGlobal, 0 = no change to periodic flushing
    function testLogLevelSetCrit(test) {
        test.trivial(xcalarLogLevelSet(thriftHandle, 2, 1, 0));
    }

    // 7 = LOG_DEBUG, 0 = NoFlush, 0 = no change to periodic flushing
    function testLogLevelSetDebug(test) {
        test.trivial(xcalarLogLevelSet(thriftHandle, 7, 0, 0));
    }

    function testGetIpAddrNode0(test) {
        test.trivial(xcalarGetIpAddr(thriftHandle, 0));
    }

    function testPreview(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/yelp/user";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;

        xcalarPreview(thriftHandle, sourceArgs, 11, 2)
        .then(function(result) {
            printResult(result);
            var previewOutput = result;
            var preview = JSON.parse(previewOutput.outputJson);
            console.log("\t yelp/user preview : " + preview.base64Data);
            var expectedStr = "[\n{\"yelping_s";
            console.log("\t expected encoded: " + btoa(expectedStr.substring(2,13)));
            console.log("\t expected len: " + expectedStr.length - 2);
            test.assert(atob(preview.base64Data) === expectedStr.substring(2,13));
            test.assert(preview.thisDataSize === expectedStr.length - 2);
            test.assert(preview.fileName ===
                "yelp_academic_dataset_user_fixed.json");
            test.assert(preview.totalDataSize === 27053171);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testTarget(test) {
        var targetName = "mgmtdtest target";
        var targetType = "shared";
        var targetParams = {"mountpoint": "/netstore"};
        // Add target
        xcalarTargetCreate(thriftHandle, targetType, targetName, targetParams)
        .then(function() {
            // Get list of keys using this keyname as a regex
            return xcalarTargetList(thriftHandle);
        })
        .then(function(targetList) {
            var targFound = false;
            for (var ii = 0; ii < targetList.length; ii++) {
                if (targetList[ii].name === targetName) {
                    targFound = true;
                    break;
                }
            }
            test.assert(targFound);
            return xcalarTargetDelete(thriftHandle, targetName);
        })
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(result) {
            test.fail(StatusTStr[result["xcalarStatus"]]);
        });
    }

    function testListTargetTypes(test) {
        var typeId = "shared";
        var typeName = "Shared File System";
        var paramName = "mountpoint";
        // Add target
        xcalarTargetTypeList(thriftHandle)
        .then(function(targetTypeList) {
            var thisTargType = null;
            for (var ii = 0; ii < targetTypeList.length; ii++) {
                if (targetTypeList[ii].type_id === typeId) {
                    thisTargType = targetTypeList[ii];
                }
            }
            test.assert(thisTargType !== null);
            test.assert(thisTargType.type_id === typeId);
            test.assert(thisTargType.type_name === typeName);
            test.assert(typeof(thisTargType.description) === "string");
            test.assert(thisTargType.parameters.length === 1);
            test.assert(typeof(thisTargType.parameters[0].description) === "string");
        })
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(result) {
            test.fail(StatusTStr[result["xcalarStatus"]]);
        });
    }

    function testDriver(test) {
        var driverName = "mgmtdtest driver";
        // This needs to be kept up to date with UserDefinedFunction.h
        var moduleName = "/sharedUDFs/mgmtdtest_driver_mod";
        var driverSource = "import xcalar.container.driver.base as driver\n" +
            "def placeholder(): return\n" +
            "@driver.register_export_driver(name=\"mgmtdtest driver\")\n" +
            "@driver.param(name=\"param 1\", desc=\"test driver param2\", optional=True)\n" +
            "def driver(): return";
        // Add driver
        xcalarApiUdfDelete(thriftHandle, moduleName)
        .always(function() {
            console.log("finished cleaning udf! adding udf");
            xcalarApiUdfAdd(thriftHandle, UdfTypeT.UdfTypePython,
                            moduleName, driverSource)
            .then(function() {
                console.log("finished adding udf! listing drivers");
                return xcalarDriverList(thriftHandle);
            })
            .then(function(driverList) {
                console.log("finished listing; checking");
                var driverFound = false;
                for (var ii = 0; ii < driverList.length; ii++) {
                    if (driverList[ii].name === driverName) {
                        // Spot check the driver
                        test.assert(driverList[ii].params[0].name == "param 1");
                        driverFound = true;
                        break;
                    }
                }
                test.assert(driverFound);
                console.log("finished checking; deleting");
                return xcalarApiUdfDelete(thriftHandle, moduleName);
            })
            .done(function(status) {
                printResult(status);
                test.pass();
            })
            .fail(function(result) {
                test.fail(StatusTStr[result["xcalarStatus"]]);
            });
        });
    }

    function testLoad(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/yelp/user";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseJson";
        parseArgs.parserArgJson = "{}";

        xcalarLoad(thriftHandle, "yelp", [sourceArgs], parseArgs, 0)
        .then(function(result) {
            printResult(result);
            loadOutput = result;
            test.assert(result.numBytes >= 17039360);
            test.assert(result.numFiles == 1);
            origDataset = loadOutput.dataset.name;
            yelpUserDataset = loadOutput.dataset.name;
            return getDatasetCount("yelp");
        })
        .then(function(count) {
            test.assert(count === 70817);
            // Reuse the last call's sourceArgs
            sourceArgs.path = qaTestDir + "/yelp/reviews";
            return (xcalarLoad(thriftHandle, "yelpReviews", [sourceArgs], parseArgs, 0));
        })
        .then(function(result) {
            yelpReviewsDataset = result.dataset.name;
            return getDatasetCount("yelpReviews");
        })
        .then(function(count) {
            test.assert(count == 335022);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testLoadRegex(test) {
        var yelpTipDataset = "";
        var yelpTipLoadOutput = "";

        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/yelp";
        sourceArgs.fileNamePattern = "re:(user|tip)\\/.*\\.json";
        sourceArgs.recursive = true;
        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseJson";
        parseArgs.parserArgJson = "{}";

        xcalarLoad(thriftHandle, "yelpTip", [sourceArgs], parseArgs, 0)
        .then(function(result) {
            printResult(result);
            yelpTipLoadOutput = result;
            yelpTipDataset = yelpTipLoadOutput.dataset.name;
            return getDatasetCount("yelpTip");
        })
        .then(function(count) {
            test.assert(count === 184810);
            return (xcalarDeleteDagNodes(thriftHandle, yelpTipDataset,
                                         SourceTypeT.SrcDataset));
        })
        .then(function(deleteDagNodesOutput) {
            console.log("deleteDagNodesOutput.numNodes:", deleteDagNodesOutput.numNodes);
            for (var ii = 0; ii < deleteDagNodesOutput.numNodes; ii++) {
                var deleteDagNodeStatus = deleteDagNodesOutput.statuses[ii];
                console.log(deleteDagNodeStatus.nodeInfo.name,
                            StatusTStr[deleteDagNodeStatus.status]);
            }
            if (deleteDagNodesOutput.numNodes != 1) {
                test.fail("number of nodes deleted != 1");
                return;
            }

            if (deleteDagNodesOutput.statuses[0].nodeInfo.name != yelpTipDataset) {
                test.fail("dag node deleted is not", yelpTipDataset);
                return;
            }

            if (deleteDagNodesOutput.statuses[0].status != StatusT.StatusOk) {
                test.fail("Failed to delete dag node");
                return;
            }

            return (xcalarDatasetUnload(thriftHandle, yelpTipDataset));
        })
        .then(function(datasetUnloadOutput) {
            console.log("datasetUnloadOutput.numNodes:", datasetUnloadOutput.numDatasets);
            for (var ii = 0; ii < datasetUnloadOutput.numDatasets; ii++) {
                var datasetUnloadStatus = datasetUnloadOutput.statuses[ii];
                console.log(datasetUnloadStatus.dataset.name,
                            StatusTStr[datasetUnloadStatus.status]);
            }

            if (datasetUnloadOutput.numDatasets != 1) {
                test.fail("number of datasets deleted != 1");
                return;
            }

            if (datasetUnloadOutput.statuses[0].dataset.name != yelpTipDataset) {
                test.fail("dataset deleted is not", yelpTipDataset);
                return;
            }

            if (datasetUnloadOutput.statuses[0].status != StatusT.StatusOk) {
                test.fail("Failed to delete dataset");
                return;
            }

            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testLoadEdgeCaseDos(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/edgeCases/dosFormat.csv";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseCsv";
        parseArgs.parserArgJson = JSON.stringify({"recordDelim": "\r", "schemaMode": "header"});

        xcalarLoad(thriftHandle, "dosFormat", [sourceArgs], parseArgs, 0)
        .then(function(result) {
            return getDatasetCount("dosFormat");
        })
        .then(function(numRows) {
            console.log("got numrows " + numRows);
            test.assert(numRows == 123);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }


    function testBadLoad(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/edgeCases/bad.json";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseJson";
        parseArgs.parserArgJson = "{}";

        xcalarLoad(thriftHandle, "bad", [sourceArgs], parseArgs, 0)
        .then(function(result) {
            test.fail("load succeeded when it should have failed");
        })
        .fail(function(failStruct) {
            if (!failStruct || typeof(failStruct) !== "object") {
                test.fail("Fail struct type is wrong");
            }
            var loadOutput = failStruct.output;
            var errStr = "Extra data: line 2 column 1 (char 10891)";
            var errFile = qaTestDir + "/edgeCases/bad.json";
            if (loadOutput.errorString.indexOf(errStr) !== -1 &&
                loadOutput.errorFile == errFile) {
                test.pass();
            } else {
                test.fail("errorString: \"" + loadOutput.errorString +
                          "\" should be: \"" + errStr + "\" errorFile: \"" +
                          loadOutput.errorFile + "\" should be: \"" + errFile);
            }
        });
    }

    function testBulkDestroyDs(test) {
        setSessionName(session2)
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/yelp/reviews";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseJson";
        parseArgs.parserArgJson = "{}";

        xcalarLoad(thriftHandle, "review", [sourceArgs], parseArgs, 0)
        .then(function(result) {
            var testloadOutput = result;
            return xcalarDeleteDagNodes(thriftHandle, "*",
                                        SourceTypeT.SrcDataset);
        })
        .then(function(destroyDatasetsOutput) {
            printResult(destroyDatasetsOutput);

            for (var i = 0, delDsStatus = null;
                i < destroyDatasetsOutput.numNodes; i ++) {
                delDsStatus = destroyDatasetsOutput.statuses[i];
                console.log("\t" + delDsStatus.nodeInfo.name + ": " +
                            StatusTStr[delDsStatus.status]);
            }

            return (xcalarDatasetUnload(thriftHandle, "*"));
        })
        .then(function(datasetUnloadOutput) {

            for (var ii = 0, unloadDsStatus = null;
                 ii < datasetUnloadOutput.numDatasets; ii++) {
                unloadDsStatus = datasetUnloadOutput.statuses[ii];
                console.log("\t" + unloadDsStatus.dataset.name + ": " +
                            StatusTStr[unloadDsStatus.status]);
            }
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testListDatasets(test) {
        xcalarListDatasets(thriftHandle)
        .then(function(listDatasetsOutput) {
            printResult(listDatasetsOutput);

            var foundLoadDs = false;
            for (var i = 0, dataset = null; i < listDatasetsOutput.numDatasets;
                 i ++) {
                dataset = listDatasetsOutput.datasets[i];

                console.log("\tdataset[" + i.toString() + "].url = " +
                            dataset.url);
                console.log("\tdataset[" + i.toString() + "].name = " +
                            dataset.name);
                console.log("\tdataset[" + i.toString() + "].datasetId = " +
                    dataset.datasetId);
                console.log("\tdataset[" + i.toString() + "].formatType = " +
                    DfFormatTypeTStr[dataset.formatType]);
                console.log("\tdataset[" + i.toString() + "].loadIsComplete = "+
                    dataset.loadIsComplete.toString());

                if (dataset.name === loadOutput.dataset.name) {
                    foundLoadDs = true;
                    break;
                }
            }
            test.assert(foundLoadDs,
                        "Found dataset \"" + loadOutput.dataset.name + "\"",
                        "Could not find loaded dataset \"" +
                        loadOutput.dataset.name + "\"");
            test.pass();

        })
        .fail(test.fail);
    }

    function testGetDatasetsInfo(test) {
        var datasetNamePattern = "*";
        xcalarGetDatasetsInfo(thriftHandle, datasetNamePattern)
        .then(function(getDatasetsOutput) {
            printResult(getDatasetsOutput);

            for (var i = 0, datasetInfo = null;
                i < getDatasetsOutput.numDatasets; i++) {
                    datasetInfo = getDatasetsOutput.datasets[i];
                    console.log("\tdataset[" + i.toString() + "].datasetName = " +
                        datasetInfo.datasetName);
                    console.log("\tdataset[" + i.toString() + "].datasetSize = " +
                        datasetInfo.datasetSize);
                    for (var k = 0; k < datasetInfo.numColumns; k++) {
                        console.log("\tdataset[" + i.toString() + "].columnName[" +
                            k.toString() + "] = " + datasetInfo.columns[k].name);
                    }
                }

            test.pass();
        })
        .fail(test.fail);
    }

    function testListDatasetUsers(test) {
        var datasetName = datasetPrefix + "yelp";
        xcalarListDatasetUsers(thriftHandle, datasetName)
            .then(function(listDatasetUsersOutput) {
                printResult(listDatasetUsersOutput);

                for (var i = 0, user = null;
                    i < listDatasetUsersOutput.usersCount; i++) {

                    user = listDatasetUsersOutput.user[i];

                    console.log("\tuser[" + i.toString() + "].userIdName = " +
                                user.userId.userIdName);
                    console.log("\tuser[" + i.toString() + "].referenceCount = " +
                                user.referenceCount);
                }

                test.pass();
            })
        .fail(test.fail);
    }

    function testListUserDatasets(test) {
        var userIdName = "test";
        xcalarListUserDatasets(thriftHandle, userIdName)
        .then(function(listUserDatasetsOutput) {
            printResult(listUserDatasetsOutput);

            for (var i = 0, dataset = null;
                i < listUserDatasetsOutput.numDatasets; i++) {

                    dataset = listUserDatasetsOutput.datasets[i];

                    console.log("\tdataset[" + i.toString() + "].datasetName = " +
                                dataset.datasetName);
                    console.log("\tdataset[" + i.toString() + "].isLocked = " +
                                dataset.isLocked);
                }
            test.pass();
        })
        .fail(test.fail);
    }

    function testDatasetCreate(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/yelp/user";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseJson";
        parseArgs.parserArgJson = "{}";

        xcalarDatasetCreate(thriftHandle, dsTestDatasetName, [sourceArgs], parseArgs, 0)
        .then(function() {
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        })
    }

    function testDatasetLoad(test) {
        setSessionName(session2)
        var sourceArgs = new DataSourceArgsT();
        var parseArgs = new ParseArgsT();
        xcalarLoad(thriftHandle, dsTestDatasetName, [sourceArgs], parseArgs, 0)
        .then(function(result) {
            test.assert(result.numBytes >= 17039360);
            test.assert(result.numFiles == 1);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testDatasetUnload(test) {
        xcalarDatasetUnload(thriftHandle, dsTestDatasetName)
        .then(function(result) {
            printResult(result);
            test.assert(result.numDatasets == 1);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        })
    }

    function testDatasetGetMeta(test) {
        xcalarDatasetGetMeta(thriftHandle, dsTestDatasetName)
        .then(function(result) {
            // printResult(result);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testDatasetDelete(test) {
        var dsName = datasetPrefix + dsTestDatasetName;
        xcalarDeleteDagNodes(thriftHandle, dsName, SourceTypeT.SrcDataset)
        .then(function(deleteDagNodesOutput) {
            return (xcalarDatasetDelete(thriftHandle, dsTestDatasetName));
        })
        .then(function() {
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testIndexDatasetIntSync(test) {
        test.trivial(xcalarIndex(thriftHandle,
                                 loadOutput.dataset.name,
                                 "yelp/user-review_count",
                                 [new XcalarApiKeyT({name:"review_count", type:"DfInt64", keyFieldName:"", ordering:"Unordered"})],
                                 "yelp_user"));

    }

    function testIndexDatasetInt(test) {
        xcalarIndex(thriftHandle,
                    loadOutput.dataset.name,
                    "yelp/user-votes.funny",
                    [new XcalarApiKeyT({name:"votes.funny", type:"DfInt64", keyFieldName:"", ordering:"Unordered"})],
                    "yelp_user")
        .done(function(indexOutput) {
            printResult(indexOutput);
            origTable = indexOutput.tableName;
            test.pass();
        })
        .fail(test.fail);
    }

    function testIndexDatasetStr(test) {
        xcalarIndex(thriftHandle,
                    loadOutput.dataset.name,
                    "yelp/user-user_id",
                    [new XcalarApiKeyT({name:"user_id", type:"DfString", keyFieldName:"", ordering:"Unordered"})],
                    "yelp_user")
        .done(function(indexStrOutput) {
            printResult(indexStrOutput);
            origStrTable = indexStrOutput.tableName;
            test.pass();
        })
        .fail(test.fail);
    }

    function testIndexDatasetWithPrefix(test) {
        var tableName = "yelpUserWithPrefix";
        var resultSetId;
        xcalarIndex(thriftHandle,
                    loadOutput.dataset.name,
                    tableName,
                    [new XcalarApiKeyT({name:"user_id", type:"DfString", keyFieldName:"", ordering:"Unordered"})],
                    "prefix")
        .then(function() {
            return xcalarMakeResultSetFromTable(thriftHandle, tableName);
        })
        .then(function(ret) {
            resultSetId = ret.resultSetId;
            return xcalarResultSetAbsolute(thriftHandle, resultSetId, 0);
        })
        .then(function(ret) {
            return xcalarResultSetNext(thriftHandle, resultSetId, 1);
        })
        .then(function(ret) {
            var oneValue = JSON.parse(ret.values[0]);
            printResult(oneValue);
            for (var key in oneValue) {
                printResult(key);
                test.assert(key.indexOf("prefix::") === 0 ||
                            key.indexOf("prefix-") === 0);
            }

            return xcalarFreeResultSet(thriftHandle, resultSetId);
        })
        .then(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(test.fail);
    }

    function testIndexTable(test) {
        test.trivial(xcalarIndex(thriftHandle,
                                 origStrTable,
                                 "yelp/user-name",
                                 [new XcalarApiKeyT({name:"yelp_user::name", type:"DfString", keyFieldName:"", ordering:"Unordered"})]));
    }

    function testRenameNode(test) {
        xcalarRenameNode(thriftHandle, origTable, "newName")
        .then(function(status) {
            printResult(status);
            return xcalarRenameNode(thriftHandle, "newName", origTable);
        })
        .then(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testGetQueryIndex(test) {
        var workItem = new WorkItem();
        workItem.input = new XcalarApiInputT();
        workItem.input.indexInput = new XcalarApiIndexInputT();
        workItem.input.indexInput.source = new XcalarApiNamedInputT();
        workItem.input.indexInput.dstTable = new XcalarApiTableInputT();

        workItem.api = XcalarApisT.XcalarApiIndex;
        workItem.input.indexInput.source.isTable = false;
        workItem.input.indexInput.source.name = "dataset";
        workItem.input.indexInput.source.xid = XcalarApiXidInvalidT;
        workItem.input.indexInput.dstTable.tableName = "dstTable";
        workItem.input.indexInput.dstTable.tableId = XcalarApiTableIdInvalidT;
        workItem.input.indexInput.keyName = "keyName";
        workItem.input.indexInput.dhtName = "";

        console.log(xcalarApiGetQuery(thriftHandle, workItem));
        test.pass();
    }

    function testGetQueryLoad(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = "url";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;

        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseJson";
        parseArgs.parserArgJson = "{}";

        var workItem = new WorkItem();
        workItem.input = new XcalarApiInputT();
        workItem.input.loadInput = new XcalarApiBulkLoadInputT();
        workItem.input.loadInput.loadArgs = new XcalarApiDfLoadArgsT();
        workItem.input.loadInput.loadArgs.sourceArgs = sourceArgs;
        workItem.input.loadInput.loadArgs.parseArgs = parseArgs;

        workItem.api = XcalarApisT.XcalarApiBulkLoad;
        workItem.input.loadInput.dest = "datasetName";
        workItem.input.loadInput.loadArgs.size = 1024;

        console.log(xcalarApiGetQuery(thriftHandle, workItem));
        test.pass();
    }

    function testIndexDatasetBogus(test) {
         test.trivial(xcalarIndex(thriftHandle,
                                  loadOutput.dataset.name,
                                  "yelp/user-garbage",
                                  [new XcalarApiKeyT({name:"garbage", type:"DfUnknown", keyFieldName:"", ordering:"Unordered"})],
                                  "yelp_user"));
    }

    function testIndexTable2(test) {
        test.trivial(xcalarIndex(thriftHandle,
                                 origStrTable,
                                 "yelp/user-yelping_since",
                                 [new XcalarApiKeyT({name:"yelp_user::yelping_since", type:"Df string", keyFieldName:"", ordering:"Unordered"})]));
    }

    function testGetTableRefCount(test) {
        test.trivial(xcalarGetTableRefCount(thriftHandle, origTable));
    }

    function testGetTableMeta(test) {
        xcalarGetTableMeta(thriftHandle, origTable, false)
        .done(function(metaOutput) {
            printResult(metaOutput);

            var pgCount1 = 0;
            var pgCount2 = 0;
            var rowCount1 = 0;
            var rowCount2 = 0;

            for (var i = 0; i < metaOutput.numMetas; i ++) {
                rowCount1 += metaOutput.metas[i].numRows;
                pgCount1 += metaOutput.metas[i].numPages;
                for (var j = 0; j < metaOutput.metas[i].numSlots; j++) {
                    rowCount2 += metaOutput.metas[i].numRowsPerSlot[j];
                    pgCount2 += metaOutput.metas[i].numPagesPerSlot[j];
                }
            }

            if (pgCount1 == pgCount2 && rowCount1 == rowCount2) {
                test.pass();
            } else {
                var reason = "pgCount1: " + pgCount1 +
                    " pgCount2: " + pgCount2 +
                    " rowCount1: " + rowCount1 +
                    " rowCount2: " + rowCount2;
                test.fail(reason);
            }
        })
        .fail(test.fail);
    }

    function curryVerifyCountOutput(test) {
        function verifyCountOutput(metaOutput) {
            printResult(metaOutput);

            var totalCount = 0;
            for (var i = 0; i < metaOutput.numMetas; i ++) {
                totalCount += metaOutput.metas[i].numRows;
                console.log("Node " + i + ": " + metaOutput.metas[i].numRows);
            }

            console.log("\tcount: " + totalCount.toString());
            test.assert(totalCount === 70817, undefined,
                        "wrong count: " + totalCount + " expected: 70817");
            test.pass();
        }
        return (verifyCountOutput);
    }

    function testGetDatasetCount(test) {
        var verifyDatasetCount = curryVerifyCountOutput(test);
        xcalarGetDatasetMeta(thriftHandle, yelpUserDataset)
        .done(verifyDatasetCount)
        .fail(test.fail);
    }

    function testGetTableCount(test) {
        var verifyTableCount = curryVerifyCountOutput(test);
        xcalarGetTableMeta(thriftHandle, origTable, false)
        .done(verifyTableCount)
        .fail(test.fail);
    }

    function testListTables(test) {
        xcalarListTables(thriftHandle, "yelp*", SourceTypeT.SrcTable)
        .then(function(listTablesOutput) {
            printResult(listTablesOutput);

            var foundVotesFunny = false;
            for (var i = 0, node = null; i < listTablesOutput.numNodes; i ++) {
                node = listTablesOutput.nodeInfo[i];
                console.log("\ttable[" + i.toString() + "].tableName = " + node.name);
                console.log("\ttable[" + i.toString() + "].tableId = " +
                            node.dagNodeId);
                console.log("\ttable[" + i.toString() + "].state = " +
                            node.state.toString());
                if (node.name === origTable && node.size > 0) {
                    foundVotesFunny = true;
                }
            }
            test.assert(foundVotesFunny, "Found node \"" + origTable + "\"",
                        "failed to find node \"" + origTable + "\"");
            test.pass();
        })
        .fail(test.fail);
    }

    function testGetStats(test) {
        xcalarGetStats(thriftHandle, 0)
        .then(function(statOutput) {
            printResult(statOutput);

            for (var i = 0, stat = null; i < statOutput.numStats; i ++) {
                stat = statOutput.stats[i];

                console.log("\tstat[" + i.toString() + "].statName = " +
                        stat.statName);
                console.log("\tstat[" + i.toString() + "].statValue = " +
                        stat.statValue.toString());
                console.log("\tstat[" + i.toString() + "].statType = " +
                        stat.statType.toString());
                console.log("\tstat[" + i.toString() + "].groupId = " +
                        stat.groupId.toString());
            }
            test.assert(statOutput.numStats, undefined, "No stats returned");
            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testGetStatGroupIdMap(test) {
        xcalarGetStatGroupIdMap(thriftHandle, 0, 5)
        .then(function(groupMapOutput) {
            printResult(groupMapOutput);

            if (groupMapOutput.numGroupNames !== 0) {
                console.log("\tnumGroupNames: " +
                        groupMapOutput.numGroupNames.toString());

                for (var i = 0, groupInfo = null;
                        i < groupMapOutput.numGroupNames; i ++) {
                    groupInfo =  groupMapOutput.groupNameInfoArray[i];
                    console.log("\tgroupName[" + i.toString() + "] = " +
                                    groupInfo.statsGroupName);
                    console.log("\tgroupIdNum[" + i.toString() + "] = " +
                                    groupInfo.groupIdNum);
                    console.log("\ttotalSingeStats[" + i.toString() + "] = " +
                                    groupInfo.totalSingleStats);
                }

                test.pass();
            } else {
                var reason = "numGroupNames == 0";
                test.fail(reason);
            }
        })
        .fail(test.fail);
    }

    function testResetStats(test) {
        test.trivial(xcalarResetStats(thriftHandle, 0));
    }

    function testMakeResultSetFromDataset(test) {
        xcalarMakeResultSetFromDataset(thriftHandle,
                                       loadOutput.dataset.name,
                                       false)
        .then(function(result) {
            printResult(result);
            makeResultSetOutput1 = result;
            test.pass();
        })
        .fail(test.fail);
    }

    function testMakeResultSetFromTable(test) {
        xcalarMakeResultSetFromTable(thriftHandle,
                                     origTable)
        .then(function(result) {
            printResult(result);
            makeResultSetOutput2 = result;
            test.pass();
        })
        .fail(test.fail);
    }

    function testMakeResultSetFromAggregate(test) {
        xcalarMakeResultSetFromTable(thriftHandle, aggrTable)
        .then(function(result) {
            printResult(result);
            makeResultSetOutput3 = result;
            test.pass();
        })
        .fail(test.fail);
    }

    function testResultSetNextDataset(test) {
        xcalarResultSetNext(thriftHandle,
                            makeResultSetOutput1.resultSetId, 5)
        .then(function(resultNextOutput1) {
            printResult(resultNextOutput1);
            test.assert(resultNextOutput1.numValues > 0);

            for (var i = 0, value = null; i < resultNextOutput1.numValues;
                 i++) {
                value = resultNextOutput1.values[i];

                console.log("\trecord[" + i.toString() + "].value = " +
                            value);
            }
            test.pass();
        })
        .fail(test.fail);
    }

    function testResultSetAbsolute(test) {
        test.trivial(xcalarResultSetAbsolute(thriftHandle,
                    makeResultSetOutput2.resultSetId, 1000));
    }

    function testResultSetAbsoluteBogus(test) {
        xcalarResultSetAbsolute(thriftHandle,
                                makeResultSetOutput2.resultSetId,
                                281474976710655)
        .then(test.fail)
        .fail(function() {
            test.pass();
        });
    }

    function testResultSetNextTable(test) {
        xcalarResultSetNext(thriftHandle,
                            makeResultSetOutput2.resultSetId, 5)
        .then(function(resultNextOutput2) {
            printResult(resultNextOutput2);
            test.assert(resultNextOutput2.numValues > 0);

            for (var i = 0, value = null; i < resultNextOutput2.numValues;
                 i ++) {
                value = resultNextOutput2.values[i];
                console.log("\trecord[" + i.toString() + "].value = " +
                            value);
            }
            test.pass();
        })
        .fail(test.fail);
    }

    function testResultSetNextAggregate(test) {
        xcalarResultSetNext(thriftHandle,
                            makeResultSetOutput3.resultSetId, 5)
        .then(function(resultNextOutput3) {
            printResult(resultNextOutput3);
            test.assert(resultNextOutput3.numValues > 0);

            for (var i = 0, value = null; i < resultNextOutput3.numValues;
                 i++) {
                value = resultNextOutput3.values[i];
                console.log("\trecord[" + i.toString() + "].value = " +
                            value);
            }
            test.pass();
        })
        .fail(test.fail);
    }

    function testFreeResultSetAggregate(test) {
        test.trivial(xcalarFreeResultSet(thriftHandle,
                                         makeResultSetOutput3.resultSetId));
    }

    function testFreeResultSetDataset(test) {
        test.trivial(xcalarFreeResultSet(thriftHandle,
                                         makeResultSetOutput1.resultSetId));
    }

    function testFreeResultSetTable(test) {
        test.trivial(xcalarFreeResultSet(thriftHandle,
                                         makeResultSetOutput2.resultSetId));
    }

    function testFilter(test) {
        xcalarFilter(thriftHandle, "gt(yelp_user::votes.funny, 900)", origTable,
                     "yelp/user-votes.funny-gt900")
        .then(function(ret) {
              test.assert(ret.tableName == "yelp/user-votes.funny-gt900");
              return xcalarMakeResultSetFromTable(thriftHandle, "yelp/user-votes.funny-gt900");
        })
        .then(function(ret) {
              test.assert(ret.numEntries == 488);
              return xcalarFreeResultSet(thriftHandle, ret.resultSetId);
        })
        .then(function(ret) {
            test.pass();
        })
        .fail(test.fail);
    }

    function testProject(test) {
        xcalarProject(thriftHandle, 2, ["yelp_user::votes.funny",
                                        "yelp_user::user_id"],
                      origTable, "yelp/user-votes.funny-projected")
        .then(function(ret) {
            test.assert(ret.tableName == "yelp/user-votes.funny-projected");
            return xcalarGetTableMeta(thriftHandle,
                                        "yelp/user-votes.funny-projected", false);
        })
        .then(function(metaOutput) {
            test.assert(metaOutput.numValues === 1);
            test.assert(metaOutput.numImmediates === 0);
            return xcalarApiMap(thriftHandle, "votesFunnyPlusUseful",
                                "add(yelp_user::votes.funny, yelp_user::votes.useful)",
                                "yelp/user-votes.funny-gt900",
                                "yelp/user-votes.funny-plus-useful-map");
        })
        .then(function(ret) {
            test.assert(ret.tableName == "yelp/user-votes.funny-plus-useful-map");
            return xcalarGetTableMeta(thriftHandle,
                                        "yelp/user-votes.funny-plus-useful-map", false);
        })
        .then(function(metaOutput) {
            console.log(metaOutput.numValues);
            console.log(metaOutput.numImmediates);
            test.assert(metaOutput.numValues == 3);
            test.assert(metaOutput.numImmediates == 2);
            return xcalarApiMap(thriftHandle, "complimentsFunnyPlusCute",
                                "add(compliments.funny, compliments.cute)",
                                "yelp/user-votes.funny-plus-useful-map",
                                "yelp/user-compliments.funny-plus-cute-map");
        })
        .then(function(ret) {
            test.assert(ret.tableName == "yelp/user-compliments.funny-plus-cute-map");
            return xcalarGetTableMeta(thriftHandle,
                                        "yelp/user-compliments.funny-plus-cute-map", false);
        })
        .then(function(metaOutput) {
            console.log(metaOutput.numValues);
            console.log(metaOutput.numImmediates);
            test.assert(metaOutput.numValues == 4);
            test.assert(metaOutput.numImmediates == 3);
            return xcalarProject(thriftHandle, 2,
                                 ["votesFunnyPlusUseful", "complimentsFunnyPlusCute"],
                                 "yelp/user-compliments.funny-plus-cute-map",
                                 "yelp/projected_two_immediate_columns");
        })
        .then(function(ret) {
            test.assert(ret.tableName == "yelp/projected_two_immediate_columns");
            return xcalarGetTableMeta(thriftHandle,
                                        "yelp/projected_two_immediate_columns", false);
        })
        .then(function(metaOutput) {
            console.log(metaOutput.numValues);
            console.log(metaOutput.numImmediates);
            test.assert(metaOutput.numValues == 2);
            test.assert(metaOutput.numImmediates == 2);
            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testJoin(test) {
        var leftColumn = [];
        var map = new XcalarApiColumnT();
        map.sourceColumn = "yelp_user";
        map.destColumn = "leftDataset";
        map.columnType = "DfFatptr";
        leftColumn.push(map);

        var map = new XcalarApiColumnT();
        map.sourceColumn = "yelp_user-votes.funny";
        map.destColumn = "leftKey";
        map.columnType = "DfInt64";
        leftColumn.push(map);

        var rightColumn = [];
        var map2 = new XcalarApiColumnT();
        map2.sourceColumn = "yelp_user";
        map2.destColumn = "rightDataset";
        map2.columnType = "DfFatptr";
        rightColumn.push(map2);

        xcalarJoin(thriftHandle, "yelp/user-votes.funny-gt900",
                   "yelp/user-votes.funny-gt900",
                   "yelp/user-dummyjoin",
                   JoinOperatorT.InnerJoin,
                   leftColumn, rightColumn,
                   "neq(leftKey, yelp_user-votes.funny)")
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(reason) {
            test.fail(JSON.stringify(reason));
        });
    }

    function testCrossJoin(test) {
        var leftColumn = [];
        var map = new XcalarApiColumnT();
        map.sourceColumn = "yelp_user";
        map.destColumn = "leftDataset";
        map.columnType = "DfFatptr";
        leftColumn.push(map);

        var map = new XcalarApiColumnT();
        map.sourceColumn = "yelp_user-votes.funny";
        map.destColumn = "leftKey";
        map.columnType = "DfInt64";
        leftColumn.push(map);

        var rightColumn = [];
        var map2 = new XcalarApiColumnT();
        map2.sourceColumn = "yelp_user";
        map2.destColumn = "rightDataset";
        map2.columnType = "DfFatptr";
        rightColumn.push(map2);

        xcalarJoin(thriftHandle, "yelp/user-votes.funny-gt900",
                   "yelp/user-votes.funny-gt900",
                   "yelp/user-dummycrossjoin",
                   JoinOperatorT.CrossJoin,
                   leftColumn, rightColumn,
                   "neq(leftKey, yelp_user-votes.funny)")
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(reason) {
            test.fail(JSON.stringify(reason));
        });
    }

    function testUnionAll(test) {
        var columns = [];
        var tables = [];

        xcalarApiMap(thriftHandle, ["map1"], ["int(yelp_user-votes.funny)"],
                     "yelp/user-dummyjoin", "unionTable1map", false)
        .then(function() {
            return xcalarApiMap(thriftHandle, ["map2"], ["int(yelp_user-votes.funny)"],
                     "yelp/user-dummyjoin", "unionTable2map", false);
        })
        .then(function(ret) {
            var t1c = new XcalarApiColumnT();
            t1c.sourceColumn = "map1";
            t1c.destColumn = "mapdest";
            t1c.columnType = "DfInt64";
            var t2c = new XcalarApiColumnT();
            t2c.sourceColumn = "map2";
            t2c.destColumn = "mapdest";
            t2c.columnType = "DfInt64";
            tables.push("unionTable1map");
            tables.push("unionTable2map");
            columns.push([t1c]);
            columns.push([t2c]);
            return xcalarUnion(thriftHandle, tables, "unionAllTest", columns,
                               false, UnionOperatorT.UnionStandard);
        })
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(err) {
            test.fail(JSON.stringify(err));
        });
    }

    function testUnion(test) {
        var columns = [];
        var tables = [];

        // Dedup requires indexed columns
        xcalarIndex(thriftHandle,
                             "unionTable1map",
                             "unionTable1",
                             [new XcalarApiKeyT({name:"map1", type:"DfInt64", keyFieldName:"", ordering:"Unordered"})])
        .then(function() {
            return xcalarIndex(thriftHandle,
                                 "unionTable2map",
                                 "unionTable2",
                                 [new XcalarApiKeyT({name:"map2", type:"DfInt64", keyFieldName:"", ordering:"Unordered"})])
        })
        .then(function(ret) {
            var t1c = new XcalarApiColumnT();
            t1c.sourceColumn = "map1";
            t1c.destColumn = "mapdest";
            t1c.columnType = "DfInt64";
            var t2c = new XcalarApiColumnT();
            t2c.sourceColumn = "map2";
            t2c.destColumn = "mapdest";
            t2c.columnType = "DfInt64";
            tables.push("unionTable1");
            tables.push("unionTable2");
            columns.push([t1c]);
            columns.push([t2c]);

            return xcalarUnion(thriftHandle, tables, "unionStandardTest", columns,
                                   true, UnionOperatorT.UnionStandard)
        })
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(err) {
            test.fail(JSON.stringify(err));
        });
    }


    function testIntersectAll(test) {
        var columns = [];
        var tables = [];

        var t1c = new XcalarApiColumnT();
        t1c.sourceColumn = "map1";
        t1c.destColumn = "mapdest";
        t1c.columnType = "DfInt64";
        var t2c = new XcalarApiColumnT();
        t2c.sourceColumn = "map2";
        t2c.destColumn = "mapdest";
        t2c.columnType = "DfInt64";
        // All set operations require indexed columns
        tables.push("unionTable1");
        tables.push("unionTable2");
        columns.push([t1c]);
        columns.push([t2c]);

        xcalarUnion(thriftHandle, tables, "unionIntersectAllTest", columns,
                               false, UnionOperatorT.UnionIntersect)
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(err) {
            test.fail(JSON.stringify(err));
        });
    }

    function testIntersect(test) {
        var columns = [];
        var tables = [];

        var t1c = new XcalarApiColumnT();
        t1c.sourceColumn = "map1";
        t1c.destColumn = "mapdest";
        t1c.columnType = "DfInt64";
        var t2c = new XcalarApiColumnT();
        t2c.sourceColumn = "map2";
        t2c.destColumn = "mapdest";
        t2c.columnType = "DfInt64";
        // All set operations require indexed columns
        tables.push("unionTable1");
        tables.push("unionTable2");
        columns.push([t1c]);
        columns.push([t2c]);

        xcalarUnion(thriftHandle, tables, "unionIntersectTest", columns,
                               true, UnionOperatorT.UnionIntersect)
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(err) {
            test.fail(JSON.stringify(err));
        });
    }

    function testExceptAll(test) {
        var columns = [];
        var tables = [];

        var t1c = new XcalarApiColumnT();
        t1c.sourceColumn = "map1";
        t1c.destColumn = "mapdest";
        t1c.columnType = "DfInt64";
        var t2c = new XcalarApiColumnT();
        t2c.sourceColumn = "map2";
        t2c.destColumn = "mapdest";
        t2c.columnType = "DfInt64";
        // All set operations require indexed columns
        tables.push("unionTable1");
        tables.push("unionTable2");
        columns.push([t1c]);
        columns.push([t2c]);

        xcalarUnion(thriftHandle, tables, "unionExceptAllTest", columns,
                               false, UnionOperatorT.UnionExcept)
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(err) {
            test.fail(JSON.stringify(err));
        });
    }

    function testExcept(test) {
        var columns = [];
        var tables = [];

        var t1c = new XcalarApiColumnT();
        t1c.sourceColumn = "map1";
        t1c.destColumn = "mapdest";
        t1c.columnType = "DfInt64";
        var t2c = new XcalarApiColumnT();
        t2c.sourceColumn = "map2";
        t2c.destColumn = "mapdest";
        t2c.columnType = "DfInt64";
        // All set operations require indexed columns
        tables.push("unionTable1");
        tables.push("unionTable2");
        columns.push([t1c]);
        columns.push([t2c]);

        xcalarUnion(thriftHandle, tables, "unionExceptTest", columns,
                               false, UnionOperatorT.UnionExcept)
        .then(function(result) {
            printResult(result);
            newTableOutput = result;
            test.pass();
        })
        .fail(function(err) {
            test.fail(JSON.stringify(err));
        });
    }

    function testGetOpStats(test) {
        test.trivial(xcalarApiGetOpStats(thriftHandle, "yelp/user-dummyjoin"));
    }

    function testQuery(test) {
        var query = "index --key votes.funny --dataset " + datasetPrefix +
                    "yelp" + " --dsttable yelp-votesFunnyTable --prefix p; index --key " +
                    "yelp_user::review_count" +
                    " --srctable yelp-votesFunnyTable --dsttable " +
                    "yelp-review_countTable;" +
                    "  map --eval \"add(1,2)\"  --srctable yelp-votesFunnyTable"
                    + " --fieldName newField --dsttable yelp-mapTable;" +
                    " filter yelp-mapTable \" sub(2,1)\" yelp-filterTable;" +
                    " groupBy --srctable yelp-filterTable --eval " +
                    "\"avg(yelp_user::votes.cool)\" --fieldName avgCool --dsttable " +
                    "yelp-groupByTable;" +
                    " join --leftTable yelp-review_countTable --rightTable" +
                    "  yelp-groupByTable --joinTable " + queryTableName;

        queryName = "testQuery";

        test.trivial(xcalarQuery(thriftHandle, queryName, query, false));
    }

    function testGetDagOnAggr(test) {
        var query = "index --key xcalarRecordNum --dataset " + origDataset +
                    " --dsttable yelpUsers#js0 --prefix p;" +
                    "aggregate --srctable yelpUsers#js0 --dsttable " +
                    "yelpUsers-aggregate#js1 --eval \"count(yelp_user::review_count)\"";

        var locaQueryName = "aggr query";

        console.log("submit query" + query);
        xcalarQuery(thriftHandle, locaQueryName, query, true)
        .done(function(queryOutput) {
            printResult(queryOutput);

            (function wait() {
              setTimeout(function() {
                xcalarQueryState(thriftHandle, locaQueryName, true)
                .then(function(result) {
                    var qrStateOutput = result;
                    if (qrStateOutput.queryState === QueryStateT.qrProcssing) {
                        return wait();
                    }

                    if (qrStateOutput.queryState === QueryStateT.qrFinished) {
                        console.log("call get dag on aggr");
                        return xcalarDag(thriftHandle,  "yelpUsers-aggregate#js1");
                    }

                    test.fail("qrStateOutput.queryState = " +
                              QueryStateTStr[qrStateOutput.queryState]);
                })
                .then(function(dagOutput) {
                    console.log("dagOutput.numNodes = " + dagOutput.numNodes);
                    test.assert(dagOutput.numNodes === 3, undefined,
                                "the number of dag node returned is incorrect");
                })
                .fail(test.fail);
              }, 1000);
            })();

        })
        .fail(test.fail);
    }

    function testQueryState(test) {
        test.trivial(xcalarQueryState(thriftHandle, queryName, true));
    }

    function waitForDag(test) {
        var queryStateOutput;

        (function wait() {
            setTimeout(function() {
                xcalarQueryState(thriftHandle, queryName, false)
                .done(function(result) {
                    queryStateOutput = result;
                    if (queryStateOutput.queryState ===
                                                      QueryStateT.qrProcssing) {
                        return wait();
                    }

                    if (queryStateOutput.queryState ===
                                                       QueryStateT.qrFinished) {
                        test.pass();
                    } else {
                        var reason = "queryStateOutput.queryState = " +
                                    QueryStateTStr[queryStateOutput.queryState];
                        test.fail(reason);
                    }
                 })
                 .fail(function(reason) {
                     test.fail(reason);
                 });
             }, 1000);
         })();
    }

    function testDag(test) {
        xcalarDag(thriftHandle,  queryTableName)
        .done(function(dagOutput) {
            console.log("dagOutput.numNodes = " + dagOutput.numNodes);
            test.assert(dagOutput.numNodes === 9, undefined,
                        "the number of dag node returned is incorrect");
            test.pass();
        })
        .fail(test.fail);
    }

    function testTagDagNodes(test) {
        var tableName = "yelp/user-review_count";
        var table = new XcalarApiNamedInputT();
        table.name = tableName;
        xcalarTagDagNodes(thriftHandle, "testTag", [table])
        .then(function() {
            return xcalarDag(thriftHandle, table.name);
        })
        .then(function(dagOutput) {
            var nodeId;
            for (var ii = 0; ii < dagOutput.numNodes; ii++) {
                if (dagOutput.node[ii].name.name == table.name) {
                    test.assert(dagOutput.node[ii].tag == "testTag");
                    nodeId = dagOutput.node[ii].dagNodeId;
                }
            }

            table = new XcalarApiNamedInputT();
            table.name = "";
            table.nodeId = nodeId;

            return xcalarTagDagNodes(thriftHandle, "testTag2", [table])
        })
        .then(function() {
            return xcalarDag(thriftHandle, tableName);
        })
        .then(function(dagOutput) {
            for (var ii = 0; ii < dagOutput.numNodes; ii++) {
                if (dagOutput.node[ii].dagNodeId == table.nodeId) {
                    test.assert(dagOutput.node[ii].tag == "testTag2");
                }
            }

            test.pass();
        })
        .fail(test.fail);
    }

    function testCommentDagNodes(test) {
        var tableName = "yelp/user-review_count"
        xcalarCommentDagNodes(thriftHandle, "testComment", 1, [tableName])
        .then(function() {
            return xcalarDag(thriftHandle,  tableName);
        })
        .then(function(dagOutput) {
            for (var ii = 0; ii < dagOutput.numNodes; ii++) {
                if (dagOutput.node[ii].name.name == tableName) {
                    test.assert(dagOutput.node[ii].comment == "testComment");
                }
            }
            test.pass();
        })
        .fail(test.fail);
    }

    function testGroupBy(test) {
        test.trivial(xcalarGroupBy(thriftHandle, "yelp/user-votes.funny-gt900",
                      "yelp/user-votes.funny-gt900-average",
                      "avg(yelp_user::votes.funny)", "averageVotesFunny", true));
    }

    function testAggregate(test) {
        aggrTable = "aggrTable";
        xcalarAggregate(thriftHandle, origStrTable, aggrTable, "sum(yelp_user::fans)")
        .done(function(aggregateOutput) {
            console.log("jsonAnswer: " + JSON.stringify(aggregateOutput) +
                        "\n");
            var jsonAnswer = aggregateOutput;
            test.assert(jsonAnswer.Value === 114674, undefined,
                        "jsonAnswer !== 114674");
            test.pass();
        })
        .fail(test.fail);
    }

    function testMap(test) {
        var resultSetFromMapTable = -1;
        xcalarApiMap(thriftHandle, "votedCoolTimesFunny",
                     "mult(yelp_user::votes.cool, yelp_user::votes.funny)",
                     origTable,
                     "yelp/user-votes.cool-times-funny-map")
        .then(function(ret) {
            test.assert(ret.tableName === "yelp/user-votes.cool-times-funny-map");
            // sorting the values to be able to predictably assert on the return from map
            // NOTE: sorting must be done AFTER map command - sorting won't be preserved
            // if we do sort, *then* map!!
            return xcalarIndex(thriftHandle,
                               ret.tableName,
                               "yelp/voted.cool-times-funny-sortedby-most_reviewed",
                               [new XcalarApiKeyT({name:"yelp_user::review_count", type:"DfInt64", keyFieldName:"", ordering:"Descending"})])
        })
        .then(function(ret) {
            test.assert(ret.tableName === "yelp/voted.cool-times-funny-sortedby-most_reviewed");
            return xcalarMakeResultSetFromTable(thriftHandle,
                                                ret.tableName);
        })
        .then(function(ret) {
            test.assert(ret.numEntries === 70817);
            resultSetFromMapTable = ret;
            return xcalarResultSetAbsolute(thriftHandle,
                                           ret.resultSetId, 0);
        })
        .then(function(ret) {
            return xcalarResultSetNext(thriftHandle,
                                       resultSetFromMapTable.resultSetId, 10);
        })
        .then(function(ret) {
            test.assert(ret.numValues > 0);

            return xcalarFreeResultSet(thriftHandle, resultSetFromMapTable.resultSetId);
        })
        .then(function(ret) {
            test.pass();
        })
        .fail(test.fail);
    }

    function testApiGetRowNum(test) {
        test.trivial(xcalarApiGetRowNum(thriftHandle, "rowNum",
                           "yelp/user-votes.funny-gt900",
                           "yelp/user-votes.funny-rowNum"));
    }

    function testApiSynthesize(test) {
        test.trivial(xcalarApiSynthesize(thriftHandle,
                           "yelp/user-votes.funny-gt900",
                           "yelp/user-votes.funny-synthesize", []));
    }

    function testDestroyDatasetInUse(test) {
        xcalarDeleteDagNodes(thriftHandle, loadOutput.dataset.name, SourceTypeT.SrcDataset)
        .then(function(status) {
            var reason = "Destroyed dataset in use succeeded when "+
                         "it should have failed";
            test.fail(reason);
        })
        .fail(function(status) {
            if (status === StatusT.StatusDgNodeInUse) {
                test.pass();
            } else {
                test.fail(StatusTStr[status]);
            }
        });
    }

    function testExportCSV(test) {
        var target = new ExExportTargetHdrT();
        target.type = ExTargetTypeT.ExTargetSFType;
        target.name = "Default";
        var columnNames = ["yelp_user::user_id", "yelp_user::name"];
        var headerColumns = ["id_of_user", "user name"];
        var columns = columnNames.map(function (e, i) {
            var col = new XcalarApiExportColumnT();
            col.columnName = columnNames[i];
            col.headerName = headerColumns[i];
            return col;
        });

        xcalarExport(thriftHandle, "yelp/user-votes.funny-gt900",
                     "single_csv", {"target": "Default Shared Root", "file_path": "/tmp/mgmtdtestfile.csv"},
                     columns, "exportNamexoxo")
        .then(function(retStruct) {
            var status = retStruct.status;
            printResult(status);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testExportCancel(test) {
        var specInput = new ExInitExportSpecificInputT();
        specInput.sfInput = new ExInitExportSFInputT();
        specInput.sfInput.fileName = "yelp-mgmtdTest" +
                                     Math.floor(Math.random()*1000 + 10000) + ".csv";
        specInput.sfInput.splitRule = new ExSFFileSplitRuleT();
        specInput.sfInput.splitRule.type = ExSFFileSplitTypeT.ExSFFileSplitForceSingle;
        specInput.sfInput.headerType = ExSFHeaderTypeT.ExSFHeaderEveryFile;
        specInput.sfInput.format = DfFormatTypeT.DfFormatCsv;
        specInput.sfInput.formatArgs = new ExInitExportFormatSpecificArgsT();
        specInput.sfInput.formatArgs.csv = new ExInitExportCSVArgsT();
        specInput.sfInput.formatArgs.csv.fieldDelim = ",";
        specInput.sfInput.formatArgs.csv.recordDelim = "\n";
        specInput.sfInput.formatArgs.csv.quoteDelim = "\"";

        console.log("\texport file name = " + specInput.sfInput.fileName);
        var target = new ExExportTargetHdrT();
        target.type = ExTargetTypeT.ExTargetSFType;
        target.name = "Default";
        var numColumns = 4;
        var columnNames = ["yelp_user::votes.funny", "yelp_user::votes.useful", "yelp_user::user_id", "yelp_user::text"];
        var headerColumns = ["votes.funny", "votes.useful", "id_of_user", "Review Contents"];
        var columns = columnNames.map(function (e, i) {
            var col = new ExColumnNameT();
            col.name = columnNames[i];
            col.headerAlias = headerColumns[i];
            return col;
        });

        function exportAndCancel(indexOutput) {
            var isExportDone=false;
            var isCancelDone=false;

            function tryAgain() {
                xcalarDeleteDagNodes(thriftHandle, "yelp/reviews-votes.funny-export-cancel",
                                     SourceTypeT.SrcExport)
                .then(function(deleteDagNodeOutput) {
                    if (deleteDagNodeOutput.numNodes != 1) {
                        test.fail("Number of nodes deleted != 1 (" + deleteDagNodeOutput.numNodes + ")");
                    } else if (deleteDagNodeOutput.statuses[0].status != StatusT.StatusOk) {
                        test.fail("Error deleting dag node. Status: " + StatusTStr[deleteDagNodeOutput.statuses[0].status] + deleteDagNodeOutput.statuses[0].status);
                    } else {
                        exportAndCancel(indexOutput);
                    }
                })
                .fail(function(reason) {
                    test.fail("Failed to drop dag node. Reason: " + StatusTStr[reason.xcalarStatus]);
                });
            }

            console.log("Index done. Starting both export and cancel now");
            xcalarExport(thriftHandle, "yelp/reviews-votes.funny",
                         target, specInput,
                         ExExportCreateRuleT.ExExportDeleteAndReplace,
                         true, numColumns,
                         columns, "yelp/reviews-votes.funny-export-cancel")
            .then(function(retStruct) {
                console.log("Export succeeded when it was supposed to be cancelled. Trying again");
                isExportDone=true;
                if (isCancelDone === true) {
                    tryAgain();
                }
            })
            .fail(function(reason) {
                if (reason.xcalarStatus === StatusT.StatusCanceled) {
                    test.pass();
                } else {
                    test.fail("Export failed with reason: " + StatusTStr[reason.xcalarStatus]);
                }
            });

            setTimeout(function () {
                xcalarApiCancelOp(thriftHandle, "yelp/reviews-votes.funny-export-cancel")
                .then(function(status) {
                    isCancelDone=true;
                    if (isExportDone === true) {
                        tryAgain();
                    }
                })
                .fail(function(status) {
                    if (status != StatusT.StatusOperationHasFinished &&
                        status != StatusT.StatusDagNodeNotFound) {
                        var reason = "Export cancel failed with status: " + StatusTStr[status];
                        test.fail(reason);
                    } else {
                        isCancelDone=true;
                        if (isExportDone === true) {
                            tryAgain();
                        }
                    }
                });
            }, 50);
        }

        xcalarIndex(thriftHandle,
                    yelpReviewsDataset,
                    "yelp/reviews-votes.funny",
                    [new XcalarApiKeyT({name:"votes.funny", type:"DfInt64", keyFieldName:"", ordering:"Ascending"})],
                    "yelp_user")
        .then(exportAndCancel)
        .fail(function(reason) {
            test.fail("Index of reviews dataset failed with: " + StatusTStr[reason.xcalarStatus] +  " (" + reason + ")");
        });
    }

    function testMakeRetina(test) {
        retinaName = "yelpRetina-1";
        var dstTable = new XcalarApiRetinaDstT();
        dstTable.numColumns = 3;
        var columnNames = ["yelp_user::user_id", "yelp_user::name", "yelp_user::votes.funny"];
        var headerColumns = ["User ID", "User Name", "Number of Funny Votes"];
        var columns = columnNames.map(function (e, i) {
            var col = new ExColumnNameT();
            col.name = columnNames[i];
            col.headerAlias = headerColumns[i];
            return col;
        });
        dstTable.columns = columns;
        dstTable.target = new XcalarApiNamedInputT();
        dstTable.target.name = "yelp/user-votes.funny-gt900-average";
        dstTable.target.isTable = true;
        xcalarMakeRetina(thriftHandle, retinaName, [dstTable])
        .then(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(reason) {
            if (reason.xcalarStatus === StatusT.StatusRetinaAlreadyExists) {
                console.log("Retina " + retinaName + " already exists. Deleting and trying again")
                xcalarApiDeleteRetina(thriftHandle, retinaName)
                .then(function() {
                    testMakeRetina(test);
                })
                .fail(function(reason) {
                    reason = "deleteRetina failed with status: " + StatusTStr[reason.xcalarStatus];
                    test.fail(reason);
                })
            } else {
                reason = "makeRetina failed with status: " + StatusTStr[reason.xcalarStatus];
                test.fail(reason);
            }
        });
    }

    function testListRetinas(test) {
        xcalarListRetinas(thriftHandle, "yelpRetina*")
        .then(function(listRetinasOutput) {
            var foundRetina = false;
            printResult(listRetinasOutput);
            for (var i = 0; i < listRetinasOutput.numRetinas; i ++) {
                if (listRetinasOutput.retinaDescs[i].retinaName == retinaName) {
                    foundRetina = true;
                }
                console.log("\tretinaDescs[" + i + "].retinaName = " +
                            listRetinasOutput.retinaDescs[i].retinaName);
            }
            test.assert(foundRetina, undefined,
                        "Could not find retina \"" + retinaName + "\"");
            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testGetRetina(iter, test) {
        xcalarGetRetina(thriftHandle, retinaName)
        .done(function(getRetinaOutput) {
            printResult(getRetinaOutput);

            console.log("\tretinaName: " +
                        getRetinaOutput.retina.retinaDesc.retinaName);
            console.log("\tnumNodes: " +
                        getRetinaOutput.retina.retinaDag.numNodes);

            for (var ii = 0; ii < getRetinaOutput.retina.retinaDag.numNodes;
                 ii++) {
                console.log("\tnode[" + ii + "].dagNodeId = " +
                            getRetinaOutput.retina.retinaDag.node[ii].dagNodeId);
                console.log("\tnode[" + ii + "].api = " +
                            XcalarApisTStr[getRetinaOutput.retina.retinaDag.
                            node[ii].api]);
                console.log("\tnode[" + ii + "].apiInputSize = " +
                            getRetinaOutput.retina.retinaDag.node[ii].inputSize);
                switch (getRetinaOutput.retina.retinaDag.node[ii].api) {
                case XcalarApisT.XcalarApiFilter:
                    console.log("\tnode[" + ii + "].input = " + JSON.stringify(getRetinaOutput.retina.retinaDag.node[ii].input.filterInput))
                    console.log("\tnode[" + ii + "].filterStr = " +
                                getRetinaOutput.retina.retinaDag.node[ii].
                                input.filterInput.eval[0].evalString);
                    if (iter == 2) {
                        test.assert(getRetinaOutput.retina.retinaDag.node[ii].
                                    input.filterInput.eval[0].evalString ===
                                    retinaFilterStr, undefined,
                               "FilterStr does not match parameterized string");
                    }

                    break;
                case XcalarApisT.XcalarApiBulkLoad:
                    console.log("\tnode[" + ii + "].datasetUrl = " +
                                getRetinaOutput.retina.retinaDag.node[ii].input.
                                loadInput.url);
                    break;
                default:
                    break;
                }
            }

            test.pass();
        })
        .fail(test.fail);
    }

    function testGetRetina1(test) {
        return (testGetRetina(1, test));
    }

    function testGetRetina2(test) {
        return (testGetRetina(2, test));
    }

    function testUpdateRetina(test) {
        xcalarGetRetinaJson(thriftHandle, retinaName)
        .then(function(jsonOutput) {
            printResult(jsonOutput);

            var json = JSON.parse(jsonOutput.retinaJson);

            for (var ii = 0; ii < json.query.length; ii++) {
                if (json.query[ii].operation === "XcalarApiFilter") {
                    json.query[ii].args.eval[0].evalString =
                        retinaFilterStr;
                }
            }

            return (xcalarUpdateRetina(thriftHandle, retinaName,
                                       JSON.stringify(json)));
        })
        .then(function(status) {
            test.pass();
        })
        .fail(test.fail);
    }

    function testExecuteRetina(test) {
        var parameters = [];
        parameters.push(new XcalarApiParameterT({ paramName: "foo",
                                                  paramValue: "1000" }));

        xcalarExecuteRetina(thriftHandle, retinaName, parameters, false, "")
        .then(function(status) {
            test.pass();
        })
        .fail(function(error) {
            var reason = "xcalarExecuteRetina failed with reason: " +
                         StatusTStr[error];
            test.fail(reason);
        });
    }

    function testCancelRetina(test) {
        var parameters = [];
        var cancelQueryName = "CancelQuery";
        parameters.push(new XcalarApiParameterT({ paramName: "foo",
                                                  paramValue: "1000" }));

        function retinaAndCancel() {
            console.log("starting executeRetina and cancel");

            xcalarExecuteRetina(thriftHandle, retinaName, parameters, false, "", cancelQueryName)
            .then(function(status) {
                console.log("Retina succeeded when it was supposed to be cancelled. Trying again");
                retinaAndCancel();
            })
            .fail(function(reason) {
                if (reason.xcalarStatus === StatusT.StatusCanceled) {
                    xcalarQueryState(thriftHandle, cancelQueryName, false)
                    .then(function(result) {
                        var qrStateOutput = result;
                        if (qrStateOutput.queryState != QueryStateT.qrCancelled) {
                            test.fail("not canceled qrStateOutput.queryState = " +
                                      QueryStateTStr[qrStateOutput.queryState]);
                        }

                        test.pass();
                    })
                    .fail(function(status) {
                        test.fail(StatusTStr[status.xcalarStatus]);
                    });
                } else if (reason.xcalarStatus === StatusT.StatusQrQueryInUse) {
                    console.log("Retina did not get the chance to run.  Trying again");
                    retinaAndCancel();
                } else {
                    test.fail("ExecuteRetina failed with reason: " + StatusTStr[reason.xcalarStatus]);
                }
            });

            setTimeout(function(){
                cancelRetina();
            }, 100);

            function cancelRetina() {
                xcalarQueryCancel(thriftHandle, cancelQueryName)
                .then(function(cancelStatus) {
                    console.log("Retina cancel succeeded");
                })
                .fail(function(reason) {
                    cancelRetina();
                });
            }
        }

        retinaAndCancel()
    }

    function testListParametersInRetina(test) {
        xcalarListParametersInRetina(thriftHandle, retinaName)
        .done(function(listParametersInRetinaOutput) {
            printResult(listParametersInRetinaOutput);

            console.log("\tnumParameters: " +
                        listParametersInRetinaOutput.numParameters);
            for (var i = 0; i < listParametersInRetinaOutput.numParameters;
                 i++) {
                console.log("\tparameters[" + i + "].paramName = " +
                            listParametersInRetinaOutput.parameters[i].
                            paramName);
                console.log("\tparameters[" + i + "].paramValue = " +
                            listParametersInRetinaOutput.parameters[i].
                            paramValue);
            }

            if (listParametersInRetinaOutput.numParameters == 1 &&
                listParametersInRetinaOutput.parameters[0].paramName ==
                "foo") {
                test.pass();
            } else {
                var reason = "list Parameters seems wrong";
                test.fail(reason);
            }
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testDeleteRetina(test) {
        xcalarListRetinas(thriftHandle)
        .then(function(listRetinasOutput) {
            function makeDeleteOneRetina(ii) {
                return (function() {
                    if (ii == listRetinasOutput.numRetinas) {
                        test.pass();
                    } else {
                        console.log("Deleting ", listRetinasOutput.retinaDescs[ii].retinaName);
                        xcalarApiDeleteRetina(thriftHandle, listRetinasOutput.retinaDescs[ii].retinaName)
                        .done(makeDeleteOneRetina(ii + 1))
                        .fail(function(reason) {
                            test.fail("Error while deleting " + listRetinasOutput.retinaDescs[ii].retinaName + ": " + StatusTStr[reason.xcalarStatus] + " (" + reason + ")");
                        });
                    }
                });
            }

            (makeDeleteOneRetina(0))();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testListFiles(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = "/";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        xcalarListFiles(thriftHandle, sourceArgs)
        .done(function(listFilesOutput) {
            printResult(listFilesOutput);

            for (var i = 0, file = null; i < listFilesOutput.numFiles; i ++) {
                file = listFilesOutput.files[i];

                console.log("\tfile[" + i.toString() + "].name = " + file.name);
                console.log("\tfile[" + i.toString() + "].attr.size = " +
                    file.attr.size.toString());
                console.log("\tfile[" + i.toString() + "].attr.isDirectory = " +
                    file.attr.isDirectory.toString());
            }

            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    // Witness to bug 10624 -- list files in a non-existent directory should
    // return error in the Txn log.
    function testTxnLog(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = "/NotThere";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        xcalarListFiles(thriftHandle, sourceArgs)
        .done(function(listFilesOutput) {
            printResult(listFilesOutput);
            test.fail("Unexpected success listing non-existent files");
        })
        .fail(function(listFilesOutput) {
            printResult(listFilesOutput);

            test.assert(listFilesOutput.log != "");
            test.pass();
        });
    }

    // Witness to bug 2020
    function testApiMapStringToString(test) {
        var evalString = "string(yelp_user::user_id)";

        xcalarApiMap(thriftHandle, "castUserId", evalString, origTable,
                     "user_id2")
        .done(function(filterOutput) {
            test.pass();
        })
        .fail(test.fail);
    }

    // Witness to bug 8711
    function testApiMapInPlaceReplace(test) {
        var evalString = "string(yelp_user::user_id2)";
        test.trivial(xcalarApiMap(thriftHandle, "castUserId", evalString,
                                  "user_id2", "inplaceReplace"));
    }

    // Witness to bug 238
    function testApiMapLongEvalString(test) {
        var evalString = "add(yelp_user::votes.funny, 1)";
        while (evalString.length <= XcalarApisConstantsT.XcalarApiMaxEvalStringLen) {
            evalString = "add(1, " + evalString + ")";
        }

        xcalarApiMap(thriftHandle, "DoesNotExist", evalString, origTable,
                     "ShouldNotExist")
        .done(function(filterOutput) {
            returnValue = 1;
            var reason = "Map succeeded with long eval string when it should have failed";
            test.fail(reason);
        })
        .fail(function(reason) {
            if (reason.xcalarStatus === StatusT.StatusEvalStringTooLong) {
                test.pass();
            } else {
                reason = "Map returned status " + StatusTStr[reason.xcalarStatus] + " (" + reason + ")";
                test.fail(reason);
            }
        });
    }

    function testApiFilterLongEvalString(test) {
        var evalString = "add(yelp_user::votes.funny, 1)";
        while (evalString.length <= XcalarApisConstantsT.XcalarApiMaxEvalStringLen) {
            evalString = "add(1, " + evalString + ")";
        }

        xcalarFilter(thriftHandle, evalString, origTable, "filterLongEvalStr")
        .done(function(filterOutput) {
            returnValue = 1;
            var reason = "Map succeeded with long eval string when it should have failed";
            test.fail(reason);
        })
        .fail(function(reason) {
            if (reason.xcalarStatus === StatusT.StatusEvalStringTooLong) {
                test.pass();
            } else {
                test.fail(reason);
            }
        });
    }

    function testApiKeyAddOrReplace(test, keyName, keyValue) {
        xcalarKeyAddOrReplace(thriftHandle,
                              XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                              keyName, keyValue, true)
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });

    }

    function testApiKeyInvalidScope(test) {
        // XXX Remove once XcalarApiKeyScopeUser is implemented.
        xcalarKeyAddOrReplace(thriftHandle,
                              XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeUser,
                              "foo", "foobar", false)
        .done(function(status) {
            test.fail("Expected failure with scope XcalarApiKeyScopeUser.");
        })
        .fail(function(reason) {
            if (reason.xcalarStatus !== StatusT.StatusUnimpl) {
                test.fail(reason);
            }
            xcalarKeyAddOrReplace(thriftHandle, 666, "foo", "foobar", false)
            .done(function(status) {
                test.fail("Expected failure given invalid scope.");
            })
            .fail(function(reason) {
                if (reason.xcalarStatus === StatusT.StatusInval) {
                    test.pass();
                } else {
                    test.fail(reason);
                }
            });
        });
    }

    function testApiKeyAdd(test) {
        testApiKeyAddOrReplace(test, "mykey", "myvalue1");
    }

    function testApiKeyReplace(test) {
        testApiKeyAddOrReplace(test, "mykey", "myvalue2");
    }

    function testApiKeyAppend(test) {
        // Insert original key
        xcalarKeyAddOrReplace(thriftHandle,
                              XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                              "myotherkey", "a", false)
        .then(function() {
            // Append first 'a'
            return xcalarKeyAppend(thriftHandle,
                                   XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                   "myotherkey", "a");
        })
        .then(function() {
            // Append second 'a'
            return xcalarKeyAppend(thriftHandle,
                                   XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                   "myotherkey", "a");
        })
        .then(function() {
            // Lookup. Make sure result is 'aaa'
            return xcalarKeyLookup(thriftHandle,
                                   XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                   "myotherkey");
        })
        .then(function(lookupOutput) {
            if (lookupOutput.value != "aaa") {
                var reason = "wrong value. got \"" + lookupOutput.value + "\" instead of \"aaa\"";
                test.fail(reason);
            } else {
                test.pass();
            }
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testApiKeyList(test) {
        var keyname = "testListKeyMgmtd";
        // Insert original key
        xcalarKeyAddOrReplace(thriftHandle,
                              XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                              keyname, "a", true)
        .then(function() {
            // Get list of keys using this keyname as a regex
            return xcalarKeyList(thriftHandle,
                                 XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                 keyname);
        })
        .then(function(keyList) {
            test.assert(keyList.keys.indexOf(keyname) != -1);
            return xcalarKeyDelete(thriftHandle,
                                   XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal, keyname);
        })
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(result) {
            test.fail(StatusTStr[result["xcalarStatus"]]);
        });
    }

    function testApiKeySetIfEqual(test) {
        // Insert original key
        xcalarKeyAddOrReplace(thriftHandle,
                              XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                              "yourkey", "b", false)
        .then(function() {
            // Try replacing with incorrect oldValue
            xcalarKeySetIfEqual(thriftHandle,
                                XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                false, "yourkey", "wrongvalue", "x", "x", "x")
            .then(function() {
                var reason = "Expected failure due to incorrect oldValue.";
                test.fail(reason);
            })
            .fail(function(reason) {
                // Try replacing with correct oldValue
                xcalarKeySetIfEqual(thriftHandle,
                                   XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                   false, "yourkey", "b", "c", "x", "y")
                .then(function() {
                    // Lookup. Make sure result is as expected
                    return xcalarKeyLookup(thriftHandle,
                                   XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                   "yourkey");
                })
                .then(function(lookupOutput) {
                    if (lookupOutput.value != "c") {
                        test.fail("Wrong value. Got '" + lookupOutput.value +
                                  "'. Expected 'c'.");
                    } else {
                        return xcalarKeyLookup(thriftHandle,
                                     XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                     "x");
                    }
                })
                .then(function(lookupOutput) {
                    if (lookupOutput.value != "y") {
                        test.fail("Wrong value. Got '" + lookupOutput.value +
                                  "'. Expected 'y'.");
                    } else {
                        xcalarKeySetIfEqual(thriftHandle,
                                     XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                     false, "x", "y", "z")
                        .then(function() {
                            xcalarKeyLookup(thriftHandle,
                                     XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                                     "x")
                            .then(function(lookupOutput) {
                                if (lookupOutput.value != "z") {
                                    test.fail("Wrong value. Got '" +
                                         lookupOutput.value +
                                         "'. Expected 'z'.");
                                } else {
                                    test.pass();
                                }
                            })
                            .fail(function(reason) {
                                test.fail(reason);
                            });
                        })
                        .fail(function(reason) {
                            test.fail(reason);
                        });
                    }
                })
                .fail(function(reason) {
                    test.fail(reason);
                });
            });
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testApiKeyLookup(test) {
        xcalarKeyLookup(thriftHandle,
                        XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                        "mykey")
        .done(function(lookupOutput) {
            printResult(lookupOutput);
            if (lookupOutput.value != "myvalue2") {
                var reason = "wrong value. got \"" + lookupOutput.value + "\" instead of \"myvalue2\"";
                test.fail(reason);
            } else {
                test.pass();
            }
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testApiKeyDelete(test) {
        xcalarKeyDelete(thriftHandle,
                        XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal, "mykey")
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testApiKeyBogusLookup(test) {
        xcalarKeyLookup(thriftHandle,
                        XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal,
                        "mykey")
        .done(function(lookupOutput) {
            printResult(lookupOutput);
            var reason = "lookup did not fail";
            test.fail(reason);
        })
        .fail(function(reason) {
            test.pass();
        });
    }

    function testApiKeySessions(test) {
        session1 = "mgmtdTestApiKeySessions1" + (new Date().getTime());
        session2 = "mgmtdTestApiKeySessions2" + (new Date().getTime());
        session4 = "mgmtdTestInactivateSession" + (new Date().getTime());

        var keyName = "sessionKey";

        xcalarApiSessionList(thriftHandle, "*")
        .then(function(ret) {
            return xcalarApiSessionDelete(thriftHandle, "*")
            .always(function() {
                // Start in brand new sesion...
                xcalarApiSessionNew(thriftHandle, session1, false, "")
                .then(function() {
                    // ..activate it since a new session is inactive on birth
                    return xcalarApiSessionActivate(thriftHandle, session1);
                })
                .then(function() {
                    // Create and activate session4 - ready for testSessionInact
                    xcalarApiSessionNew(thriftHandle, session4, false, "")
                    .then(function() {
                        return xcalarApiSessionActivate(thriftHandle, session4);
                    })
                })
                .then(function() {
                    // ... and add a key.
                    setSessionName(session1)
                    return xcalarKeyAddOrReplace(thriftHandle,
                                                 XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeSession,
                                                 keyName, "x", false);
                })
                .then(function() {
                    // Make sure it exists in this session.
                    return xcalarKeyLookup(thriftHandle,
                                           XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeSession,
                                           keyName);
                })
                .then(function(lookupOutput) {
                    if (lookupOutput.value === "x") {
                        // Create a new session and switch to it.
                        return xcalarApiSessionNew(thriftHandle, session2, false, "");
                    } else {
                        test.fail("Failed lookup. Expected x got " + lookupOutput.value);
                    }
                })
                .then(function(sessionNewRet) {
                    session2Id = sessionNewRet.output.outputResult.
                        sessionNewOutput.sessionId
                    return xcalarApiSessionActivate(thriftHandle, session2);
                })
                .then (function() {
                    // Make sure the key we created in the other session doesn't turn up
                    // in this one.
                    setSessionName(session2)
                    xcalarKeyLookup(thriftHandle,
                                    XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeSession,
                                    keyName)
                    .then(function() {
                        test.fail("Lookup in session2 should have failed.");
                    })
                    .fail(function(reason) {
                        setSessionName(sessionName)
                        test.pass();
                    });
                })
                .fail(function(reason) {
                    test.fail(StatusTStr[reason.xcalarStatus]);
                });
            });
        }, function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testTop(test) {
        xcalarApiTop(thriftHandle, XcalarApisConstantsT.XcalarApiDefaultTopIntervalInMs,
                        XcalarApisConstantsT.XcalarApiDefaultCacheValidityInMs)
        .done(function(topOutput) {
            var ii;
            printResult(topOutput);
            for (ii = 0; ii < topOutput.numNodes; ii++) {
                console.log("\tNode Id: ", topOutput.topOutputPerNode[ii].nodeId);
                console.log("\tCpuUsage(%): ", topOutput.topOutputPerNode[ii].cpuUsageInPercent);
                console.log("\tMemUsage(%): ", topOutput.topOutputPerNode[ii].memUsageInPercent);
                console.log("\tMemUsed: ", topOutput.topOutputPerNode[ii].memUsedInBytes);
                console.log("\tMemAvailable: ", topOutput.topOutputPerNode[ii].totalAvailableMemInBytes);
                console.log("\tnetworkRecvInBytesPerSec: ", topOutput.topOutputPerNode[ii].networkRecvInBytesPerSec);
                console.log("\tnetworkSendInBytesPerSec: ", topOutput.topOutputPerNode[ii].networkSendInBytesPerSec);
                console.log("\txdbUsedBytes: ", topOutput.topOutputPerNode[ii].xdbUsedBytes);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].xdbTotalBytes);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].parentCpuUsageInPercent);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].childrenCpuUsageInPercent);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].numCores);
                console.log("\tsysSwapUsedInBytes: ", topOutput.topOutputPerNode[ii].sysSwapUsedInBytes);
                console.log("\tsysSwapTotalInBytes: ", topOutput.topOutputPerNode[ii].sysSwapTotalInBytes);
                console.log("\tuptimeInSeconds: ", topOutput.topOutputPerNode[ii].uptimeInSeconds);
                console.log("\tdatasetUsedBytes: ", topOutput.topOutputPerNode[ii].datasetUsedBytes);
                console.log("\tsysMemUsedInBytes: ", topOutput.topOutputPerNode[ii].sysMemUsedInBytes);
                console.log("\n\n");
            }
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testPerNodeTop(test) {
        xcalarApiLocalTop(thriftHandle, XcalarApisConstantsT.XcalarApiDefaultTopIntervalInMs,
                        XcalarApisConstantsT.XcalarApiDefaultCacheValidityInMs)
        .done(function(topOutput) {
            var ii;
            printResult(topOutput);
            for (ii = 0; ii < topOutput.numNodes; ii++) {
                console.log("\tNode Id: ", topOutput.topOutputPerNode[ii].nodeId);
                console.log("\tCpuUsage(%): ", topOutput.topOutputPerNode[ii].cpuUsageInPercent);
                console.log("\tMemUsage(%): ", topOutput.topOutputPerNode[ii].memUsageInPercent);
                console.log("\tMemUsed: ", topOutput.topOutputPerNode[ii].memUsedInBytes);
                console.log("\tMemAvailable: ", topOutput.topOutputPerNode[ii].totalAvailableMemInBytes);
                console.log("\tnetworkRecvInBytesPerSec: ", topOutput.topOutputPerNode[ii].networkRecvInBytesPerSec);
                console.log("\tnetworkSendInBytesPerSec: ", topOutput.topOutputPerNode[ii].networkSendInBytesPerSec);
                console.log("\txdbUsedBytes: ", topOutput.topOutputPerNode[ii].xdbUsedBytes);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].xdbTotalBytes);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].parentCpuUsageInPercent);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].childrenCpuUsageInPercent);
                console.log("\txdbTotalBytes: ", topOutput.topOutputPerNode[ii].numCores);
                console.log("\tsysSwapUsedInBytes: ", topOutput.topOutputPerNode[ii].sysSwapUsedInBytes);
                console.log("\tsysSwapTotalInBytes: ", topOutput.topOutputPerNode[ii].sysSwapTotalInBytes);
                console.log("\tuptimeInSeconds: ", topOutput.topOutputPerNode[ii].uptimeInSeconds);
                console.log("\tdatasetUsedBytes: ", topOutput.topOutputPerNode[ii].datasetUsedBytes);
                console.log("\tsysMemUsedInBytes: ", topOutput.topOutputPerNode[ii].sysMemUsedInBytes);
                console.log("\n\n");
            }
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testGetMemoryUsage(test) {
        test.trivial(xcalarApiGetMemoryUsage(thriftHandle, "test", 1));
    }

    function testListXdfs(test) {
        xcalarApiListXdfs(thriftHandle, "*", "*")
        .done(function(listXdfsOutput) {
            var ii;
            var jj;
            printResult(listXdfsOutput);
            for (ii = 0; ii < listXdfsOutput.numXdfs; ii++) {
                 var numArgs;
                 numArgs = listXdfsOutput.fnDescs[ii].numArgs;
                 if (numArgs < 0) {
                     numArgs *= -1;
                 }
                 console.log("\tfnName: ", listXdfsOutput.fnDescs[ii].fnName);
                 console.log("\tfnDesc: ", listXdfsOutput.fnDescs[ii].fnDesc);
                 console.log("\tNumArgs: ", listXdfsOutput.fnDescs[ii].numArgs);
                 for (jj = 0; jj < numArgs; jj++) {
                      console.log("\tArg ", jj, ": ", listXdfsOutput.fnDescs[ii].argDescs[jj].argDesc);
                 }
                 console.log("\n\n");
             }
             test.pass();
        })
        .fail(function(status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testListVarArgUdf(test) {
        var fnName = "func";
        var argName = "*myArgsList";
        var source = "def " + fnName + "(" + argName + "):\n return \"\"\n";
        var moduleName = "mgmttestVarArgUdf";
        var fullyQualifiedFnName = moduleName + ":" + fnName;
        var userIdName = "test";
        /**
         * Make sure the strings used in the absolute path name for the UDF
         * match those in XLRDIR/src/include/udf/UserDefinedFunction.h
        */
        var expectedFnName = "/workbook/" + userIdName + "/" + session2Id + "/udf/" + fullyQualifiedFnName;
        var ii;

        xcalarApiUdfDelete(thriftHandle, moduleName)
        .always(function () {
            xcalarApiUdfAdd(thriftHandle, UdfTypeT.UdfTypePython,
                             moduleName, source)
            .then(function () {
                return xcalarApiListXdfs(thriftHandle, fullyQualifiedFnName, "User-defined functions");
            })
            .then(function(listXdfsOutput) {
                if (listXdfsOutput.numXdfs != 1) {
                    for (ii = 0; ii < listXdfsOutput.numXdfs; ii++) {
                        console.log("Xdf: ", listXdfsOutput.fnDescs[ii].fnName);
                    }
                    test.fail("Number of XDFs returned = " + listXdfsOutput.numXdfs + " != 1");
                }

                if (listXdfsOutput.fnDescs[0].fnName != expectedFnName) {
                    test.fail("Name of test returned: " + listXdfSOutput.fnDescs[0].fnName + " Expected: " + expectedFnName);
                }

                if (listXdfsOutput.fnDescs[0].numArgs != -1) {
                    var numArgs;
                    numArgs = listXdfsOutput.fnDescs[0].numArgs;
                    if (numArgs < 0) {
                        numArgs *= -1;
                    }

                    console.log(listXdfsOutput.fnDescs[0].fnName);
                    for (ii = 0; ii < numArgs; ii++) {
                        console.log("Arg: ", listXdfsOutput.fndescs[0].argDescs[ii].argDesc);
                    }
                    test.fail("Number of args returned: " + listXdfsOutput.fnDescs[0].numArgs + " Expected: -1");
                }

                if (listXdfsOutput.fnDescs[0].argDescs[0].argDesc != argName) {
                    test.fail("Name of arg returned: " + listXdfsOutput.fnDescs[0].argDescs[ii].argDesc + " Expected: " + argName);
                }

                test.pass();
            })
            .fail(function(status) {
                test.fail("listXdfs returned status: " + StatusTStr[status]);
            });
        });
    }

    function testCreateDht(test) {
        var dhtName = "mgmtTestCustomDht";

        function deleteTableSuccessFn(status) {
            xcalarApiDeleteDht(thriftHandle, dhtName)
            .done(function (status) {
                test.pass();
            })
            .fail(function(status) {
                var reason = "deleteDht returned status: " + StatusTStr[status];
                test.fail(reason);
            });
        }

        function indexDatasetSuccessFn(indexOutput) {
            xcalarGetTableMeta(thriftHandle, indexOutput.tableName, false)
            .done(function(metaOutput) {
                var totalCount = 0;
                for (var ii = 0; ii < metaOutput.numMetas; ii++) {
                    console.log("Node " + ii + " - " + metaOutput.metas[ii].numRows);
                    if (metaOutput.metas[ii].numRows === 0) {
                        var reason = "Node " + ii + " has 0 entries";
                        test.fail(reason);
                    }
                    totalCount += metaOutput.metas[ii].numRows;
                }

                if (totalCount === 70817) {
                    xcalarDeleteDagNodes(thriftHandle, indexOutput.tableName, SourceTypeT.SrcTable)
                    .done(deleteTableSuccessFn)
                    .fail(function(status) {
                        var reason = "deleteTable returned status: " + StatusTStr[status];
                        test.fail(reason);
                    });
                } else {
                    var reason = "Total count " + totalCount + " != 70817";
                    test.fail(reason);
                }
            })
            .fail(function(status) {
                var reason = "getCount returned status: " + StatusTStr[status];
                test.fail(reason);
            });
        }

        function createDhtSuccessFn(status) {
            xcalarIndex(thriftHandle,
                        yelpUserDataset,
                        "yelp/user-average_stars",
                        [new XcalarApiKeyT({name:"average_stars", type:"DfFloat64", keyFieldName:"", ordering:"Invalid"})],
                        "yelp_user",
                        dhtName)
            .done(indexDatasetSuccessFn)
            .fail(function(status) {
                var reason = "Index dataset returned status: " + StatusTStr[status];
                test.fail(reason);
            });
        }

        function startCreateDhtTest(status) {
            console.log("deleteDht returned status: " + StatusTStr[status]);
            xcalarApiCreateDht(thriftHandle, dhtName, 5.0, 0.0,
                               XcalarOrderingT.XcalarOrderingUnordered)
            .done(createDhtSuccessFn)
            .fail(function(status) {
                var reason = "createDht returned status: " + StatusTStr[status];
                test.fail(reason);
            });

        }

        xcalarApiDeleteDht(thriftHandle, dhtName)
        .then(startCreateDhtTest, startCreateDhtTest);
    }

    function testPyExecOnLoad(test) {

        var content = fs.read(system.env.MGMTDTEST_DIR +
                      '/PyExecOnLoadTest.py');

        xcalarApiUdfDelete(thriftHandle, "PyExecOnLoadTest")
        .always(function() {
            xcalarApiUdfAdd(thriftHandle, UdfTypeT.UdfTypePython,
                            "PyExecOnLoadTest", content)
            .done(function(uploadPythonOutput) {
                if (status == StatusT.StatusOk) {
                    var sourceArgs = new DataSourceArgsT();
                    sourceArgs.targetName = targetName;
                    sourceArgs.path = qaTestDir + "/yelp/user";
                    sourceArgs.fileNamePattern = "";
                    sourceArgs.recursive = false;
                    var parseArgs = new ParseArgsT();
                    parseArgs.parserFnName = "PyExecOnLoadTest:poorManCsvToJson";
                    parseArgs.parserArgJson = "{}";

                    xcalarLoad(thriftHandle, "movies", [sourceArgs], parseArgs, 0)
                    .done(function(result) {
                        printResult(result);
                        loadOutput = result;
                        moviesDataset = loadOutput.dataset.name;
                        moviesDatasetSet = true;
                        origDataset = loadOutput.dataset.name;
                        test.pass();
                    })
                    .fail(function(reason) {
                        test.fail(StatusTStr[reason.xcalarStatus]);
                    });
                } else {
                    var reason = "status = " + status;
                    test.fail(reason);
                }
            })
            .fail(function(status) {
                test.fail(StatusTStr[status]);
            });
        });
    }

    function testArchiveTable(test) {
        xcalarArchiveTables(thriftHandle, ["yelp/user-name"])
        .then(function(status) {
            printResult(status);
            return (xcalarUnarchiveTables(thriftHandle, ["yelp/user-name"]));
        })
        .then(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testDeleteTable(test) {
        xcalarDeleteDagNodes(thriftHandle, "yelp/user-votes.funny-map", SourceTypeT.SrcTable)
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testSessionRename(test) {
        xcalarApiSessionRename(thriftHandle, session2 + "-rename", session2)
        .then(function(status) {
            printResult(status);
            return xcalarApiSessionList(thriftHandle, "*");
        })
        .then(function(sessions) {
            sessions = sessions.sessions;
            printResult(sessions);
            for (var i = 0; i<sessions.length; i++) {
                test.assert(sessions[i].name !== session2);
            }
            session2 = session2 + "-rename";
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testSessionPersist(test) {
        setSessionName(session2)
        xcalarApiSessionPersist(thriftHandle, session2)
        .done(function(sessionListOutput) {
            printResult(sessionListOutput);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testSessionInact(test) {
        // Use session4 to test inactivate
        // First confirm that session4 is active
        xcalarApiSessionList(thriftHandle, session4)
        .then(function(ret) {
            test.assert(ret.numSessions === 1);
            test.assert(ret.sessions[0].name === session4);
            test.assert(ret.sessions[0].state.toLowerCase() === "active");
            return xcalarApiSessionInact(thriftHandle, session4);
        })
        .then(function() {
            return xcalarApiSessionList(thriftHandle, session4);
        })
        .then(function(ret) {
            test.assert(ret.numSessions === 1);
            test.assert(ret.sessions[0].name === session4);
            test.assert(ret.sessions[0].state.toLowerCase() === "inactive");
            return xcalarApiSessionActivate(thriftHandle, session4);
        })
        .then(function(ret) {
            printResult(ret);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testSessionDownload(test) {
        var pathToFiles = qaTestDir + "/jupyterNotebooks/";
        xcalarApiSessionDownload(thriftHandle, session2, pathToFiles)
        .done(function(res) {
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testSessionUpload(test) {
        var pathToFilesDown = qaTestDir + "/jupyterNotebooks/";
        var pathToFilesUp = "/tmp/"
        xcalarApiSessionDownload(thriftHandle, session2, pathToFilesDown)
        .done(function(sessionDownloadOutput) {
            session3 = session2 + "-upload";
            xcalarApiSessionUpload(thriftHandle, session3, sessionDownloadOutput.sessionContent, pathToFilesUp)
            .done(function(res) {
                printResult(res);
                test.pass();
            })
            .fail(function(reason) {
                test.fail(reason);
            })
        })
        .fail(function(reason) {
            test.fail(reason);
        })
    }

    function testPublishTableChangeOwner(test) {
        var ptSession = "";
        var ptUser = "";
        var ptSessionChange = "PublishTableSessionChange" + (new Date().getTime());
        var ptName = "pubTable" + (new Date().getTime());
        var indexTable = "";
        var projectTable = "";
        var synthTable = "";

        // Start a new session
        console.log("Create session " + ptSessionChange);
        xcalarApiSessionNew(thriftHandle, ptSessionChange, false, "")
        .then(function() {
            return xcalarApiSessionActivate(thriftHandle, ptSessionChange);
        })
        .then(function() {
            return xcalarProject(thriftHandle, 1, ["yelp_user::review_count"], origTable, projectTable);
        })
        .then(function(result) {
            projectTable = result.tableName;
            return xcalarIndex(thriftHandle, projectTable, indexTable,
                [new XcalarApiKeyT({name:"yelp_user::review_count", type:"DfInt64", keyFieldName:"pk", ordering:"Unordered"})]);
        })
        .then(function(result) {
            indexTable = result.tableName;
            return xcalarApiSynthesize(thriftHandle, indexTable, synthTable, [new XcalarApiColumnT({sourceColumn: 'pk', destColumn: 'pk', columnType: 'DfUnknown'})]);
        })
        .then(function(result) {
            synthTable = result.tableName
            var unixTS = new Date().getTime();
            console.log("Create Publish table " + ptName);
            return xcalarApiPublish(thriftHandle, synthTable, ptName, unixTS);
        })
        .then(function() {
            console.log("List publish tables before ownership change");
            return xcalarListPublishedTables(thriftHandle, ptName, true, -1, true);
        })
        .then(function(listOut) {
            var found = "false";
            for (var ii = 0; ii < listOut.numTables; ii++) {
                if (listOut.tables[ii].name === ptName) {
                    found = "true";
                    console.log("List publish table " + listOut.tables[ii].name + " owned by session " + listOut.tables[ii].userIdName + " user " + listOut.tables[ii].sessionName);
                    ptUser = listOut.tables[ii].userIdName;
                    ptSession = listOut.tables[ii].sessionName;
                }
            }
            test.assert(found === "true");
            console.log("Publish table " + ptName + " change ownership to session " + ptSessionChange +  " user " + ptUser);
            return xcalarPtChangeOwner(thriftHandle, ptName, ptUser, ptSessionChange);
        })
        .then(function() {
            console.log("List publish tables after ownership change");
            return xcalarListPublishedTables(thriftHandle, ptName, true, -1, true);
        })
        .then(function(listOut) {
            var found = "false";
            for (var ii = 0; ii < listOut.numTables; ii++) {
                if (listOut.tables[ii].name === ptName) {
                    found = "true";
                    test.assert(listOut.tables[ii].userIdName === ptUser);
                    test.assert(listOut.tables[ii].sessionName === ptSessionChange);
                }
            }
            test.assert(found === "true");
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testPerNodeOpStats() {
        xcalarApiGetPerNodeOpStats(thriftHandle)
        .done(function(res) {
            printResult(res);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(reason);
        });
    }

    function testBulkDeletePublishedTables(test) {
        xcalarListPublishedTables(thriftHandle, "*", true, -1, true)
        .then(function(listTablesOutput) {
            printResult(listTablesOutput);

            if (listTablesOutput.numTables > 0) {
                var pt = listTablesOutput.tables[0]
                xcalarUnpublish(thriftHandle, pt.name, false)
                .then(function() {
                    xcalarListPublishedTables(thriftHandle, "*", true, -1, true)
                    .then(function(listTablesOutput2) {
                        test.assert(listTablesOutput2.numTables < listTablesOutput.numTables);
                        testBulkDeletePublishedTables(test);
                    })
                    .fail(test.fail);
                })
                .fail(test.fail);
            } else if (listTablesOutput.numTables == 0) {
                test.pass();
            } else {
                test.fail("listTablesOutput.numTables == " + listTablesOutput.numTables);
            }
        })
        .fail(test.fail);
    }

    // Witness to bug 103
    function testBulkDeleteTables(test) {
        xcalarDeleteDagNodes(thriftHandle, "*", SourceTypeT.SrcTable)
        .done(function(deleteTablesOutput) {
            printResult(deleteTablesOutput);

            for (var i = 0, delTableStatus = null; i < deleteTablesOutput.numNodes; i ++) {
                delTableStatus = deleteTablesOutput.statuses[i];
                console.log("\t" + delTableStatus.nodeInfo.name + ": " +
                            StatusTStr[delTableStatus.status]);
            }
            test.pass();
        })
        .fail(test.fail);
    }

    function testBulkDeleteExport(test) {
        xcalarDeleteDagNodes(thriftHandle, "*", SourceTypeT.SrcExport)
        .done(function(deleteDagNodesOutput) {
            printResult(deleteDagNodesOutput);

            for (var i = 0, delTableStatus = null; i < deleteDagNodesOutput.numNodes; i ++) {
                delTableStatus = deleteDagNodesOutput.statuses[i];
                console.log("\t" + delTableStatus.nodeInfo.name + ": " +
                            StatusTStr[delTableStatus.status]);
            }

            test.pass();
        })
        .fail(test.fail);
    }

    function testBulkDeleteConstants(test) {
        xcalarDeleteDagNodes(thriftHandle, "*", SourceTypeT.SrcConstant)
        .done(function(deleteDagNodesOutput) {
            printResult(deleteDagNodesOutput);

            for (var i = 0, delTableStatus = null; i < deleteDagNodesOutput.numNodes; i ++) {
                delTableStatus = deleteDagNodesOutput.statuses[i];
                console.log("\t" + delTableStatus.nodeInfo.name + ": " +
                            StatusTStr[delTableStatus.status]);
            }

            test.pass();
        })
        .fail(test.fail);
    }

    function testBulkDeleteDataset(test) {
        xcalarDeleteDagNodes(thriftHandle, "*", SourceTypeT.SrcDataset)
        .then(function(deleteDagNodesOutput) {
            printResult(deleteDagNodesOutput);

            for (var i = 0, delTableStatus = null; i < deleteDagNodesOutput.numNodes; i ++) {
                delTableStatus = deleteDagNodesOutput.statuses[i];
                console.log("\t" + delTableStatus.nodeInfo.name + ": " +
                            StatusTStr[delTableStatus.status]);
            }
            return (xcalarDatasetUnload(thriftHandle, "*"));
        })
        .then(function(datasetUnloadOutput) {
            printResult(datasetUnloadOutput);

            for (var ii = 0, unloadDatasetStatus = null;
                 ii < datasetUnloadOutput.numDatasets; ii++) {
                unloadDatasetStatus = datasetUnloadOutput.statuses[ii];
                console.log("\t" + unloadDatasetStatus.dataset.name + ": " +
                            StatusTStr[unloadDatasetStatus.status]);
            }

            test.pass();
        })
        .fail(function (status) {
            test.fail(StatusTStr[status]);
        });
    }

    function testDestroyDataset(test) {
        if (moviesDatasetSet) {
            xcalarDeleteDagNodes(thriftHandle, moviesDataset, SourceTypeT.SrcDataset)
            .then(function(deleteDagNodesOutput) {
                printResult(deleteDagNodesOutput);
                return (xcalarDatasetUnload(thriftHandle, moviesDataset));
            })
            .then(function(datasetUnloadOutput) {
                printResult(datasetUnloadOutput);
                if (datasetUnloadOutput.numDatasets != 1) {
                    test.fail("NumDatasets != 1");
                    return;
                }

                if (datasetUnloadOutput.statuses[0].dataset.name != moviesDataset) {
                    test.fail("Dataset we got " + datasetUnloadOutput.statuses[0].dataset.name + ", " +
                              "Dataset we expected " + moviesDataset);
                    return;
                }

                if (datasetUnloadOutput.statuses[0].status != StatusT.StatusOk) {
                    test.fail("Delete dataset returned status: " + StatusTStr[datasetUnloadOutput.statuses[0].status]);
                    return;
                }

                test.pass();
            })
            .fail(function(reason) {
                test.fail(StatusTStr[reason.xcalarStatus]);
            });
        } else {
            console.log("Skipping test because this test depends on testPyExecOnLoad\n");
            skip(test);
        }
    }

    // Witness to bug 98
    function testShutdown(test) {
        xcalarShutdown(thriftHandle)
        .done(function(status) {
            printResult(status);
            test.pass();
        })
        .fail(test.fail);
    }

    function testSupportGenerate(test) {

        // Generate a mini bundle so as to not take a lot of time.
        xcalarApiSupportGenerate(thriftHandle, true, 0)
        .done(function(output) {
            if (fs.exists(output.bundlePath)) {
                fs.removeTree(output.bundlePath);
                test.pass();
            } else {
                printResult(output);
                test.fail("Failed to locate bundle path from output.");
            }
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testUdf(test) {
        setSessionName(session2)
        var source1 = "def foo():\n return 'foo'\n";
        var source2 = "def bar(c):\n return 'bar'\n";

        function udfCleanup() {
            var deferred = jQuery.Deferred();
            xcalarApiUdfDelete(thriftHandle, "mgmttest*")
            .always(deferred.resolve);
            return deferred.promise();
        }

        udfCleanup()
        .then(function () {
            return xcalarApiUdfAdd(thriftHandle, UdfTypeT.UdfTypePython,
                            "mgmttestfoo", source1);
        })
        .then(function () {
            return xcalarApiUdfGet(thriftHandle, "mgmttestfoo");
        })
        .then(function(output) {
            if (output.source != source1) {
                printResult(output);
                test.fail("Expected source '" + source1 + "' got '" +
                          output.source + "'.");
            } else {
                return xcalarApiListXdfs(thriftHandle, "mgmttestfoo:foo", "*");
            }
        })
        .then(function(output) {
            test.assert(output.numXdfs === 1);
            test.assert(output.fnDescs[0].numArgs === 0);
            return xcalarApiUdfUpdate(thriftHandle,
                                      UdfTypeT.UdfTypePython,
                                      "mgmttestfoo", source2);
        })
        .then(function () {
            return xcalarApiUdfGet(thriftHandle, "mgmttestfoo");
        })
        .then(function (output) {
            if (output.source != source2) {
                printResult(output);
                test.fail("Expected source '" + source2 + "' got '" +
                        output.source + "'.");
            } else {
                return xcalarApiListXdfs(thriftHandle, "mgmttestfoo:*", "*");
            }
        })
        .then(function(output) {
            test.assert(output.numXdfs === 1);
            test.assert(output.fnDescs[0].numArgs === 1);
            var userIdName = "test";
            /**
             * Make sure the strings used in the absolute path name for the UDF
             * match those in XLRDIR/src/include/udf/UserDefinedFunction.h
            */
            test.assert(output.fnDescs[0].fnName === "/workbook/" + userIdName + "/" + session2Id + "/udf/mgmttestfoo:bar");
            test.assert(output.fnDescs[0].argDescs[0].argDesc === "c");
            return xcalarApiUdfGetRes(thriftHandle, XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeSession, "mgmttestfoo");
        })
        .then(function(output) {
            var userIdName = "test";
            test.assert(output.udfResPath == "/workbook/" + userIdName + "/" + session2Id + "/udf/mgmttestfoo");
            return xcalarApiUdfDelete(thriftHandle, "mgmttestfoo");
        })
        .then(function () {
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function doTestImportRetina(test, importRetinaName, retinaPath) {
        var file = fs.open(retinaPath, 'rb');
        var content = file.read();

        xcalarApiImportRetina(thriftHandle, importRetinaName, true, content)
        .done(function(importRetinaOutput) {
            console.log("numUdfs: " , importRetinaOutput.numUdfModules);
            if (importRetinaOutput.numUdfModules != 3) {
                test.fail("Number of Udf modules is wrong!");
            } else {
                var udfUploadFailed = false;
                for (var ii = 0; ii < importRetinaOutput.numUdfModules; ii++) {
                    console.log("udf[" + ii + "].moduleName = ",
                                importRetinaOutput.udfModuleStatuses[ii].moduleName);
                    console.log("udf[" + ii + "].status = ",
                                StatusTStr[importRetinaOutput.udfModuleStatuses[ii].status],
                                " (", importRetinaOutput.udfModuleStatuses[ii].status, ")");
                    if (!(importRetinaOutput.udfModuleStatuses[ii].status == StatusT.StatusOk ||
                        importRetinaOutput.udfModuleStatuses[ii].status == StatusT.StatusUdfModuleOverwrittenSuccessfully ||
                        (importRetinaOutput.udfModuleStatuses[ii].moduleName == "default" &&
                         importRetinaOutput.udfModuleStatuses[ii].status == StatusT.StatusUdfModuleAlreadyExists))) {
                        udfUploadFailed = true;
                    }
                    console.log("udf[" + ii + "].error.message = ",
                                importRetinaOutput.udfModuleStatuses[ii].error.message);
                    console.log("udf[" + ii + "].error.traceback = ",
                                importRetinaOutput.udfModuleStatuses[ii].error.traceback);
                }

                if (udfUploadFailed) {
                    test.fail("Udf import failed");
                }
            }

            xcalarListRetinas(thriftHandle)
            .then(function(listRetinasOutput) {
                for (var ii = 0; ii < listRetinasOutput.numRetinas; ii++) {
                    if (listRetinasOutput.retinaDescs[ii].retinaName == importRetinaName) {
                        test.pass();
                    }
                }
                test.fail("Could not find " + importRetinaName + " in listRetinas");
            })
            .fail(function(reason) {
                test.fail(reason);
            });
        })
        .fail(function(reason) {
            test.fail("Import retina failed with status: " + StatusTStr[reason.xcalarStatus] +
                      "(" + reason + ")");
        });

        file.close();
    }

    function testImportRetina(test) {
        retinaImportName = "testImportRetina";
        doTestImportRetina(test, retinaImportName,
                           system.env.MGMTDTEST_DIR + "/testRetina.tar.gz");
    }

    // Needs to be after testImportRetina
    function testExportRetina(test) {
        var retinaPath = system.env.TMP_DIR + "/testRetina.tar.gz";
        if (retinaImportName === "") {
            test.fail("Needs to run after testImportRetina");
        }

        xcalarApiExportRetina(thriftHandle, retinaImportName)
        .done(function(exportRetinaOutput) {
            fs.write(retinaPath, exportRetinaOutput.retina, 'wb');
            doTestImportRetina(test, "testExportRetina", retinaPath);
        })
        .fail(function(reason) {
            test.fail("Export retina failed with status: " + StatusTStr[reason.xcalarStatus] +
                      "(" + reason + ")");
        });
    }

    function testFuncDriverList(test) {
        xcalarApiListFuncTest(thriftHandle, "libhello::*")
        .done(function(listFuncTestOutput) {
            if (listFuncTestOutput.numTests != 1) {
                var message = "numTests matching libhello::* is " + listFuncTestOutput.numTests;
                for (ii = 0; ii < listFuncTestOutput.numTests; ii++) {
                    message += " " + listFuncTestOutput.testNames[ii];
                }

                test.fail(message);
            }

            if (listFuncTestOutput.testNames[0] != "libhello::hello") {
                test.fail("testName we got was " + listFuncTestOutput.testNames[0]);
            }

            test.pass();
        })
        .fail(function(reason) {
            test.fail("List functional tests failed with status: " + StatusTStr[reason.xcalarStatus] +
                      " (" + reason + ")");
        });
    }

    function testLogLevelGet(test) {
        test.trivial(xcalarLogLevelGet(thriftHandle));
    }

    function testApisWithNoArgs(test) {
        // This test calls Xcalar APIs without arguments. Some APIs are not suppose to
        // have arguments and so require them.  We don't differentiate between the two
        // as the goal of the test is to ensure that mgmtd and usrnode don't crash.
        // Some APIs are handled in the mgmtd where the lack of arguments leads to a
        // workItem not being allocated.  Other APIs make it to usrnode and should have
        // errors returned.

        var saveVerbose = verbose;
        // Turn off "verbose" otherwise handlers will try to log nonexistent arguments
        // and trap.
        verbose = false;

        var apiList = [];

        for (var prop in window) {
            if (prop.indexOf("xcalar") === 0 &&
                window[prop] instanceof Function &&
                prop.indexOf("WorkItem") === -1) {
                // XXX: Make necesary changes to allow these APIs to accommodate
                // this test.
                if (prop === "xcalarConnectThrift" ||
                    prop === "xcalarApiGetQuery" ||
                    prop === "xcalarApiGetQueryOld") continue;
                // Deprecated API which hasn't been removed
                if (prop === "xcalarGetStatsByGroupId") continue;
                // This API throws an exception if you pass it incorrect arguments
                if (prop === "xcalarPreview") continue;
                // Don't run without arguments as full size support bundle is
                // generated.
                if (prop === "xcalarApiSupportGenerate") continue;
                apiList.push(window[prop]);
            }
        }
        function testApi(apiFunc) {
            var deferred = jQuery.Deferred();
            apiFunc(thriftHandle)
            .always(deferred.resolve);
            return deferred.promise();
        }

        var promArray = [];

        for (var i = 0; i < apiList.length; i++) {
            var prom = testApi.bind(test, apiList[i]);
            promArray.push(prom);
        }
        console.log("Calling " + promArray.length + " APIs with no arguments");
        PromiseHelper.chain(promArray)
        .then(function() {
            verbose = saveVerbose;
            test.pass();
        })
        .fail(test.fail);
    }

    function testCsvLoadWithSchema(test) {
        var sourceArgs = new DataSourceArgsT();
        sourceArgs.targetName = targetName;
        sourceArgs.path = qaTestDir + "/tpchDatasets/region.tbl";
        sourceArgs.fileNamePattern = "";
        sourceArgs.recursive = false;
        var csvLoadOutput;
        var dsName;
        var dsResultSet;

        var parseArgs = new ParseArgsT();
        parseArgs.parserFnName = "default:parseCsv";
        var csvArgs = {
            "recordDelim": XcalarApiDefaultRecordDelimT,
            "fieldDelim":"|",
            "isCRLF": true,
            "schemaMode": "loadInput",
        };
        parseArgs.parserArgJson = JSON.stringify(csvArgs);
        parseArgs.schema = [new XcalarApiColumnT({sourceColumn:"R_REGIONKEY",
                                                  destColumn:"R_REGIONKEY",
                                                  columnType:"DfInt64"}),
                            new XcalarApiColumnT({sourceColumn:"R_NAME",
                                                  destColumn:"R_NAME",
                                                  columnType:"DfString"}),
                            new XcalarApiColumnT({sourceColumn:"R_COMMENT",
                                                  destColumn:"R_COMMENT",
                                                  columnType:"DfString"})]

        xcalarLoad(thriftHandle, "tpch-region", [sourceArgs], parseArgs, 0)
        .then(function(result) {
            printResult(result);
            csvLoadOutput = result;
            dsName = csvLoadOutput.dataset.name;
            return (xcalarMakeResultSetFromDataset(thriftHandle, dsName, false))
        })
        .then(function(result) {
            printResult(result);
            dsResultSet = result;
            return (xcalarResultSetNext(thriftHandle, dsResultSet.resultSetId, 1))
        })
        .then(function(rsOutput) {
            printResult(rsOutput);
            test.assert(rsOutput.numValues == 1);
            var row = JSON.parse(rsOutput.values[0]);
            test.assert(row.hasOwnProperty("R_REGIONKEY"));
            test.assert(row.hasOwnProperty("R_NAME"));
            test.assert(row.hasOwnProperty("R_COMMENT"));
            test.assert(typeof(row["R_NAME"]) == "string");
            test.assert(typeof(row["R_COMMENT"]) == "string");
            test.assert(typeof(row["R_REGIONKEY"]) == "number");
            return (xcalarDag(thriftHandle, dsName))
        })
        .then(function(dagOutput) {
            printResult(dagOutput);
            test.assert(dagOutput.numNodes === 1);
            var dagNode = dagOutput.node[0];
            test.assert(dagNode.name.name == dsName);
            var loadArgs = dagNode.input.loadInput.loadArgs;
            var parseArgs = JSON.parse(loadArgs.parseArgs.parserArgJson);
            console.log("ParseArgs: " + loadArgs.parseArgs.parserArgJson);
            console.log("CsvArgs: " + JSON.stringify(csvArgs));
            test.assert(parseArgs.recordDelim === csvArgs.recordDelim);
            test.assert(parseArgs.fieldDelim === csvArgs.fieldDelim);
            test.assert(parseArgs.isCRLF === csvArgs.isCRLF);
            test.assert(parseArgs.schemaMode === csvArgs.schemaMode);

            return (xcalarFreeResultSet(thriftHandle, dsResultSet.resultSetId))
        })
        .then(function(result) {
            return (xcalarDeleteDagNodes(thriftHandle, dsName, SourceTypeT.SrcDataset))
        })
        .then(function(deleteDagNodesOutput) {
            printResult(deleteDagNodesOutput);
            return (xcalarDatasetUnload(thriftHandle, dsName));
        })
        .then(function(datasetUnloadOutput) {
            printResult(datasetUnloadOutput);
            test.assert(datasetUnloadOutput.numDatasets == 1);
            test.assert(datasetUnloadOutput.statuses[0].dataset.name == dsName);
            test.assert(datasetUnloadOutput.statuses[0].status == StatusT.StatusOk);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    function testFuncDriverRun(test) {
        xcalarApiStartFuncTest(thriftHandle, false, false, false, ["libhello::*"])
        .done(function(startFuncTestOutput) {
            if (startFuncTestOutput.numTests != 1) {
                test.fail("numTests matching libhello::* is " + startFuncTestOutput.numTests);
            }

            if (startFuncTestOutput.testOutputs[0].testName != "libhello::hello") {
                test.fail("We got a bogus test name: " + startFuncTestOutput.testOutputs[0].testName);
            }
            if (startFuncTestOutput.testOutputs[0].status != StatusT.StatusOk) {
                test.fail(startFuncTestOutput.testOutputs[0].testName + " failed with status: " +
                          StatusTStr[startFuncTestOutput.testOutputs[0].status] + " (" +
                          startFuncTestOutput.testOutputs[0].status + ")");
            }

            test.pass();
        })
        .fail(function(reason) {
            test.fail("Run funtional tests failed with status: " + StatusTStr[reason.xcalarStatus] +
                      " (" + reason + ")");
        });
    }

    function testParquetApp(test) {
        var parquetAppName = "XcalarParquet";
        var targetMgrAppName = "XcalarTargetMgr";
        var parquetTargetName = "parquetMgmtTest";
        var resultSetId;
        var randomColumns = [];
        var randomPartitionKeyValue;
        var parquetDsName = "parquetDs";
        var pathToParquetDataset = qaTestDir + "/parquet/spark/nestedPartitions.parquet";
        var expectedPartitionKeys = [ "type", "yelping_since" ]
        var possibleKeyValues;
        var schema;
        var args = { "func": "getInfo", "targetName": "Default Shared Root", "path": pathToParquetDataset }
        xcalarAppRun(thriftHandle, parquetAppName,  false, JSON.stringify(args))
        .then(appReapRetryer)
        .then(function(result) {
            var outStr = result.outStr;
            console.log("Result from getInfo(): " + outStr)
            var appResult = JSON.parse(JSON.parse(outStr)[0][0]);
            schema = appResult.schema;
            var partitionKeys = appResult.partitionKeys;
            if (partitionKeys.length != expectedPartitionKeys.length) {
                test.fail("Expected " + expectedPartitionKeys.length + " partitionKeys, got " + partitionKeys.length);
                return;
            }

            for (var ii = 0; ii < expectedPartitionKeys.length; ii++) {
                if (partitionKeys[ii] != expectedPartitionKeys[ii]) {
                    test.fail("Expected partition key to be \"" + expectedPartitionKeys[ii] + "\", but got \"" + partitionKeys[ii] + "\"");
                    return;
                }
            }

            args["func"] = "getPossibleKeyValues";
            args["key"] = expectedPartitionKeys[0];
            return (xcalarAppRun(thriftHandle, parquetAppName, false, JSON.stringify(args)))
        })
        .then(appReapRetryer)
        .then(function(result) {
            var outStr = result.outStr;
            console.log("Result from getPossibleKeyValues(type): " + outStr);
            var expectedValues = [ "user", "__HIVE_DEFAULT_PARTITION__" ];
            var appResult = JSON.parse(JSON.parse(outStr)[0][0]);
            if (appResult.length != expectedValues.length) {
                test.fail("Expected " + expectedValues.length + " values, got " + appResult.length);
                return;
            }

            for (var ii = 0; ii < expectedValues.length; ii++) {
                // Order of expectedValues might not match order of returned values and that's ok
                var found = false;
                for (var jj = 0; jj < appResult.length; jj++) {
                    if (appResult[jj] == expectedValues[ii]) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    test.fail(JSON.stringify(appResult) + " does not contain " + expectedValues[ii]);
                    return;
                }
            }

            args["key"] = expectedPartitionKeys[1];
            args["givenKeys"] = { "type": [ "user" ] };
            return (xcalarAppRun(thriftHandle, parquetAppName, false, JSON.stringify(args)));
        })
        .then(appReapRetryer)
        .then(function(result) {
            var outStr = result.outStr;
            console.log("Result from getPossibleKeyValues(yelping_since): " + outStr);
            possibleKeyValues = JSON.parse(JSON.parse(outStr)[0][0]);

            randomPartitionKeyValue = possibleKeyValues[Math.floor(Math.random() * possibleKeyValues.length)]

            var targetParams = { "backingTargetName": "Default Shared Root" }
            var targetArgs = { "targetTypeId": "parquetds", "targetName": parquetTargetName,
                               "targetParams": targetParams, "func": "addTarget" }
            return xcalarAppRun(thriftHandle, targetMgrAppName, false, JSON.stringify(targetArgs))
        })
        .then(appReapRetryer)
        .then(function(result) {
            var outStr = result.outStr;
            console.log("Result from addTarget: " + outStr);

            var sourceArgs = new DataSourceArgsT();
            sourceArgs.targetName = parquetTargetName;
            //sourceArgs.path = pathToParquetDataset + "?" + expectedPartitionKeys[0] + "=*&" + expectedPartitionKeys[1] + "=" + randomPartitionKeyValue;
            sourceArgs.path = pathToParquetDataset;
            sourceArgs.fileNamePattern = "";
            sourceArgs.recursive = false;

            for (var column in schema) {
                if ((Math.random() * 100) < 50) {
                    randomColumns.push(column)
                }
            }

            for (var idx in expectedPartitionKeys) {
                randomColumns.push(expectedPartitionKeys[idx]);
            }

            var partKeys = {};
            partKeys[expectedPartitionKeys[0]] = ['*'];
            partKeys[expectedPartitionKeys[1]] = [randomPartitionKeyValue];

            console.log("partKeys", JSON.stringify(partKeys))
            console.log(JSON.stringify(randomColumns))
            var parquetArgs = { "columns": randomColumns, "partitionKeys": partKeys, "parquetParser": "pyArrow" }
            var parseArgs = new ParseArgsT();
            parseArgs.parserFnName = "default:parseParquet";
            parseArgs.parserArgJson = JSON.stringify(parquetArgs);

            return (xcalarLoad(thriftHandle, parquetDsName, [sourceArgs], parseArgs, 0));
        })
        .then(function(result) {
            printResult(result);
            loadOutput = result;
            parquetDsName = loadOutput.dataset.name;

            return (xcalarMakeResultSetFromDataset(thriftHandle, parquetDsName, false));
        })
        .then(function(ret) {
            resultSetId = ret.resultSetId;
            return (xcalarResultSetNext(thriftHandle, resultSetId, 1));
        })
        .then(function(resultSet) {
            var values = JSON.parse(resultSet.values[0])
            console.log(JSON.stringify(values))
            for (var idx in randomColumns) {
                var column = randomColumns[idx]
                console.log("Checking for column " + column)
                if (!values.hasOwnProperty(column)) {
                    test.fail(JSON.stringify(values) + " is missing column " + column);
                    return;
                }
            }
            if (values[expectedPartitionKeys[1]] != randomPartitionKeyValue) {
                test.fail("Expected " + expectedPartitionKeys[1] + " = " + randomPartitionKeyValue + " but got " + values[expectedPartitionKeys[1]]);
            }
            return (xcalarFreeResultSet(thriftHandle, resultSetId));
        })
        .then(function(status) {
            return (xcalarDeleteDagNodes(thriftHandle, parquetDsName, SourceTypeT.SrcDataset))
        })
        .then(function(deleteDagNodesOutput) {
            printResult(deleteDagNodesOutput);
            return (xcalarDatasetUnload(thriftHandle, parquetDsName))
        })
        .then(function(datasetUnloadOutput) {
            printResult(datasetUnloadOutput);
            test.pass();
        })
        .fail(function(reason) {
            test.fail(StatusTStr[reason.xcalarStatus]);
        });
    }

    passes            = 0;
    fails             = 0;
    skips             = 0;
    returnValue       = 0;
    defaultTimeout    = 256000000;
    disableIsPass     = true;

    var content = fs.read(system.env.MGMTDTEST_DIR + '/test-config.cfg');
    var port = content.slice(content.indexOf('Thrift.Port'));
    port = port.slice(port.indexOf('=') + 1, port.indexOf('\n'));

    thriftHandle   = xcalarConnectThrift("localhost:"+port);
    loadArgs       = null;
    loadOutput     = null;
    origDataset    = null;
    yelpUserDataset = null;
    queryName      = null;
    origTable      = null;
    aggrTable      = null;
    origStrTable   = null;
    queryTableName = "yelp-joinTable";

    makeResultSetOutput1 = null;   // for dataset
    makeResultSetOutput2 = null;   // for table
    makeResultSetOutput3 = null;   // for aggregate
    newTableOutput       = null;

    retinaName            = "";
    retinaFilterStr = "gt(yelp_user::votes.funny, <foo>)";
    retinaExportFile  = "retinaDstFile.csv";

    // Format
    // addTestCase(testFn, testName, timeout, TestCaseEnabled, Witness)

    addTestCase(testGetNumNodes, "getNumNodes", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetVersion, "getVersion", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetConfigParams, "getConfigParams", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testSetConfigParam, "setConfigParam", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testRuntimeParams, "runtimeParams", defaultTimeout, TestCaseDisabled, "");
    addTestCase(testFuncDriverList, "listFuncTests", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testFuncDriverRun, "runFuncTests", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testTarget, "test target operations", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListTargetTypes, "test target types", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testTxnLog, "Test Txn logging", defaultTimeout, TestCaseEnabled, "10624");

    // addTestCase(testApisWithNoArgs, "call Xcalar APIs without args", defaultTimeout, TestCaseEnabled, "");

    // This actually starts our sessions, so run this before any test
    // that requires sessions
    addTestCase(testApiKeySessions, "key sessions", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testSessionPersist, "persist session", defaultTimeout, TestCaseEnabled);
    addTestCase(testSessionRename, "rename session", defaultTimeout, TestCaseEnabled);
    // XXX Re-enable when we actually need getPerNodeOpStats
    addTestCase(testPerNodeOpStats, "get per node op stats", defaultTimeout, TestCaseDisabled);

    // Dataflow 2.0 dataset functionality
    addTestCase(testDatasetCreate, "Dataset create meta", defaultTimeout, TestCaseEnabled);
    addTestCase(testDatasetLoad, "Dataset load", defaultTimeout, TestCaseEnabled);
    addTestCase(testDatasetUnload, "Dataset unload", defaultTimeout, TestCaseEnabled);
    addTestCase(testDatasetGetMeta, "Dataset get meta", defaultTimeout, TestCaseEnabled);
    addTestCase(testDatasetDelete, "Dataset delete meta", defaultTimeout, TestCaseEnabled);


    addTestCase(testBulkDestroyDs, "bulk destroy ds", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testBadLoad, "bad load", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testLoad, "load", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testLoadRegex, "loadRegex", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testPreview, "preview", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testLoadEdgeCaseDos, "loadDos", defaultTimeout, TestCaseEnabled, "4415");
    // Xc-1981
    addTestCase(testGetDagOnAggr, "get dag on aggregate", defaultTimeout, TestCaseDisabled, "1981");

    addTestCase(testListDatasets, "list datasets", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetDatasetsInfo, "get dataset info", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListDatasetUsers, "list dataset users", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListUserDatasets, "list user's datasets", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetQueryIndex, "test get query Index", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetQueryLoad, "test get query Load", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIndexDatasetIntSync, "index dataset (int) Sync", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIndexDatasetInt, "index dataset (int)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIndexDatasetStr, "index dataset (str)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIndexTable, "index table (str) Sync", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIndexDatasetBogus, "bogus index dataset", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIndexTable2, "index table (str) 2", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetTableRefCount, "table refCount", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetTableMeta, "table meta", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testRenameNode, "rename node", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetDatasetCount, "dataset count", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetTableCount, "table count", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListTables, "list tables", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testPublishTableChangeOwner, "publish table change owner", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testSessionInact, "inact session", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testSessionDownload, "download session", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testSessionUpload, "upload session", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testGetStats, "get stats", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetStatGroupIdMap, "get stats group id map", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testIndexDatasetWithPrefix, "index dataset with prefix", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testResetStats, "reset stats", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testMakeResultSetFromDataset, "result set (via dataset)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testMakeResultSetFromTable, "result set (via tables)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testResultSetNextDataset, "result set next (dataset)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testResultSetAbsolute, "result set absolute", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testResultSetAbsoluteBogus, "result set absolute bogus", defaultTimeout, TestCaseEnabled, "95");
    addTestCase(testResultSetNextTable, "result set next (table)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testFreeResultSetDataset, "free result set (dataset)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testFreeResultSetTable, "free result set (table)", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testFilter, "filter", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testProject, "project", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testJoin, "join", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testCrossJoin, "cross join", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testUnionAll, "union all", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testUnion, "union", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIntersectAll, "intersect all", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testIntersect, "intersect", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testExceptAll, "except all", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testExcept, "except", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetOpStats, "getOpStats", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testQuery, "Submit Query", defaultTimeout, TestCaseDisabled, "");
    addTestCase(testQueryState, "Request query state of indexing dataset (int)", defaultTimeout, TestCaseDisabled, "");
    addTestCase(waitForDag, "waitForDag", defaultTimeout, TestCaseDisabled, "");
    addTestCase(testDag, "dag", defaultTimeout, TestCaseDisabled, "568");
    addTestCase(testTagDagNodes, "tag dag nodes", defaultTimeout, TestCaseEnabled, "9130");
    addTestCase(testCommentDagNodes, "comment dag nodes", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGroupBy, "groupBy", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testAggregate, "Aggregate", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testMakeResultSetFromAggregate, "result set of aggregate", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testResultSetNextAggregate, "result set next of aggregate", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testFreeResultSetAggregate, "result set free of aggregate", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testMap, "map", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testDestroyDatasetInUse, "destroy dataset in use", defaultTimeout, TestCaseDisabled, "");
    addTestCase(testDriver, "test driver operations", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testExportCSV, "export csv", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testExportCancel, "export cancel", defaultTimeout, TestCaseDisabled, "");

    // Together, these set of test cases make up the retina sanity
    addTestCase(testMakeRetina, "makeRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListRetinas, "listRetinas", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetRetina1, "getRetina - iter 1 / 2", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testUpdateRetina, "updateRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetRetina2, "getRetina - iter 2 / 2", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testExecuteRetina, "executeRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testCancelRetina, "cancelRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListParametersInRetina, "listParametersInRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testImportRetina, "importRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testExportRetina, "exportRetina", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testDeleteRetina, "deleteRetina", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testListFiles, "list files", defaultTimeout, TestCaseEnabled, "");

    // This pair must go together
    addTestCase(testPyExecOnLoad, "python during load", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testDestroyDataset, "destroy dataset", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testUdf, "UDF test", defaultTimeout, TestCaseEnabled, "");

    // Witness to bug 238
    addTestCase(testApiMapLongEvalString, "Map long eval string", defaultTimeout, TestCaseEnabled, "238");
    addTestCase(testApiFilterLongEvalString, "Filter long eval string", defaultTimeout, TestCaseEnabled, "238");

    // Witness to bug 2020
    addTestCase(testApiMapStringToString, "cast string to string", defaultTimeout, TestCaseEnabled, "2020");

    // Witness to bug 8711
    addTestCase(testApiMapInPlaceReplace, "in place map replace", defaultTimeout, TestCaseEnabled, "8711");

    addTestCase(testApiKeyList, "key list", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeyAdd, "key add", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeyReplace, "key replace", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeyLookup, "key lookup", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeyDelete, "key delete", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeyBogusLookup, "bogus key lookup", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeyAppend, "key append", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testApiKeySetIfEqual, "key set if equal", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testTop, "top test", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testPerNodeTop, "per node top test", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testGetMemoryUsage, "get memory usage test", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testListXdfs, "listXdfs test", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testApps, "Apps test", defaultTimeout, TestCaseEnabled, "");

    // Witness to bug Xc-4963
    addTestCase(testListVarArgUdf, "listVarArgUdf test", defaultTimeout, TestCaseEnabled, "4963");

    addTestCase(testSupportGenerate, "support generate", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testCreateDht, "create DHT test", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testArchiveTable, "archive table", defaultTimeout, TestCaseDisabled, "");

    // XXX re-enable when the query-DAG bug is fixed
    addTestCase(testDeleteTable, "delete table", defaultTimeout, TestCaseDisabled, "");

    addTestCase(testBulkDeleteTables, "bulk delete tables", defaultTimeout, TestCaseEnabled, "103");
    addTestCase(testBulkDeletePublishedTables, "bulk delete published tables", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testBulkDeleteExport, "bulk delete export node", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testBulkDeleteConstants, "bulk delete constant node", defaultTimeout, TestCaseEnabled, "");
    addTestCase(testBulkDeleteDataset, "bulk delete datasets", defaultTimeout, TestCaseEnabled, "2314");
    addTestCase(testShutdown, "shutdown", defaultTimeout, TestCaseEnabled, "98");
    // LogLevelSet has 3 params (max):
    //     logLevel [ FlushGlobal [ NUMBER ] | FlushLocal ]
    // logLevel: in below, LOG_CRIT has value 2, LOG_DEBUG is 7
    // FlushLevel: use 0 for NoFlush, 1 for FlushGlobal, 2 for FlushLocal
    // NUMBER: > 0 flush period in secs; -1 to turn off flushing; 0 is a no-op
    //
    // Note that NUMBER > 0 will work only on a deployment which has buffering
    // e.g. using NUMBER > 0 will fail on DEBUG builds (which don't have
    // buffered logs). So for the automation just use 0 for NUMBER which should
    // pass on all builds.
    addTestCase(testLogLevelSetCrit, "loglevelset LOG_CRIT 1 0", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testLogLevelSetDebug, "loglevelset LOG_DEBUG 0 0", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testGetIpAddrNode0, "getipaddr 0", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testLogLevelGet, "loglevelget", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testCsvLoadWithSchema, "csvloadwithschema", defaultTimeout, TestCaseEnabled, "");

    addTestCase(testParquetApp, "parquet", defaultTimeout, TestCaseEnabled, "");

    runTestSuite(testCases);

});
