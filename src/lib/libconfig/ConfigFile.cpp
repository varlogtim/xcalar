// Copyright 2014-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <strings.h>
#include <assert.h>
#include <unistd.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "config/Config.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "common/InitTeardown.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "constants/XcalarConfig.h"
#include "app/AppMgr.h"
#include "libapis/LibApisCommon.h"
#include "runtime/Runtime.h"
#include "util/License.h"

static constexpr const char *moduleName = "libconfigParse";
static const char *userIdName = "ConfigUser";

Status
Config::parseBoolean(char *input, bool *output)
{
    if (strcasecmp(input, "true") == 0) {
        *output = true;
        return StatusOk;
    } else if (strcasecmp(input, "false") == 0) {
        *output = false;
        return StatusOk;
    } else {
        return StatusInval;
    }
}

Status
ConfigModuleNodes::parse(Config::Configuration *config,
                         char *key,
                         char *value,
                         bool stringentRules)
{
    Status status;
    int len;
    unsigned nodeNum;
    Config::ConfigNode *nodes;
    char innerKey[Config::MaxKeyLength];

    if (InitTeardown::get() != NULL &&
        InitTeardown::get()->getInitLevel() >= InitLevel::UsrNode) {
    }
    if (strcasecmp(key, "NumNodes") == 0) {
        unsigned numNodes = atoi(value);

        if (numNodes > 0) {
            status = Config::get()->setNumTotalNodes(numNodes);
            if (status != StatusOk) {
                return status;
            }

            config->numNodes = numNodes;
            config->nodes =
                (Config::ConfigNode *) memAllocExt(sizeof(*nodes) * numNodes,
                                                   moduleName);

            if (config->nodes == NULL) {
                return StatusNoMem;
            }

            memZero(config->nodes, sizeof(*nodes) * numNodes);
        } else {
            config->numNodes = 0;
        }

        return StatusOk;
    }

    len = sscanf(key, "%u.%s", &nodeNum, innerKey);
    assert(len > 0);

    assert(nodeNum < config->numNodes);

    if (strcasecmp(innerKey, "IpAddr") == 0) {
        config->nodes[nodeNum].ipAddr =
            (char *) memAllocExt(strlen(value) + 1, moduleName);
        if (config->nodes[nodeNum].ipAddr == NULL) {
            return StatusNoMem;
        }
        strcpy(config->nodes[nodeNum].ipAddr, value);
        // XXX Validate value isn't empty string.
    } else if (strcasecmp(innerKey, "Port") == 0) {
        config->nodes[nodeNum].port = atoi(value);
        assert(config->nodes[nodeNum].port > 0);
    } else if (strcasecmp(innerKey, "ApiPort") == 0) {
        config->nodes[nodeNum].apiPort = atoi(value);
        assert(config->nodes[nodeNum].apiPort > 0);
    } else if (strcasecmp(innerKey, "MonitorPort") == 0) {
        config->nodes[nodeNum].monitorPort = atoi(value);
        assert(config->nodes[nodeNum].monitorPort > 0);
    } else {
        return StatusConfigInvalid;
    }

    return StatusOk;
}

// Convert the config information back into the default.cfg file
// format and write it to the provided buffer.
Status
Config::reverseParseNodes(Configuration *config,
                          char *buff,
                          size_t buffLen,
                          size_t *lenUsed)
{
    Status status = StatusOk;
    char *curBuff = buff;
    size_t curBuffLen = buffLen;
    size_t ret;

    *lenUsed = 0;

    ret = snprintf(curBuff, curBuffLen, "Node.NumNodes=%d\n", config->numNodes);
    if (ret >= curBuffLen) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    *lenUsed += ret;
    curBuff += ret;
    curBuffLen -= ret;

    for (unsigned ii = 0; ii < config->numNodes; ii++) {
        ret = snprintf(curBuff,
                       curBuffLen,
                       "Node.%d.IpAddr=%s\n",
                       ii,
                       config->nodes[ii].ipAddr);
        if (ret >= curBuffLen) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        *lenUsed += ret;
        curBuff += ret;
        curBuffLen -= ret;

        ret = snprintf(curBuff,
                       curBuffLen,
                       "Node.%d.Port=%d\n",
                       ii,
                       config->nodes[ii].port);
        if (ret >= curBuffLen) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        *lenUsed += ret;
        curBuff += ret;
        curBuffLen -= ret;

        ret = snprintf(curBuff,
                       curBuffLen,
                       "Node.%d.ApiPort=%d\n",
                       ii,
                       config->nodes[ii].apiPort);
        if (ret >= curBuffLen) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        *lenUsed += ret;
        curBuff += ret;
        curBuffLen -= ret;

        if (config->nodes[ii].monitorPort > 0) {
            ret = snprintf(curBuff,
                           curBuffLen,
                           "Node.%d.MonitorPort=%d\n",
                           ii,
                           config->nodes[ii].monitorPort);
            if (ret >= curBuffLen) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            *lenUsed += ret;
            curBuff += ret;
            curBuffLen -= ret;
        }
    }

CommonExit:

    return status;
}

Status
ConfigModuleThrift::parse(Config::Configuration *config,
                          char *key,
                          char *value,
                          bool stringentRules)
{
    size_t bufSize;

    if (InitTeardown::get() != NULL &&
        InitTeardown::get()->getInitLevel() >= InitLevel::UsrNode) {
    }

    if (strcasecmp(key, "Port") == 0) {
        config->thriftPort = (int) strtol(value, NULL, 0);
    } else if (strcasecmp(key, "Host") == 0) {
        bufSize = strlen(value) + 1;
        config->thriftHost = (char *) memAllocExt(bufSize, moduleName);
        assert(config->thriftHost != NULL);
        strlcpy(config->thriftHost, value, bufSize);
    } else {
        return StatusConfigInvalid;
    }

    return StatusOk;
}

// Convert the thrift information back into the default.cfg file
// format and write into the provided buffer.
Status
Config::reverseParseThrift(Configuration *config,
                           char *buff,
                           size_t buffLen,
                           size_t *lenUsed)
{
    Status status = StatusOk;
    char *curBuff = buff;
    size_t curBuffLen = buffLen;
    size_t ret;

    *lenUsed = 0;

    ret = snprintf(curBuff, curBuffLen, "Thrift.Port=%d\n", config->thriftPort);
    if (ret >= curBuffLen) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    *lenUsed += ret;
    curBuff += ret;
    curBuffLen -= ret;

    ret = snprintf(curBuff, curBuffLen, "Thrift.Host=%s\n", config->thriftHost);
    if (ret >= curBuffLen) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    *lenUsed += ret;

CommonExit:

    return status;
}

// Validate and handle certain keys.
static Status
checkAndHandleSpecialKeys(const char *key, const char *value)
{
    Status status = StatusUnknown;

    if (strcasecmp(key, XcalarConfig::ClusterLogLevelParamName) == 0) {
        // Validating and applying the specified value
        status = parseAndProcessLogLevel(value);
        BailIfFailed(status);
    } else if (strcasecmp(key, XcalarConfig::MaxInteractiveDataSizeParamName) ==
               0) {
        uint64_t datasetSize = strtol(value, NULL, 0);
        // This can be called during boot, when licenseMgr is not inited yet
        if (LicenseMgr::get() != NULL && LicenseMgr::get()->licIsLoaded() &&
            datasetSize > LicenseMgr::get()
                              ->getLicenseData()
                              ->getMaxInteractiveDataSize()) {
            status = StatusInval;
            goto CommonExit;
        }
    }

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid value '%s' specified for '%s' parameter",
                value,
                key);
    }

    return status;
}

Status
ConfigModuleConstants::parse(Config::Configuration *config,
                             char *key,
                             char *value,
                             bool stringentRules)
{
    Status status = StatusOk;
    unsigned ii;
    Status sts;
    bool found = false;
    XcalarConfig *xcConfig = XcalarConfig::get();

    xcConfig->configLock_.lock();

    KeyValueMappings *mappings = xcConfig->mappings_;

    if (InitTeardown::get() != NULL &&
        InitTeardown::get()->getInitLevel() >= InitLevel::UsrNode) {
    }

    for (ii = 0; ii < KeyValueMappings::MappingsCount && !found; ii++) {
        if (strcasecmp(key, mappings[ii].keyName) == 0) {
            found = true;
            if (stringentRules) {
                // Enforce the rules which apply when setting a constant via
                // the API.
                if (!mappings[ii].changeable) {
                    status = StatusConfigParamImmutable;
                    goto CommonExit;
                }
            }
            switch (mappings[ii].type) {
            case KeyValueMappings::Boolean:
                sts =
                    Config::get()->parseBoolean(value,
                                                (bool *) mappings[ii].variable);
                if (sts != StatusOk) {
                    status = StatusConfigInvalid;
                    goto CommonExit;
                }
                break;
            case KeyValueMappings::Integer:
                uint32_t v32;
                v32 = atoi(value);
                if (mappings[ii].doRangeCheck) {
                    if (v32 < mappings[ii].minValue ||
                        v32 > mappings[ii].maxValue) {
                        xSyslog(moduleName,
                                XlogErr,
                                "Parameter '%s' value '%s' is out of range"
                                " (Min: %lu, Max: %lu)",
                                key,
                                value,
                                mappings[ii].minValue,
                                mappings[ii].maxValue);
                        status = StatusUsrNodeIncorrectParams;
                        goto CommonExit;
                    }
                }
                // Check if this is a "known" setting that requires special
                // handling.
                sts = checkAndHandleSpecialKeys(mappings[ii].keyName, value);
                if (sts != StatusOk) {
                    status = StatusUsrNodeIncorrectParams;
                    goto CommonExit;
                }

                *((int *) mappings[ii].variable) = v32;
                break;
            case KeyValueMappings::LongInteger:
                uint64_t v64;
                v64 = strtol(value, NULL, 0);
                if (mappings[ii].doRangeCheck) {
                    if (v64 < mappings[ii].minValue ||
                        v64 > mappings[ii].maxValue) {
                        xSyslog(moduleName,
                                XlogErr,
                                "Parameter '%s' value '%s' is out of range"
                                " (Min: %lu, Max: %lu)",
                                key,
                                value,
                                mappings[ii].minValue,
                                mappings[ii].maxValue);
                        status = StatusUsrNodeIncorrectParams;
                        goto CommonExit;
                    }
                }

                sts = checkAndHandleSpecialKeys(mappings[ii].keyName, value);
                if (sts != StatusOk) {
                    status = StatusUsrNodeIncorrectParams;
                    goto CommonExit;
                }

                *((long *) mappings[ii].variable) = v64;
                break;
            case KeyValueMappings::String:
                // Check if this is a "known" setting that requires special
                // handling.
                sts = checkAndHandleSpecialKeys(mappings[ii].keyName, value);
                if (sts != StatusOk) {
                    status = StatusUsrNodeIncorrectParams;
                    goto CommonExit;
                }
                strlcpy((char *) mappings[ii].variable,
                        value,
                        mappings[ii].bufSize);
                break;
            default:
                // it is axiomatic any key we have has a valid type
                assert(0);
                status = StatusConfigInvalid;
                goto CommonExit;
            }
            break;
        }
    }
    if (InitTeardown::get() != NULL &&
        InitTeardown::get()->getInitLevel() >= InitLevel::UsrNode) {
        if (!found) {
            xSyslog(moduleName,
                    XlogWarn,
                    "No such configuration parameter key=%s value=%s",
                    key,
                    value);
            status = StatusUsrNodeIncorrectParams;
            goto CommonExit;
        } else {
            xSyslog(moduleName,
                    XlogWarn,
                    "Changing configuration parameter key '%s' to '%s'",
                    key,
                    value);
        }
    }

CommonExit:

    xcConfig->configLock_.unlock();

    return status;
}

bool
Config::changedFromDefault(KeyValueMappings *kvm)
{
    bool changed = false;

    switch (kvm->type) {
    case KeyValueMappings::Boolean:
        if (*(bool *) kvm->variable != kvm->defaultBoolean) {
            changed = true;
        }
        break;
    case KeyValueMappings::Integer:
        if (*(uint32_t *) kvm->variable != kvm->defaultInteger) {
            changed = true;
        }
        break;
    case KeyValueMappings::LongInteger:
        if (*(size_t *) kvm->variable != kvm->defaultLongInteger) {
            changed = true;
        }
        break;
    case KeyValueMappings::String:
        assert(kvm->variable != NULL);
        if (kvm->defaultString != NULL) {
            if (strncmp((char *) kvm->defaultString,
                        (char *) kvm->variable,
                        kvm->bufSize) != 0) {
                changed = true;
            }
        } else {
            // No default value.  Anything other than a empty string
            // is considered a change.
            if (strlen((char *) kvm->variable) > 0) {
                changed = true;
            }
        }
        break;
    default:
        assert(0);
        break;
    }

    return changed;
}

const char *
Config::getRootPath()
{
    KeyValueMappings *mappings = XcalarConfig::get()->mappings_;
    return (const char *) (mappings[KeyValueMappings::XcRootPath].variable);
}

const char *
Config::getLogDirPath()
{
    // for now; can generalize into getConfigParameter later
    KeyValueMappings *mappings = XcalarConfig::get()->mappings_;

    return (const char *) (mappings[KeyValueMappings::XcLogDirPath].variable);
}

// This function is called to set parameter values from the API.  More
// stringent checks are done for changes via API.
Status
Config::setConfigParameter(char *name, char *value)
{
    Status status = StatusOk;
    unsigned numNodes = Config::get()->getActiveNodes();
    Status statusArray[numNodes];

    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgSetConfigInput input;

    strlcpy(input.paramName, name, sizeof(input.paramName));
    strlcpy(input.paramValue, value, sizeof(input.paramValue));

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &input,
                                      sizeof(input),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcSetConfig1,
                                      statusArray,
                                      TwoPcNoInputOrOutput);

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcSetConfig,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrOnly),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);
    BailIfFailed(status);

    for (unsigned ii = 0; ii < numNodes; ii++) {
        if (statusArray[ii] != StatusOk) {
            status = statusArray[ii];
            break;
        }
    }

CommonExit:
    return status;
}

Status
Config::setConfigParameterLocal(char *name, char *value)
{
    Status status = StatusOk;

    status = modules_[ConstantsModule]
                 ->parse(&config_,
                         name,
                         value,
                         KeyValueMappings::StringentEnforcement);

    return status;
}

// This function writes config file constants to the provided buffer.
Status
Config::reverseParseConstants(Configuration *config,
                              char *buff,
                              size_t buffLen,
                              size_t *lenUsed)
{
    Status status = StatusOk;
    char *curBuff = buff;
    size_t curBuffLen = buffLen;
    size_t ret = 0;
    KeyValueMappings *mappings = XcalarConfig::get()->mappings_;

    *lenUsed = 0;

    for (unsigned ii = 0; ii < KeyValueMappings::MappingsCount; ii++) {
        if (mappings[ii].visible || changedFromDefault(&mappings[ii])) {
            // Visible parameter or it's a hidden one and the user has changed
            // it from its default value....presumably under the guidance of
            // Xcalar support.

            switch (mappings[ii].type) {
            case KeyValueMappings::Boolean:
                ret = snprintf(curBuff,
                               curBuffLen,
                               "Constants.%s=%s\n",
                               mappings[ii].keyName,
                               (*(bool *) mappings[ii].variable ? "true"
                                                                : "false"));
                break;

            case KeyValueMappings::Integer:
                ret = snprintf(curBuff,
                               curBuffLen,
                               "Constants.%s=%d\n",
                               mappings[ii].keyName,
                               (*(uint32_t *) mappings[ii].variable));
                break;

            case KeyValueMappings::LongInteger:
                ret = snprintf(curBuff,
                               curBuffLen,
                               "Constants.%s=%lu\n",
                               mappings[ii].keyName,
                               (*(size_t *) mappings[ii].variable));
                break;

            case KeyValueMappings::String:
                if (((char *) mappings[ii].variable)[0] == '\0') {
                    // If default is empty string avoid adding an invalid
                    // entry like
                    //      Constants.XdbLocalSerDesPath=
                    continue;
                }

                ret = snprintf(curBuff,
                               curBuffLen,
                               "Constants.%s=%s\n",
                               mappings[ii].keyName,
                               ((char *) mappings[ii].variable));
                break;

            default:
                assert(0 && "Programmer error: forgot to add new value");
                status = StatusUnimpl;
                goto CommonExit;
            };

            if (ret >= curBuffLen) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            // Update housekeeping...
            *lenUsed += ret;
            curBuff += ret;
            curBuffLen -= ret;
        }
    }

CommonExit:

    return status;
}

bool
Config::canIgnoreLine(char *line)
{
    return (strlen(line) < 2) || (line[0] == '/' && line[1] == '/') ||
           (line[0] == '#');
}

// This function returns true if the line has content that will be
// reverse parsed.
bool
Config::isReverseParsedLine(char *line)
{
    char *l;

    l = strTrim(line);
    if (canIgnoreLine(l)) {
        return false;
    }

    // Check for modules that will be reverse parsed
    if (strncasecmp(l, "Node.", strlen("Node.")) == 0 ||
        strncasecmp(l, "Constants.", strlen("Constants.")) == 0 ||
        strncasecmp(l, "Thrift.", strlen("Thrift.")) == 0) {
        return true;
    }

    return false;
}

// XXX this should use log interfaces so that we can persist changes
// safely
Status
Config::parseConfigFile(const char *path,
                        Configuration *config,
                        ParseFilter *pf)
{
    Status status;
    int lineNumber = 0;
    char rawLine[MaxBufSize];
    char *line;
    char *module, *kv, *key, *value;
    bool filterOn = false;
    const char *filtMod = NULL;
    const char *filtKey = NULL;
    KeyValueMappings *mappings = XcalarConfig::get()->mappings_;
    Modules **modules = Config::get()->modules_;

    assert(fp_ == NULL);

    // Save config file location
    if (strlcpy(configFilePath_, path, sizeof(configFilePath_)) >=
        sizeof(configFilePath_)) {
        xSyslog(moduleName,
                XlogErr,
                "Path to config file, '%s', too long",
                path);
        return StatusOverflow;
    }

    // @SymbolCheckIgnore
    fp_ = fopen(path, "r");
    if (fp_ == NULL && errno == ENOENT) {
        status = StatusNoConfigFile;
        goto CommonExit;
    }
    if (fp_ == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    config->nodes = NULL;
    config->thriftHost = NULL;
    if (pf != NULL) {
        filterOn = true;
        filtMod = modules[pf->module]->moduleName_;
        filtKey = mappings[pf->key].keyName;
    }

    while (!feof(fp_)) {
        if (fgets(rawLine, (int) sizeof(rawLine), fp_) == NULL) {
            if (ferror(fp_)) {
                status = sysErrnoToStatus(errno);
                goto CommonExit;
            }
            assert(feof(fp_));
            break;
        }
        lineNumber++;

        line = strTrim(rawLine);

        if (canIgnoreLine(line)) {
            continue;
        }

        // Config lines must be in the format Module.Key=Value
        char *savePtr = NULL;
        module = strtok_r(line, ".", &savePtr);
        if (module == NULL) {
            status = StatusConfigInvalid;
            goto CommonExit;
        } else if (filterOn) {
            // Skip all modules which don't match the filter module
            if (strcasecmp(module, filtMod) != 0) {
                continue;
            }
        }

        kv = (char *) ((uintptr_t) line + strlen(module) + 1);
        savePtr = NULL;
        key = strtok_r(kv, "=", &savePtr);
        if (key == NULL) {
            status = StatusConfigInvalid;
            goto CommonExit;
        } else if (filterOn) {
            // Skip all keys which don't match the filter key
            if (strcasecmp(key, filtKey) != 0) {
                continue;
            }
        }

        value = strtok_r(NULL, "=", &savePtr);
        if (value == NULL) {
            status = StatusConfigInvalid;
            goto CommonExit;
        }

        for (unsigned ii = NodeModule; ii < MaxModule; ii++) {
            assert(modules_[ii]->moduleName_ != NULL);
            if (strcasecmp(module, modules_[ii]->moduleName_) == 0) {
                status = modules_[ii]->parse(config,
                                             key,
                                             value,
                                             KeyValueMappings::LaxEnforcement);
                if (status == StatusUsrNodeIncorrectParams) {
                    // treat as an acceptable error when parsing the config
                    // file.
                    status = StatusOk;
                }
                if (status != StatusOk) {
                    goto CommonExit;
                }
                break;
            }
        }
    }

    status = StatusOk;

CommonExit:
    if (status == StatusConfigInvalid) {
        xSyslog(moduleName,
                XlogErr,
                "Configuration parse error at line #%d: '%s'",
                lineNumber,
                rawLine);
    }  // XXX for some reason rawLine isn't the full line?

    if (fp_ != NULL) {
        // @SymbolCheckIgnore
        fclose(fp_);
        fp_ = NULL;
    }

    return status;
}

void
Config::parseFreeConfig(Configuration *config)
{
    unsigned ii;

    if (config->nodes != NULL) {
        for (ii = 0; ii < config->numNodes; ii++) {
            memFree(config->nodes[ii].ipAddr);
            config->nodes[ii].ipAddr = NULL;
        }
        memFree(config->nodes);
        config->nodes = NULL;
    }

    if (config->thriftHost != NULL) {
        memFree(config->thriftHost);
        config->thriftHost = NULL;
    }
}

// This function builds the input payload to be passed to the XPU/App.
Status
Config::jsonifyInput(const char *filePath,
                     const char *fileContent,
                     char **outStr)
{
    Status status = StatusOk;
    json_t *obj = NULL;
    uint64_t seed;
    *outStr = NULL;

    seed = time(NULL);

    obj = json_pack(
        "{"
        "s:s,"  // func
        "s:I,"  // seed
        "s:s,"  // file path
        "s:s,"  // file content
        "}",
        "func",
        "writeFileAllNodes",
        "seed",
        seed,
        "filePath",
        filePath,
        "fileContent",
        fileContent);

    if (obj == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Unknown json error while formatting input");
        status = StatusJsonError;
        goto CommonExit;
    }

    *outStr = json_dumps(obj, 0);
    if (outStr == NULL) {
        xSyslog(moduleName, XlogErr, "Unknown json error while dumping input");
        status = StatusJsonError;
        goto CommonExit;
    }

CommonExit:

    if (obj) {
        json_decref(obj);
        obj = NULL;
    }
    if (status != StatusOk) {
        if (*outStr) {
            memFree(*outStr);
            *outStr = NULL;
        }
    }

    return status;
}

// This function writes the configuration information out to the
// default.cfg file(s) using an XPU/App.  The App detects whether
// the file is on shared storage (single file) or is not on shared
// storage.  If not on shared storage it assumes that the same
// path is used on all nodes.
Status
Config::writeNewConfigFile(const char *filePath, Configuration *config)
{
    Status status = StatusUnknown;
    char *fileContent = NULL;
    char *cur;
    size_t sizeRemaining;
    size_t sizeUsed;
    App *writeConfigApp = NULL;
    char *inObj = NULL;
    char *outStr = NULL;
    char *errStr = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;

    // This function builds the content of the file into a buffer
    // including new line delimiters and then passes the buffer
    // along with other information to the XPU App.

    fileContent = (char *) memAllocExt(MaxConfigFileSize, moduleName);
    if (fileContent == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Unable to allocate memory (%zu MB) for config file '%s'",
                MaxConfigFileSize,
                filePath);
        goto CommonExit;
    }

    cur = fileContent;
    sizeRemaining = MaxConfigFileSize;

    // Get the content of the existing file that won't be recreated when
    // we reverse parse (e.g. FuncTests.<blah>, comments, etc.).  If this
    // isn't done then the content is not transferred to the updated
    // config file.

    status = getExistingContent(filePath, cur, sizeRemaining, &sizeUsed);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Write Config failed: unable to copy existing content: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    cur += sizeUsed;
    sizeRemaining -= sizeUsed;

    status = reverseParseConstants(config, cur, sizeRemaining, &sizeUsed);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Write Config failed: unable to process constants: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    cur += sizeUsed;
    sizeRemaining -= sizeUsed;

    status = reverseParseThrift(config, cur, sizeRemaining, &sizeUsed);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Write Config failed: unable to process thrift info: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    cur += sizeUsed;
    sizeRemaining -= sizeUsed;

    status = reverseParseNodes(config, cur, sizeRemaining, &sizeUsed);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Write Config failed: unable to process node info: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    cur += sizeUsed;

    // The file content is passed as a single string
    *cur = '\0';

    // Construct the input string to the app.
    status = jsonifyInput(filePath, fileContent, &inObj);
    // The function syslogs any errors
    BailIfFailed(status);

    writeConfigApp =
        AppMgr::get()->openAppHandle(AppMgr::WriteFileAllNodesAppName, &handle);
    if (writeConfigApp == NULL) {
        status = StatusAppDoesNotExist;
        xSyslog(moduleName,
                XlogErr,
                "Write Config app %s does not exist",
                AppMgr::WriteFileAllNodesAppName);
        goto CommonExit;
    }

    // Run one instance of the App on each node of the cluster (App Group).  One
    // of the nodes will be the "master" and coordinate the activities of all
    // the Apps in the App Group.
    xSyslog(moduleName,
            XlogDebug,
            "Starting write config app '%s'",
            writeConfigApp->getName());

    status = AppMgr::get()->runMyApp(writeConfigApp,
                                     AppGroup::Scope::Global,
                                     userIdName,
                                     0,
                                     inObj,
                                     0 /* cookie */,
                                     &outStr,
                                     &errStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Write Config app '%s' failed with status : '%s', "
                "error: '%s', output: '%s'",
                writeConfigApp->getName(),
                strGetFromStatus(status),
                errStr ? errStr : "",
                outStr ? outStr : "");
        goto CommonExit;
    }
    xSyslog(moduleName,
            XlogDebug,
            "Completed write config app '%s'",
            writeConfigApp->getName());

CommonExit:
    if (writeConfigApp != NULL) {
        AppMgr::get()->closeAppHandle(writeConfigApp, handle);
    }
    if (inObj != NULL) {
        memFree(inObj);
    }
    if (fileContent != NULL) {
        memFree(fileContent);
        fileContent = NULL;
    }
    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }
    if (errStr) {
        memFree(errStr);
        errStr = NULL;
    }

    return status;
}

Status
Config::loadLogInfoFromConfig(const char *usrNodeCfgPath)
{
    ParseFilter pf;

    // Set up the Xcalar configuration "knobs" which may get modified when the
    // config file is parsed.
    Status status = XcalarConfig::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Get the Log directory
    pf.module = ConstantsModule;
    pf.key = KeyValueMappings::XcLogDirPath;

    status = parseConfigFile(usrNodeCfgPath, &config_, &pf);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Get the Log level
    pf.module = ConstantsModule;
    pf.key = KeyValueMappings::ClusterLogLevel;

    status = parseConfigFile(usrNodeCfgPath, &config_, &pf);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
Config::loadConfigFiles(const char *usrNodeCfgPath)
{
    // Set up the Xcalar configuration "knobs" which may get modified when the
    // config file is parsed.
    Status status = XcalarConfig::init();
    if (status != StatusOk) {
        return status;
    }

    status = parseConfigFile(usrNodeCfgPath, &config_, NULL);
    if (status == StatusOk) {
        // Do some additional checking.
        for (unsigned ii = 0; ii < config_.numNodes; ii++) {
            if (config_.nodes[ii].ipAddr == NULL) {
                xSyslog(moduleName,
                        XlogCrit,
                        "Missing configuration information for node %d",
                        ii);
                status = StatusConfigInvalid;
                break;
            }
        }
    }

    return status;
}

Status
Config::setConfigParam(char *name, char *value)
{
    Status status;

    // Serialize the requests as the handling breaks with concurrent
    // operations.  This could suspend a bunch of immediate threads but
    // a typical customer will have few parameters changed from their
    // default values and changing them is an infrequent operation (it
    // requires a cluster reboot to take effect).
    // In the future we'll have all the key/values in a single API and
    // will error out if there's an update in progress.  This makes
    // sense as those key/values will be stale (as they're being changed
    // by the update in progress).
    setConfigParamLock_.lock();
    xSyslog(moduleName,
            XlogDebug,
            "Setting config parameter '%s' to value '%s' starting",
            name,
            value);

    status = setConfigParameter(name, value);
    if (status == StatusOk) {
        // Parameter was set.  Write it out.
        status = writeNewConfigFile(configFilePath_, &config_);
        if (status != StatusOk) {
            // Param changed in memory but couldn't be written to disk.
            // XXX:  Think about how to handle this.  Perhaps undo the change in
            // memory...but what if that gets an error?...
            //
            // For now, syslog and return the error.
            xSyslog(moduleName,
                    XlogCrit,
                    "Failed to write configuration parameters to %s:%s",
                    configFilePath_,
                    strGetFromStatus(status));
        }
    }

    xSyslog(moduleName,
            XlogDebug,
            "Setting config parameter '%s' to value '%s' completed",
            name,
            value);

    setConfigParamLock_.unlock();

    return status;
}

Status
Config::writeConfigFiles(const char *usrNodeCfgPath)
{
    return writeNewConfigFile(usrNodeCfgPath, &config_);
}

void
Config::unloadConfigFiles()
{
    parseFreeConfig(&config_);
    activeNodes_ = DefaultNodeInit;
    if (XcalarConfig::get()) {
        XcalarConfig::get()->destroy();
    }
}

// This function gets content of the existing config file that won't be
// reversed parsed.  This content would be otherwise lost.
Status
Config::getExistingContent(const char *filePath,
                           char *buff,
                           size_t buffLen,
                           size_t *lenUsed)
{
    Status status = StatusOk;
    char rawLine[MaxBufSize];
    FILE *fp = NULL;
    char *curBuff = buff;
    size_t curBuffLen = buffLen;
    size_t len;

    *lenUsed = 0;

    // @SymbolCheckIgnore
    fp = fopen(filePath, "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    while (!feof(fp)) {
        if (fgets(rawLine, (int) sizeof(rawLine), fp) == NULL) {
            if (ferror(fp)) {
                status = sysErrnoToStatus(errno);
                goto CommonExit;
            }
            assert(feof(fp));
            break;
        }
        // If the line won't be reverse parsed then we need to save
        // it here so it isn't lost.
        if (!isReverseParsedLine(rawLine)) {
            // Some lines are terminated with \n, some not
            len = strlen(rawLine);
            if (rawLine[len - 1] == '\n') {
                rawLine[len - 1] = '\0';
                len--;
            }
            len = snprintf(curBuff, curBuffLen, "%s\n", rawLine);
            if (len >= curBuffLen) {
                status = StatusNoBufs;
                goto CommonExit;
            }
            *lenUsed += len;
            curBuff += len;
            curBuffLen -= len;
        }
    }

CommonExit:
    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    return status;
}
