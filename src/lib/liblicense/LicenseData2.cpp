// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>
#include <stdint.h>
#include <cstring>
#include <strings.h>
#include <assert.h>
#include <time.h>
#include "StrlFunc.h"
#include "util/System.h"
#include "license/LicenseData2.h"
#include "license/LicenseConstants.h"
#include "LicenseTypes.h"

LicenseData2::LicenseData2()
    : data_(NULL),
      parseBitmap_(NULL),
      requiredBitmap_(NULL),
      signature_(NULL),
      signatureKeys_(NULL),
      signatureKeyCount_(0),
      useSignatureKeys_(false),
      sigParseBitmap_(NULL),
      sigPos_(0),
      iteration_(IterationDefault),
      platform_(DefaultPlatform),
      product_(DefaultProduct),
      productFamily_(DefaultProductFamily),
      licenseExpiryBehavior_(DefaultLicenseExpiryBehavior),
      expirationDate_(ExpirationDateDefault),
      userCount_(UserCountDefault),
      nodeCount_(NodeCountDefault),
      maxInteractiveDataSize_(XcalarConfig::MaxInteractiveDataSizeDefault),
      jdbcEnabled_(JdbcEnabledDefault)

{
    keyValueMap_ =
        new LicenseKeyMetadata[LicenseKeyMetadata::LicenseFieldCount];
    setupKeyValueMap();
};

LicenseData2::~LicenseData2()
{
    int jj = 0;

    if (data_ != NULL) {
        delete[] data_;
        data_ = NULL;
    }

    if (parseBitmap_ != NULL) {
        delete[] parseBitmap_;
        parseBitmap_ = NULL;
    }

    if (requiredBitmap_ != NULL) {
        delete[] requiredBitmap_;
        requiredBitmap_ = NULL;
    }

    if (signature_ != NULL) {
        delete[] signature_;
        signature_ = NULL;
    }

    if (signatureKeys_ != NULL) {
        for (jj = 0; jj < signatureKeyCount_; jj++) {
            delete[] signatureKeys_[jj];
            signatureKeys_[jj] = NULL;
        }
        delete[] signatureKeys_;
        signatureKeys_ = NULL;
        signatureKeyCount_ = 0;
        useSignatureKeys_ = false;
    }

    if (sigParseBitmap_ != NULL) {
        delete[] sigParseBitmap_;
        sigParseBitmap_ = NULL;
    }

    if (keyValueMap_ != NULL) {
        delete[] keyValueMap_;
        keyValueMap_ = NULL;
    }
}

void
LicenseData2::setupKeyValueMap()
{
    for (int ii = 0; ii < LicenseKeyMetadata::LicenseFieldCount; ii++) {
        LicenseKeyMetadata *curMap = &keyValueMap_[ii];

        switch (ii) {
        case LicenseKeyMetadata::Iteration:
            curMap->keyName = "Iteration";
            curMap->variable = &iteration_;
            curMap->bufSize = sizeof(iteration_);
            curMap->defaultFloat = IterationDefault;
            curMap->type = LicenseKeyMetadata::Float;
            curMap->doRangeCheck = true;
            curMap->minValue = IterationMin;
            curMap->maxValue = IterationMax;
            curMap->maskPos = 0;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::Version:
            curMap->keyName = "Version";
            curMap->variable = &version_;
            curMap->bufSize = sizeof(version_);
            curMap->defaultVersion[0] = VersionDefault[0];
            curMap->defaultVersion[1] = VersionDefault[1];
            curMap->defaultVersion[2] = VersionDefault[2];
            curMap->type = LicenseKeyMetadata::VersionType;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 1;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::Platform:
            curMap->keyName = "Platform";
            curMap->variable = &platform_;
            curMap->bufSize = sizeof(platform_);
            curMap->defaultPlatform = DefaultPlatform;
            curMap->type = LicenseKeyMetadata::PlatformType;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 2;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::Product:
            curMap->keyName = "Product";
            curMap->variable = &product_;
            curMap->bufSize = sizeof(product_);
            curMap->defaultProduct = DefaultProduct;
            curMap->type = LicenseKeyMetadata::ProductType;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 3;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::ProductFamily:
            curMap->keyName = "ProductFamily";
            curMap->variable = &productFamily_;
            curMap->bufSize = sizeof(productFamily_);
            curMap->defaultFamily = DefaultProductFamily;
            curMap->type = LicenseKeyMetadata::ProductFamilyType;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 4;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::ExpirationDate:
            curMap->keyName = "ExpirationDate";
            curMap->variable = &expirationDate_;
            curMap->bufSize = sizeof(expirationDate_);
            curMap->defaultTimestamp = ExpirationDateDefault;
            curMap->type = LicenseKeyMetadata::Timestamp;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 5;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::UserCount:
            curMap->keyName = "UserCount";
            curMap->variable = &userCount_;
            curMap->bufSize = sizeof(userCount_);
            curMap->defaultInteger = UserCountDefault;
            curMap->type = LicenseKeyMetadata::Integer;
            curMap->doRangeCheck = true;
            curMap->minValue = UserCountMin;
            curMap->maxValue = UserCountMax;
            curMap->maskPos = 6;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::NodeCount:
            curMap->keyName = "NodeCount";
            curMap->variable = &nodeCount_;
            curMap->bufSize = sizeof(nodeCount_);
            curMap->defaultInteger = NodeCountDefault;
            curMap->type = LicenseKeyMetadata::Integer;
            curMap->doRangeCheck = true;
            curMap->minValue = NodeCountMin;
            curMap->maxValue = NodeCountMax;
            curMap->maskPos = 7;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::Licensee:
            curMap->keyName = "LicensedTo";
            curMap->variable = licensee_;
            curMap->bufSize = sizeof(licensee_);
            curMap->defaultString = (void *) "Nobody";
            curMap->type = LicenseKeyMetadata::String;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 8;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::OnExpiry:
            curMap->keyName = "OnExpiry";
            curMap->variable = &licenseExpiryBehavior_;
            curMap->bufSize = sizeof(&licenseExpiryBehavior_);
            curMap->defaultLicenseExpiryBehavior = LicExpiryBehaviorDisable;
            curMap->type = LicenseKeyMetadata::LicenseExpiryBehaviorType;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 9;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::MaxInteractiveDataSize:
            curMap->keyName = XcalarConfig::MaxInteractiveDataSizeParamName;
            curMap->variable = &maxInteractiveDataSize_;
            curMap->bufSize = sizeof(maxInteractiveDataSize_);
            curMap->defaultLongInteger =
                XcalarConfig::MaxInteractiveDataSizeDefault;
            curMap->type = LicenseKeyMetadata::SizeWithUnitsType;
            curMap->doRangeCheck = true;
            curMap->minValue = 1 * KB;
            curMap->maxValue = 100 * PB;
            curMap->maskPos = 10;
            curMap->minIteration = 2.0;
            break;

        case LicenseKeyMetadata::JdbcEnabled:
            curMap->keyName = "JdbcEnabled";
            curMap->variable = &jdbcEnabled_;
            curMap->bufSize = sizeof(jdbcEnabled_);
            curMap->defaultBoolean = false;
            curMap->type = LicenseKeyMetadata::Boolean;
            curMap->doRangeCheck = false;
            curMap->minValue = 0;
            curMap->maxValue = 0;
            curMap->maskPos = 11;
            curMap->minIteration = 2.0;
            break;

        default:
            assert(0);
            return;
            break;
        }
    }

    for (int ii = 0; ii < LicenseKeyMetadata::LicenseFieldCount; ii++) {
        LicenseKeyMetadata *curMap = &keyValueMap_[ii];

        if (curMap->type == LicenseKeyMetadata::String) {
            memZero(curMap->variable, curMap->bufSize);
            if (curMap->defaultString != NULL) {
                size_t charCount = snprintf((char *) curMap->variable,
                                            curMap->bufSize,
                                            "%s",
                                            (char *) curMap->defaultString);
                if (charCount >= curMap->bufSize) {
                    return;
                }
            }
        }
    }
}

bool
LicenseData2::canIgnoreLine(char *line)
{
    return (strlen(line) < 2) || (line[0] == '/' && line[1] == '/') ||
           (line[0] == '#');
}

char *
LicenseData2::strTrim(char *input)
{
    char *tail;

    // ltrim
    while (strIsWhiteSpace(*input)) {
        input++;
    }

    if (*input == '\0') {
        return input;
    }

    // rtrim
    tail = input + strlen(input) - 1;
    while (tail != input && strIsWhiteSpace(*tail)) {
        tail--;
    }
    *(tail + 1) = '\0';

    return input;
}

bool
LicenseData2::strIsWhiteSpace(char c)
{
    return (c == ' ') || (c == '\n') || (c == '\t');
}

Status
LicenseData2::parseBoolean(char *input, bool *output)
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

int
LicenseData2::parseMonth(char *input)
{
    int month = -1;

    if (strcasecmp(input, "jan") == 0) {
        month = 0;
    } else if (strcasecmp(input, "feb") == 0) {
        month = 1;
    } else if (strcasecmp(input, "mar") == 0) {
        month = 2;
    } else if (strcasecmp(input, "apr") == 0) {
        month = 3;
    } else if (strcasecmp(input, "may") == 0) {
        month = 4;
    } else if (strcasecmp(input, "jun") == 0) {
        month = 5;
    } else if (strcasecmp(input, "jul") == 0) {
        month = 6;
    } else if (strcasecmp(input, "aug") == 0) {
        month = 7;
    } else if (strcasecmp(input, "sep") == 0) {
        month = 8;
    } else if (strcasecmp(input, "oct") == 0) {
        month = 9;
    } else if (strcasecmp(input, "nov") == 0) {
        month = 10;
    } else if (strcasecmp(input, "dec") == 0) {
        month = 11;
    }

    return month;
}

Status
LicenseData2::parse(const char *data)
{
    Status status;
    char rawLine[LicenseLineBufSize];
    char licData[LicenseKeyBufSize];
    char *pos = NULL;
    char *savePtr = NULL;
    char *line;
    long idx = 0;

    assert(data != NULL);

    if (data_ != NULL) {
        delete[] data_;
        data_ = NULL;
    }

    if (parseBitmap_ != NULL) {
        delete[] parseBitmap_;
        parseBitmap_ = NULL;
    }

    if (requiredBitmap_ != NULL) {
        delete[] requiredBitmap_;
        requiredBitmap_ = NULL;
    }

    data_ = new (std::nothrow) unsigned char[strlen(data) + 1];
    if (data_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    strlcpy(licData, data, strlen(data) + 1);
    strlcpy((char *) data_, data, strlen(data) + 1);

    parseBitmap_ = new (std::nothrow) unsigned char[bitmapSize_];
    if (parseBitmap_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(parseBitmap_, bitmapSize_);

    requiredBitmap_ = new (std::nothrow) unsigned char[bitmapSize_];
    if (requiredBitmap_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(requiredBitmap_, bitmapSize_);

    for (pos = strtok_r(licData, "\n", &savePtr); pos != NULL;
         pos = strtok_r(NULL, "\n", &savePtr)) {
        strlcpy(rawLine, pos, LicenseLineBufSize);
        idx += strlen(rawLine) + 1;

        line = strTrim(rawLine);

        if (canIgnoreLine(line)) {
            continue;
        }

        status = parseLine(rawLine);

        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (!parseComplete(parseBitmap_, requiredBitmap_, parseFieldCount_)) {
        status = StatusLicErr;
    } else {
        status = StatusOk;
    }

CommonExit:
    return status;
}

Status
LicenseData2::parseLine(char *data)
{
    char *key, *value;
    char *savePtr = NULL;
    Status status = StatusOk;

    savePtr = NULL;
    key = strtok_r(data, "=", &savePtr);
    if (key == NULL) {
        status = StatusLicErr;
        goto CommonExit;
    }

    value = strtok_r(NULL, "=", &savePtr);
    if (value == NULL) {
        status = StatusLicErr;
        goto CommonExit;
    }

    status = parseKV(key, value);

CommonExit:
    return status;
}

Status
LicenseData2::parseKV(char *key, char *value)
{
    Status status = StatusOk;
    int *linteger;
    long *llong;
    char *lstring;
    int day;
    char month[8];
    int year;
    long epoch;
    time_t *ltstamp;
    float *lfloat;
    int *lversion;
    LicenseExpiryBehavior *lExpiryBehavior;
    LicensePlatform *lplatform;
    LicenseProduct *lproduct;
    LicenseProductFamily *lfamily;
    int flag;

    for (int ii = 0; ii < LicenseKeyMetadata::LicenseFieldCount; ii++) {
        LicenseKeyMetadata *curMap = &keyValueMap_[ii];

        if (strcmp(key, curMap->keyName) != 0) {
            continue;
        }

        switch (curMap->type) {
        case LicenseKeyMetadata::Boolean:
            status = parseBoolean(value, (bool *) curMap->variable);

            break;
        case LicenseKeyMetadata::Integer:
            linteger = (int *) curMap->variable;
            *linteger = atoi(value);

            if (curMap->doRangeCheck && ((*linteger < curMap->minValue) ||
                                         (*linteger > curMap->maxValue))) {
                status = StatusLicValueOutOfRange;
            }

            break;
        case LicenseKeyMetadata::LongInteger:
            llong = (long *) curMap->variable;
            *llong = atol(value);

            if (curMap->doRangeCheck &&
                ((*llong < curMap->minValue) || (*llong > curMap->maxValue))) {
                status = StatusLicValueOutOfRange;
            }

            break;
        case LicenseKeyMetadata::String:
            lstring = (char *) curMap->variable;
            strlcpy(lstring, value, curMap->bufSize);

            break;
        case LicenseKeyMetadata::Timestamp:
            ltstamp = (time_t *) curMap->variable;

            if (sscanf(value, "%04d-%3s-%02d", &year, month, &day) == 3) {
                int month_int = parseMonth(month);

                if (month_int < 0 || month_int > 11) {
                    status = StatusLicErr;
                } else {
                    struct tm tmtime;
                    tmtime.tm_sec = 0;
                    tmtime.tm_min = 0;
                    tmtime.tm_hour = 0;
                    tmtime.tm_mday = day;
                    tmtime.tm_mon = month_int;
                    tmtime.tm_year = year - 1900;

                    *ltstamp = mktime(&tmtime);
                }
            } else if (sscanf(value, "%ld", &epoch) == 1) {
                *ltstamp = epoch;
            } else {
                status = StatusLicErr;
            }

            break;
        case LicenseKeyMetadata::Float:
            lfloat = (float *) curMap->variable;
            *lfloat = (float) strtod(value, NULL);

            if (curMap->doRangeCheck && ((*lfloat < curMap->minValue) ||
                                         (*lfloat > curMap->maxValue))) {
                status = StatusLicValueOutOfRange;
            }

            break;
        case LicenseKeyMetadata::VersionType:
            lversion = (int *) curMap->variable;
            flag = sscanf(value,
                          "%d.%d.%d",
                          &lversion[0],
                          &lversion[1],
                          &lversion[2]);
            if (flag != 3) {
                status = StatusLicErr;
            }

            break;
        case LicenseKeyMetadata::PlatformType:
            lplatform = (LicensePlatform *) curMap->variable;
            *lplatform = strToLicensePlatform(value);
            if (!isValidLicensePlatform(*lplatform)) {
                status = StatusLicErr;
            }
            break;

        case LicenseKeyMetadata::ProductType:
            lproduct = (LicenseProduct *) curMap->variable;
            *lproduct = strToLicenseProduct(value);
            if (!isValidLicenseProduct(*lproduct)) {
                status = StatusLicErr;
            }
            break;

        case LicenseKeyMetadata::ProductFamilyType:
            lfamily = (LicenseProductFamily *) curMap->variable;
            *lfamily = strToLicenseProductFamily(value);
            if (!isValidLicenseProductFamily(*lfamily)) {
                status = StatusLicErr;
            }
            break;

        case LicenseKeyMetadata::LicenseExpiryBehaviorType:
            lExpiryBehavior = (LicenseExpiryBehavior *) curMap->variable;
            *lExpiryBehavior = strToLicenseExpiryBehavior(value);
            if (!isValidLicenseExpiryBehavior(*lExpiryBehavior)) {
                status = StatusLicErr;
            }
            break;

        case LicenseKeyMetadata::SizeWithUnitsType: {
            char *p;
            long numBytes;

            numBytes = strtoull(value, &p, BaseCanonicalForm);
            if (p == value || strlen(p) == 0) {
                status = StatusLicErr;
                break;
            }

            if (numBytes < 0) {
                status = StatusLicErr;
                break;
            }

            if (strcasecmp(p, "EB") == 0) {
                numBytes *= EB;
            } else if (strcasecmp(p, "PB") == 0) {
                numBytes *= PB;
            } else if (strcasecmp(p, "TB") == 0) {
                numBytes *= TB;
            } else if (strcasecmp(p, "GB") == 0) {
                numBytes *= GB;
            } else if (strcasecmp(p, "MB") == 0) {
                numBytes *= MB;
            }

            llong = (long *) curMap->variable;
            *llong = numBytes;

            break;
        }

        default:
            status = StatusLicErr;
            break;
        }

        if (status != StatusOk) {
            break;
        }

        markFound(parseBitmap_, curMap->maskPos);
        markRequired(requiredBitmap_, curMap->maskPos, curMap->minIteration);
    }

    return status;
}

void
LicenseData2::markFound(unsigned char *bitmap, int idx)
{
    if (bitmap == NULL) {
        return;
    }

    unsigned char mask = 0x1 << idx % 8;
    int byte = idx / 8;

    bitmap[byte] |= mask;
}

void
LicenseData2::markRequired(unsigned char *bitmap, int idx, float minIter)
{
    if (iteration_ >= minIter) {
        markFound(bitmap, idx);
    }
}

bool
LicenseData2::parseComplete(unsigned char *foundBitmap,
                            unsigned char *requiredBitmap,
                            int fieldCount)
{
    if (foundBitmap == NULL) {
        return false;
    }

    unsigned char mask = 0x1;

    for (int ii = 0; ii < fieldCount; ii++, mask = (0x1 << ii % 8)) {
        if (requiredBitmap == NULL) {
            if ((mask & foundBitmap[ii / 8]) == 0) {
                return false;
            }
        } else {
            if (((mask & foundBitmap[ii / 8]) == 0) &&
                ((mask & requiredBitmap[ii / 8]) != 0)) {
                return false;
            }
        }
    }

    return true;
}

void
LicenseData2::dump(char **data, int len)
{
    strlcpy(*data, (const char *) data_, len);
}

Status
LicenseData2::createSignatureData(const char *data, char *sigData)
{
    char licData[LicenseKeyBufSize];
    char rawLine[LicenseLineBufSize];
    int sigDataPos = 0;
    Status status = StatusOk;
    char *pos = NULL;
    long idx = 0;
    char *savePtr = NULL;
    char *line;

    assert(data != NULL);
    if (useSignatureKeys_) {
        strlcpy(licData, data, strlen(data) + 1);

        if (sigParseBitmap_ != NULL) {
            delete[] sigParseBitmap_;
            sigParseBitmap_ = NULL;
        }

        sigParseBitmap_ =
            new (std::nothrow) unsigned char[signatureKeyCount_ + 1];
        if (sigParseBitmap_ == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        memZero(sigParseBitmap_, signatureKeyCount_ + 1);

        for (pos = strtok_r(licData, "\n", &savePtr); pos != NULL;
             pos = strtok_r(NULL, "\n", &savePtr)) {
            strlcpy(rawLine, pos, LicenseLineBufSize);
            idx = strlen(rawLine) + 1;

            line = strTrim(rawLine);

            if (canIgnoreLine(line)) {
                continue;
            }

            if (parseSignatureDataLine(line)) {
                snprintf(sigData + sigDataPos,
                         LicenseLineBufSize,
                         "%s\n",
                         rawLine);
                sigDataPos += idx;
            }
        }

        if (!parseComplete(sigParseBitmap_, NULL, signatureKeyCount_)) {
            status = StatusLicSignatureInvalid;
        }

    } else {
        strlcpy(sigData, data, sigPos_ + 1);
    }

CommonExit:
    return status;
}

bool
LicenseData2::parseSignatureDataLine(const char *data)
{
    char line[LicenseLineBufSize];
    char *key;
    char *savePtr = NULL;
    int ii = 0;

    strlcpy(line, data, strlen(data) + 1);

    savePtr = NULL;
    key = strtok_r(line, "=", &savePtr);
    if (key == NULL) {
        return false;
    }

    if (strcmp(key, data) == 0) {
        return false;
    }

    for (ii = 0; ii < signatureKeyCount_; ii++) {
        if ((strncmp(key, signatureKeys_[ii], LicenseMaxKeySize) == 0) ||
            (strncmp(key, "signatureKeys", LicenseMaxKeySize) == 0)) {
            markFound(sigParseBitmap_, ii);
            return true;
        }
    }

    return false;
}

bool
LicenseData2::prepSignature(char *data)
{
    bool retval = true;

    parseSignature(data, true);

    if (useSignatureKeys_) {
        retval = (signatureKeyCount_ != 0);
    }

    return retval;
}

Status
LicenseData2::parseSignature(const char *data, bool signKey)
{
    Status status = StatusLicSignatureInvalid;
    char rawLine[LicenseLineBufSize];
    char licData[LicenseKeyBufSize];
    char *pos = NULL;
    long idx = 0;
    long lastIdx = 0;
    char *savePtr = NULL;
    char *line;

    assert(data != NULL);
    strlcpy(licData, data, strlen(data) + 1);

    for (pos = strtok_r(licData, "\n", &savePtr); pos != NULL;
         pos = strtok_r(NULL, "\n", &savePtr)) {
        lastIdx += idx;

        strlcpy(rawLine, pos, LicenseLineBufSize);
        idx = strlen(rawLine) + 1;

        line = strTrim(rawLine);

        if (canIgnoreLine(line)) {
            continue;
        }

        if (parseSignatureLine(line)) {
            status = StatusOk;
            sigPos_ = lastIdx;
        }

        parseSignatureKeysLine(line);

        parseIterationLine(line);
    }

    // if we are signing the key, the signature
    // is at the end of the file
    if (signKey) {
        status = StatusOk;
        sigPos_ = lastIdx + idx;
    }

    return status;
}

void
LicenseData2::parseSignatureKeysLine(const char *data)
{
    char line[LicenseLineBufSize];
    char *key, *value;
    char *savePtr = NULL;
    char *commaPtr = NULL;
    char *pos = NULL;
    int commaCount = 2;
    unsigned long ii = 0;
    int jj = 0;

    strlcpy(line, data, strlen(data) + 1);

    savePtr = NULL;
    key = strtok_r(line, "=", &savePtr);
    if (key == NULL) {
        return;
    }

    if ((strcmp(key, data) == 0) || (strcmp(key, "signatureKeys") != 0)) {
        return;
    }

    value = strtok_r(NULL, "=", &savePtr);
    if (value == NULL) {
        return;
    }

    for (ii = 0; ii < strlen(value); ii++) {
        if (value[ii] == ',') {
            commaCount++;
        }
    }

    if (signatureKeys_ != NULL) {
        for (jj = 0; jj < signatureKeyCount_; jj++) {
            delete[] signatureKeys_[jj];
        }
        delete[] signatureKeys_;
        signatureKeys_ = NULL;
    }

    signatureKeys_ = new char *[commaCount];
    for (jj = 0; jj < commaCount; jj++) {
        signatureKeys_[jj] = new char[LicenseMaxKeySize];
    }
    signatureKeyCount_ = commaCount;

    strlcpy(signatureKeys_[0], "signatureKeys", LicenseMaxKeySize);
    for (pos = strtok_r(value, ",", &commaPtr), jj = 1; pos != NULL;
         pos = strtok_r(NULL, ",", &commaPtr), jj++) {
        strlcpy(signatureKeys_[jj], pos, LicenseMaxKeySize);
    }

    useSignatureKeys_ = true;
}

bool
LicenseData2::parseSignatureLine(const char *data)
{
    char line[LicenseLineBufSize];
    char *key, *value;
    char *savePtr = NULL;

    strlcpy(line, data, strlen(data) + 1);

    savePtr = NULL;
    key = strtok_r(line, "=", &savePtr);
    if (key == NULL) {
        return false;
    }

    if ((strcmp(key, data) == 0) || (strcmp(key, "signature") != 0)) {
        return false;
    }

    value = strtok_r(NULL, "=", &savePtr);
    if (value == NULL) {
        return false;
    }

    if (signature_ != NULL) {
        delete[] signature_;
        signature_ = NULL;
    }

    signature_ = new (std::nothrow) unsigned char[strlen(value) + 1];
    if (signature_ == NULL) {
        return false;
    }

    strlcpy((char *) signature_, value, LicenseLineBufSize);

    return true;
}

void
LicenseData2::parseIterationLine(char *data)
{
    char line[LicenseLineBufSize];
    char *key, *value;
    char *savePtr = NULL;

    strlcpy(line, data, strlen(data) + 1);

    savePtr = NULL;
    key = strtok_r(line, "=", &savePtr);
    if (key == NULL) {
        return;
    }

    if ((strcmp(key, data) == 0) || (strcmp(key, "Iteration") != 0)) {
        return;
    }

    value = strtok_r(NULL, "=", &savePtr);
    if (value == NULL) {
        return;
    }

    iteration_ = (float) strtod(value, NULL);
}
