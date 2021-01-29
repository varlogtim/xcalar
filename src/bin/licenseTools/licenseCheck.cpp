// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <iostream>
#include <iomanip>
#include <time.h>
#include <cryptopp/oids.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <strings.h>
#include <unistd.h>
#include <stdlib.h>
#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "primitives/Status.h"
#include "license/LicenseData2.h"
#include "license/LicenseReader2.h"
#include "license/LicenseKeyFile.h"
#include "license/LicenseConstants.h"
#include "LicenseEmbedded.h"

using namespace std;

void
usage(char *program_name)
{
    cout << "Usage is: " << endl;
    cout << program_name << " <public key file name> <key>" << endl;
    cout << "Where: " << endl;
    cout << "<public key file name> is the name of the public "
         << "key file that validates the license." << endl;
    cout << "<key file name> is a Xcalar license key" << endl;
}

int
main(int argc, char *argv[])
{
    struct stat keyStat;
    char *keyFileName = argv[2];
    char *licenseString = NULL;
    const char *password = NULL;
    Status status = StatusOk;
    LicenseData2 loadedData;

    if (argc != 3) {
        usage(argv[0]);
        // @SymbolCheckIgnore
        exit(1);
    }

    if (stat(keyFileName, &keyStat) != 0) {
        int error_code = errno;
        status = StatusLicFileOpen;
        fprintf(stderr,
                "Error getting size of file %s: %d\n",
                keyFileName,
                error_code);
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    licenseString = (char *) malloc(keyStat.st_size + 1);
    status = LicenseKeyFile::loadData(keyFileName,
                                      licenseString,
                                      keyStat.st_size + 1);
    if (status != StatusOk) {
        switch (status.code()) {
        case StatusCodeLicFileOpen:
            fprintf(stderr, "Error opening license file\n");
            break;
        case StatusCodeLicFileRead:
            fprintf(stderr, "Error reading license key\n");
            break;
        case StatusCodeLicBufTooSmall:
            fprintf(stderr, "License key data buffer is too small\n");
            break;
        default:
            fprintf(stderr,
                    "Unknown LicenseKeyFile error: %d\n",
                    status.code());
            break;
        }
        goto CommonExit;
    }

    status = LicenseEmbedded::init();
    if (status != StatusOk) {
        fprintf(stderr, "Failed to initialize embedded license");
        goto CommonExit;
    }

    password = LicenseEmbedded::getPassword();
    {
        LicenseReader2 reader(argv[1], NULL, NULL, password);
        status = reader.read(licenseString, &loadedData);
        if (status != StatusOk) {
            switch (status.code()) {
            case StatusCodeConfigInvalid:
                fprintf(stderr, "Error license field invalid\n");
                break;
            case StatusCodeLicValueOutOfRange:
                fprintf(stderr, "Error license field out of range\n");
                break;
            case StatusCodeLicSignatureInvalid:
                fprintf(stderr, "License signature is invalid\n");
                break;
            case StatusCodeLicPasswordError:
                fprintf(stderr, "Error verifying license password\n");
                break;
            default:
                fprintf(stderr,
                        "Unknown Reader error code: %d\n",
                        status.code());
                break;
            }
            // @SymbolCheckIgnore
            goto CommonExit;
        }
    }

    if (loadedData.getExpiration() < time(NULL)) {
        status = StatusLicExpired;
        fprintf(stderr, "License is expired\n");
    }

CommonExit:
    if (licenseString != NULL) {
        // @SymbolCheckIgnore
        free(licenseString);
    }

    if (status != StatusOk) {
        // @SymbolCheckIgnore
        exit(1);
    }
}
