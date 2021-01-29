// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <time.h>
#include <cryptopp/oids.h>
#include <cryptopp/files.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <getopt.h>
#include "primitives/Status.h"
#include "StrlFunc.h"
#include "license/LicenseKeyFile.h"
#include "license/LicenseData2.h"
#include "license/LicenseReader2.h"
#include "license/LicenseConstants.h"
#include "LicenseEmbedded.h"

const char *
statErrno(int myErrno)
{
    switch (myErrno) {
    case EACCES:
        return "Permission Denied";
        break;
    case EBADF:
        return "Bad File Number";
        break;
    case EFAULT:
        return "Bad Address";
        break;
    case ELOOP:
        return "Too many symbolic links encountered";
        break;
    case ENAMETOOLONG:
        return "File name too long";
        break;
    case ENOENT:
        return "No such file or directory";
        break;
    case ENOMEM:
        return "Out of resources";
        break;
    case ENOTDIR:
        return "Not a directory";
        break;
    case EOVERFLOW:
        return "Value too large for defined data type";
        break;
    }
    return "Unknown error";
}

void
usage(char *argv[])
{
    fprintf(stderr, "%s [options] [required args]\n", argv[0]);
    fprintf(stderr, "Required arguments:\n");
    fprintf(stderr, "-k|--keypath <public key file path>\n");
    fprintf(stderr, "-e|--keyvar <base64 public key environment variable>\n");
    fprintf(stderr, "-l|--license <license key file path>\n");
    fprintf(stderr, "-p|--password <password>\n");
    fprintf(stderr, "Options:\n");
}

int
main(int argc, char *argv[])
{
    struct stat statBuf;
    char *outBuf;
    int optionIndex = 0;
    int flag = 0;
    char publicKeyPath[LicenseMaxFilePathLen] = "";
    char pubKeyVarName[LicensePasswdLen] = "";
    char licKeyPath[LicenseMaxFilePathLen] = "";
    char licData[LicenseKeyBufSize];
    const char *password = NULL;
    char passwordBuf[LicensePasswdLen];
    bool keySet = false;
    bool keyVarSet = false;
    bool licSet = false;

    static struct option long_options[] = {
        {"keypath", required_argument, 0, 'k'},
        {"keyvar", required_argument, 0, 'e'},
        {"license", required_argument, 0, 'l'},
        {"password", required_argument, 0, 'p'},
    };

    // The ":" follows options with required args.
    while (
        (flag =
             getopt_long(argc, argv, "k:l:p:e:", long_options, &optionIndex)) !=
        -1) {
        int ret = 0;

        switch (flag) {
        case 'k':
            assert(optarg != NULL);
            if (strlen(optarg) >= LicenseMaxFilePathLen) {
                fprintf(stderr,
                        "Public key file path is too long (>= %d characters)",
                        LicenseMaxFilePathLen);
                // @SymbolCheckIgnore
                exit(1);
            }
            strlcpy(publicKeyPath, optarg, LicenseMaxFilePathLen);
            ret = stat(publicKeyPath, &statBuf);
            if (ret != 0) {
                const char *err_msg = statErrno(errno);
                fprintf(stderr,
                        "Unable to open %s: %s\n",
                        publicKeyPath,
                        err_msg);
                usage(argv);
                // @SymbolCheckIgnore
                exit(1);
            }
            keySet = true;
            break;
        case 'e':
            assert(optarg != NULL);
            if (strlen(optarg) >= LicenseMaxFilePathLen) {
                fprintf(stderr,
                        "Public key file path is too long (>= %d characters)",
                        LicenseMaxFilePathLen);
                // @SymbolCheckIgnore
                exit(1);
            }
            strlcpy(pubKeyVarName, optarg, LicensePasswdLen);
            if (getenv(pubKeyVarName) == NULL) {
                fprintf(stderr,
                        "The environment variable %s does not exist",
                        pubKeyVarName);
                // @SymbolCheckIgnore
                exit(1);
            }
            keyVarSet = true;
            break;
        case 'l':
            assert(optarg != NULL);
            if (strlen(optarg) >= LicenseMaxFilePathLen) {
                fprintf(stderr,
                        "License key is too long (>= %d characters)",
                        LicenseMaxFilePathLen);
                // @SymbolCheckIgnore
                exit(1);
            }
            strlcpy(licKeyPath, optarg, LicenseMaxFilePathLen);
            ret = stat(licKeyPath, &statBuf);
            if (ret != 0) {
                const char *err_msg = statErrno(errno);
                fprintf(stderr, "Unable to open %s: %s\n", licKeyPath, err_msg);
                usage(argv);
                // @SymbolCheckIgnore
                exit(1);
            }
            licSet = true;
            break;
        case 'p':
            assert(optarg != NULL);
            if (strlen(optarg) >= LicensePasswdLen) {
                fprintf(stderr,
                        "Password is too long (>= %d characters)",
                        LicensePasswdLen);
                // @SymbolCheckIgnore
                exit(1);
            }
            strlcpy(passwordBuf, optarg, LicensePasswdLen);
            password = passwordBuf;
            break;
        default:
            fprintf(stderr, "Unknown argument\n");
            usage(argv);
            // @SymbolCheckIgnore
            exit(1);
        }
    }

    if (keySet && keyVarSet) {
        fprintf(stderr,
                "Either key file (-k) or key variable (-e) must be set, not "
                "both\n");
        usage(argv);
        // @SymbolCheckIgnore
        exit(1);
    }

    if (!(licSet && (keySet || keyVarSet))) {
        fprintf(stderr, "Public key and license must be set\n");
        usage(argv);
        // @SymbolCheckIgnore
        exit(1);
    }

    Status lic_status =
        LicenseKeyFile::loadData(licKeyPath, licData, LicenseKeyBufSize);

    if (lic_status != StatusOk) {
        switch (lic_status.code()) {
        case StatusCodeLicFileOpen:
            fprintf(stderr, "Error opening license file\n");
            break;
        case StatusCodeLicFileRead:
            fprintf(stderr, "Error reading license file\n");
            break;
        case StatusCodeLicBufTooSmall:
            fprintf(stderr, "License file is bigger than internal buffer\n");
            break;
        default:
            fprintf(stderr,
                    "Unknown LicenseKeyFile error: %d\n",
                    lic_status.code());
            break;
        }
        // @SymbolCheckIgnore
        exit(1);
    }

    if (password == NULL) {
        Status status = LicenseEmbedded::init();
        if (status != StatusOk) {
            fprintf(stderr, "Failed to initialize embedded license");
            // @SymbolCheckIgnore
            exit(1);
        }
        password = LicenseEmbedded::getPassword();
    }

    LicenseReader2 reader((keySet) ? publicKeyPath : NULL,
                          (keyVarSet) ? pubKeyVarName : NULL,
                          NULL,
                          password);

    LicenseData2 loaded_data;
    Status reader_status = reader.read(licData, &loaded_data);
    if (reader_status != StatusOk) {
        switch (reader_status.code()) {
        case StatusCodeLicFileOpen:
            fprintf(stderr, "Error opening signature or public key file\n");
            break;
        case StatusCodeLicFileRead:
            fprintf(stderr, "Error reading signature or public key file\n");
            break;
        case StatusCodeLicBufTooSmall:
            fprintf(stderr, "Signature file is bigger than internal buffer\n");
            break;
        case StatusCodeLicInputInvalid:
            fprintf(stderr, "Public key type is unknown\n");
            break;
        case StatusCodeLicPubKeyMissing:
            fprintf(stderr, "Public key is not defined\n");
            break;
        case StatusCodeLicPubKeyErr:
            fprintf(stderr, "Public key has an error\n");
            break;
        case StatusCodeLicPasswdMissing:
            fprintf(stderr, "Password is not defined\n");
            break;
        case StatusCodeLicPasswordError:
            fprintf(stderr,
                    "Password used to verify the license is incorrect\n");
            break;
        case StatusCodeLicLicenseMissing:
            fprintf(stderr, "License is not defined\n");
            break;
        case StatusCodeLicSignatureMissing:
            fprintf(stderr, "Signature is not defined\n");
            break;
        case StatusCodeLicSignatureInvalid:
            fprintf(stderr, "Error verifying signature\n");
            break;
        case StatusCodeLicValueOutOfRange:
            fprintf(stderr, "One or more license fields out of range\n");
            break;
        case StatusCodeConfigInvalid:
            fprintf(stderr, "Error parsing license\n");
            break;
        default:
            fprintf(stderr,
                    "Unknown Reader error code: %d\n",
                    reader_status.code());
            break;
        }
        // @SymbolCheckIgnore
        exit(1);
    }

    outBuf = new char[2048];

    loaded_data.dump((char **) &outBuf, 2048);

    printf("%s", outBuf);

    delete[] outBuf;

    return 0;
}
