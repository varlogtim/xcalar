// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdarg.h>
#include <stdlib.h>
#include <cryptopp/eccrypto.h>
#include <cryptopp/pssr.h>
#include <cryptopp/osrng.h>
#include <cryptopp/oids.h>
#include "primitives/Primitives.h"
#include "license/LicenseKeyFile.h"
#include "license/LicenseConstants.h"

using namespace CryptoPP;

#define KeyPairPathLen 256

void
usage(char *program_name)
{
    fprintf(stderr, "Usage is: \n");
    fprintf(stderr, "%s <file name root>\n", program_name);
    fprintf(stderr, "Where:\n");
    fprintf(stderr, "A public and private key file pair will be named\n");
    fprintf(stderr, "<file name root>-pub.key and <file name root>-priv.key\n");
}

int
main(int argc, char *argv[])
{
    if (argc != 2) {
        usage(argv[0]);
        // @SymbolCheckIgnore
        exit(1);
    }

    if ((strlen(argv[1]) == 0) || (strlen(argv[1]) >= KeyPairPathLen)) {
        fprintf(stderr, "The file name root for the key pair is too long\n");
        usage(argv[0]);
        // @SymbolCheckIgnore
        exit(1);
    }

    char *file_root = argv[1];

    char pub_key_name[KeyPairPathLen];
    snprintf(pub_key_name, KeyPairPathLen, "%s-pub.key", file_root);

    char priv_key_name[KeyPairPathLen];
    int trunc =
        snprintf(priv_key_name, KeyPairPathLen, "%s-priv.key", file_root);

    if (trunc >= KeyPairPathLen) {
        fprintf(stderr, "The file name root for the key pair is too long\n");
        usage(argv[0]);
        // @SymbolCheckIgnore
        exit(1);
    }

    printf("%s\n", pub_key_name);
    printf("%s\n", priv_key_name);

    AutoSeededRandomPool rng;

    ECDSA<ECP, SHA1>::PrivateKey private_key;
    ECDSA<ECP, SHA1>::PublicKey public_key;

    private_key.Initialize(rng, CryptoPP::ASN1::secp160r1());
    private_key.MakePublicKey(public_key);

    if (int priv_status =
            LicenseKeyFile::savePrivateKey(priv_key_name, &private_key) !=
            StatusOk) {
        switch (priv_status) {
        case StatusCodeLicFileOpen:
            fprintf(stderr, "Error opening private key\n");
            break;
        case StatusCodeLicFileWrite:
            fprintf(stderr, "Error writing private key\n");
            break;
        default:
            fprintf(stderr,
                    "Unknown LicenseKeyFile private key error: %d\n",
                    priv_status);
        }
        // @SymbolCheckIgnore
        exit(1);
    }

    if (int pub_status =
            LicenseKeyFile::savePublicKey(pub_key_name, &public_key) !=
            StatusOk) {
        switch (pub_status) {
        case StatusCodeLicFileOpen:
            fprintf(stderr, "Error opening public key\n");
            break;
        case StatusCodeLicFileWrite:
            fprintf(stderr, "Error writing public key\n");
            break;
        default:
            fprintf(stderr,
                    "Unknown LicenseKeyFile public key error: %d\n",
                    pub_status);
        }
        // @SymbolCheckIgnore
        exit(1);
    }

    return 0;
}
