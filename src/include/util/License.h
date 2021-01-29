// Copyright 2015 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LICENSE_H_
#define _LICENSE_H_

#include "license/LicenseData2.h"
#include "ns/LibNsTypes.h"
#include "LicenseEmbedded.h"

// This initial license values are saved in /etc/xcalar/ by the installation
// process.  When the cluster first boots, node 0 will copy the essence of
// the file into the XLRROOT/license/ directory.  This becomes the golden
// version.  Subsequent license updates will not change /etc/xcalar/ but will
// create a new file in XLRROOT/license/.  Each file is named XcalarLic#<num>
// where <num> is a successively higher number.  This enables us to guarantee
// that a consistent file is always available even when failures occur.  To
// protect against concurrent license updates a libNS path, named
// /license/XcalarLicense is published.  In order to update the license, the
// updater must have read-write access to the path name.

class LicenseMgr final
{
  public:
    static MustCheck Status init(const char *pubSigningKeyPath);
    MustCheck Status initInternal(const char *pubSigningKeyPath);
    void destroy();
    MustCheck static LicenseMgr *get();
    MustCheck bool licIsExpired();
    MustCheck bool licIsLoaded();
    MustCheck Status licGetLoadStatus();
    MustCheck Status installLicense(const char *licenseFile);
    MustCheck Status updateLicense(const char *compressedLicenseString,
                                   uint64_t compressedLicenseSize);
    MustCheck Status licVerify(char *licenseKey, int licKeySize);
    MustCheck Status destroyLicense();
    MustCheck Status testLicense(LicenseData2 *loadedData);
    MustCheck Status boottimeInit();

    const char *getPassword() { return LicenseEmbedded::getPassword(); }

    const char *getPubSigningKey();

    LicenseData2 *getLicenseData() { return &theLicense_; }

    const char *getLicenseString() { return currentLicenseString_; }

    Status getCompressedLicense(char **compressedLicense,
                                size_t *compressedLicenseSize)
    {
        return compressLicense(currentLicenseString_,
                               currentLicenseSize_,
                               compressedLicense,
                               compressedLicenseSize);
    }

    // GVM handlers
    MustCheck Status updateLicenseLocal(void *args);
    MustCheck Status revertLicenseLocal(void *args);

  private:
    static constexpr const char *moduleName = "LicenseMgr";
    // These are for the license file that is kept in the license
    // directory in XLRROOT.  The highest numbered file is the
    // definitive version unless a failure occurred while the file
    // was being written.
    static constexpr const char *LicenseNamePrefix = "XcalarLic";
    static constexpr const char *LicenseVersionDelim = "#";
    // These are used for the license file that is installed into
    // /etc/xcalar/.  The contents of the file are the initial content
    // of what is stored in the license XLRROOT directory.  Once any
    // license update is done (via the GUI) then the version in
    // /etc/xcalar would be stale.
    static constexpr int LicenseDirLength = 1023;
    static constexpr int LicenseStringSize = 1023;
    static constexpr int LicenseFileMinSize = 101;
    static constexpr int MaxFileNameLength = 255;

    // Contents of durable license file
    struct LicensePersistModule {
        uint64_t crc;     // crc for the rest of the struct (must be first)
        uint64_t ver;     // version; should match that encoded in name (must
                          //  be second).
        uint64_t size;    // size of the rest of the content
        char content[0];  // content of the file
    };

    // LibNs related...
    static constexpr const char *LicensePrefix = "/license/";
    static constexpr const char *LicenseFileName = "XcalarLicense";

    class LicenseRecord final : public NsObject
    {
      public:
        static constexpr uint64_t StartVersion = 0;
        static constexpr uint64_t InvalidVersion = -1;
        LicenseRecord(uint64_t consistentVersion)
            : NsObject(sizeof(LicenseRecord)),
              consistentVersion_(consistentVersion)
        {
        }

        ~LicenseRecord() {}

        // Current consistent version number
        uint64_t consistentVersion_ = InvalidVersion;

      private:
        LicenseRecord(const LicenseRecord &) = delete;
        LicenseRecord &operator=(const LicenseRecord &) = delete;
    };

    static LicenseMgr *instance;

    Mutex updateLock_;
    bool isLicLoaded_ = false;
    Status licLoadStatus_ = StatusLicMissing;
    LicenseData2 theLicense_;
    uint64_t currentVersionNum_ = 0;

    // If a user provides the public signing key, instead of using the one
    // that's burned in
    char *pubSigningKey_ = NULL;

    // License info as read from the installation file (if it
    // was read).  This becomes the current license once it has
    // been persisted to the XLRROOT/license directory.  And on
    // subsequent boots the persisted license is used.
    char *initialLicenseString_ = NULL;
    uint64_t initialLicenseSize_ = 0;
    // License info that is in use.
    char *currentLicenseString_ = NULL;
    uint64_t currentLicenseSize_ = 0;
    uint64_t currentLicenseVersion_ = 0;
    // Prior license info.  Used in error recovery if GVM update fails.
    char *priorLicenseString_ = NULL;
    uint64_t priorLicenseSize_ = 0;
    uint64_t priorLicenseVersion_ = 0;

    void licUpdateLock();
    void licUpdateUnlock();
    MustCheck bool licIsExpiredInt(LicenseData2 *loadedData);
    MustCheck Status parseLicense(const char *uLicenseKey,
                                  LicenseData2 *licenseData);
    MustCheck Status validateLicenseFile(const int version,
                                         char **licenseString,
                                         size_t *licenseSize);

    MustCheck Status createNewLicenseFile(const char *licenseString,
                                          uint64_t licenseSize);
    MustCheck Status publishAndBroadcastLicense(const char *licenseString,
                                                uint64_t licenseSize);
    MustCheck Status readLicenseFile(char *fileNamePath,
                                     char *licenseString,
                                     size_t fileSize);
    MustCheck Status compressLicense(const char *uncompressedLicense,
                                     size_t uncompressedLicenseSize,
                                     char **compressedLicense,
                                     size_t *compressedLicenseSize);
    MustCheck Status uncompressLicense(const char *compressedLicense,
                                       uint64_t compressedLicenseSize,
                                       char **uncompressedLicense,
                                       uint64_t *licenseSize);

    // Disallow
    LicenseMgr() {}   // Use init
    ~LicenseMgr() {}  // Use destroy

    LicenseMgr(const LicenseMgr &) = delete;
    LicenseMgr &operator=(const LicenseMgr &) = delete;
};
#endif  // _LICENSE_H_
