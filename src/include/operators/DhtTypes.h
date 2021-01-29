// Copyright 2015 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DHTTYPES_H
#define _DHTTYPES_H

#include <time.h>

#include "primitives/Primitives.h"
#include "ns/LibNsTypes.h"

typedef LibNsTypes::NsId DhtId;
typedef LibNsTypes::NsHandle DhtHandle;

enum DhtBroadcast {
    DoBroadcast = true,
    DoNotBroadcast = false,
};
enum DhtAutoRemove {
    DoAutoRemove = true,
    DoNotAutoRemove = false,
};

// This structure is used to pass args by libapis which are then stored in a
// DHT object.
struct DhtArgs {
    float64_t lowerBound;
    float64_t upperBound;
    Ordering ordering;
};

struct Dht {
    DhtId dhtId;
    char dhtName[LibNsTypes::MaxPathNameLen];
    float64_t lowerBound;
    float64_t upperBound;
    Ordering ordering;
    DhtBroadcast broadcast;
};

// This class is used to publish globally the DHT name and associate
// it with a dhtId which can be used to get the DHT from the GVM.
class DhtNsObject final : public NsObject
{
  public:
    static constexpr const char* PathPrefix = "/dhthash/";

    // Public interfaces
    MustCheck DhtId getDhtId() { return dhtId_; }

    // Constructor / Destructor
    DhtNsObject(DhtId dhtId) : NsObject(sizeof(DhtNsObject)), dhtId_(dhtId) {}
    ~DhtNsObject() {}

  private:
    DhtId dhtId_;

    // Disallow
    DhtNsObject(const DhtNsObject&) = delete;
    DhtNsObject(const DhtNsObject&&) = delete;
    DhtNsObject& operator=(const DhtNsObject&) = delete;
    DhtNsObject& operator=(const DhtNsObject&&) = delete;
};

// When the DHT is saved to disk, it uses the DhtExternal structure.  We do
// not embed other structures since that could render the saved version
// unreadable if the other structures change.  If significant modification
// of the DhtExternal struct is needed, save the old version and create the
// new definition with a new version number.
//
// Since DhtExternal is going to be written as a sector on disk that is at
// least 512 bytes, the struct is padded to 512 bytes to allow expansion
// without changing the size of the block read from nodes that do not have
// support to read the new fields.  If the size is changed, update the
// DhtExtBlockSize enum below to keep it in sync.

struct DhtExternal {
    char dhtExtEyec[4];
    uint16_t dhtExtLen;
    uint16_t dhtExtVer;
    float64_t lowerBound;
    float64_t upperBound;
    Ordering ordering;
    char dhtExtExpansionRoom[460];
    struct timespec dhtExtCreateTime;
    char dhtExtEndEyec[4];
    uint32_t dhtExtCrc;
};

// DhtMaxDhtNameLen is set to 244 due to the way DHTs are written to disk.
// The file name is Xcalar.DHT.<dht name>.  Most linux file systems limit
// the file name to 255 characters and the naming convention uses the first
// 11 characters, so 244 characters is the DHT maximum length name.

enum {
    DhtInvalidDhtId = 0,
    DhtExtCurrentVer = 1,
    DhtExtTestVer = 65535,   // Only used for migration test
    DhtExtBlockSize = 512,   // See DhtExternal definition
    DhtMaxDhtNameLen = 244,  // Do not increase this size!
};

// DhtBaseExternal is a struct that was never persisted.  It exists so
// that the migration functional test has something to migrate to the
// current version.
struct DhtBaseExternal {
    char dhtExtEyec[4];
    uint16_t dhtExtLen;
    uint16_t dhtExtVer;
    Ordering ordering;
    float64_t upperBound;
    float64_t lowerBound;
};

#endif  // _DHTTYPES_H
