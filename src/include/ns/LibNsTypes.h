// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBNSTYPES_H_
#define _LIBNSTYPES_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"

class LibNsTypes
{
  public:
    typedef Xid NsId;
    static constexpr size_t MaxPathNameLen = 1023;
    static constexpr Xid NsInvalidId = XidInvalid;
    static constexpr uint64_t NsInvalidRefCnt = -1;

    // Flags specified to open.
    enum NsOpenFlags {
        Invalid = 0,
        Shared = 0x1,
        Excl = 0x2,
        Reader = 0x4,
        Writer = 0x8,
        // Writers have exclusive access
        WriterExcl = (Reader | Writer | Excl),
        // Readers have shared access
        ReaderShared = (Reader | Shared),
        // More relaxed than ReaderShared. Here Readers have shared access and
        // writers have exclusive access for any given time slice.
        ReaderSharedWriterExcl = 0x10,
        ReadSharedWriteExclReader = (ReaderSharedWriterExcl | Reader),
        ReadSharedWriteExclWriter = (ReaderSharedWriterExcl | Writer),
    };

    enum NsAutoRemove {
        DoAutoRemove = true,
        DoNotAutoRemove = false,
    };

    // Name space handle returned by open.
    struct NsHandle {
        NsHandle() : nsId(NsInvalidId), version(0), openFlags(Invalid) {}
        // NsId of the NS object
        NsId nsId;
        // NsId associated with the opening of the object.  It must be the
        // same when closing the object.
        NsId openNsId;
        uint32_t version;
        NsOpenFlags openFlags;
    };

    struct PathInfoOut {
        NsId nsId;
        bool pendingRemoval;
        unsigned long long millisecondsSinceCreation;
        char pathName[MaxPathNameLen];
    };

    static bool isValidNsOpenFlags(NsOpenFlags openFlags)
    {
        if (openFlags == (WriterExcl) || openFlags == (ReaderShared) ||
            openFlags == (ReaderSharedWriterExcl | Reader) ||
            openFlags == (ReaderSharedWriterExcl | Writer)) {
            return true;
        } else {
            return false;
        }
    }

  private:
    // No one should instantiate this class
    LibNsTypes() {}
};

// This is the base class object that is managed by LibNs.  Clients of this
// library will derive from this class and add additional members and methods
// as needed.

// As the object and any derived objects will be transferred to other nodes
// "by value" the object must not contain any virtual methods (to avoid
// vtables), or references or pointers.

class NsObject
{
  public:
    // Constructor - size is needed to send object over the wire
    NsObject(size_t objSize)
    {
        assert(objSize >= sizeof(class NsObject));

        mySize_ = objSize;
        version_ = 1;
    }

    MustCheck size_t getSize() { return mySize_; }

    MustCheck uint32_t getVersion() { return version_; }

  private:
    size_t mySize_;
    uint32_t version_;

    NsObject(const NsObject&) = delete;
    NsObject(const NsObject&&) = delete;
    NsObject& operator=(const NsObject&) = delete;
    NsObject& operator=(const NsObject&&) = delete;
};

#endif  // _LIBNSTYPES_H_
