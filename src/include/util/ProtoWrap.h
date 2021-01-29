// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PROTO_WRAP_H_
#define _PROTO_WRAP_H_

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include <google/protobuf/arena.h>
#include <google/protobuf/io/coded_stream.h>

template <typename T>
MustCheck Status
pbSerializeToArray(T *pb, void *dst, size_t size)
{
    Status status = StatusOk;

    try {
        if (!pb->SerializeToArray(dst, size)) {
            status = StatusProtobufEncodeError;
        }
    } catch (std::bad_alloc) {
        status = StatusNoMem;
    } catch (std::exception &e) {
        status = StatusProtobufEncodeError;
    }

    return status;
}

template <typename T>
MustCheck Status
pbParseFromArray(T *pb, const void *src, size_t size)
{
    Status status = StatusOk;

    google::protobuf::io::CodedInputStream coded_fs((uint8_t *) src, size);
    coded_fs.SetTotalBytesLimit(INT_MAX);

    try {
        if (!pb->ParseFromCodedStream(&coded_fs)) {
            status = StatusProtobufDecodeError;
        }
    } catch (std::bad_alloc) {
        status = StatusNoMem;
    } catch (std::exception &e) {
        status = StatusProtobufDecodeError;
    }

    return status;
}

template <typename T>
MustCheck Status
pbCreateMessage(google::protobuf::Arena *arena, T **pb)
{
    Status status = StatusOk;

    try {
        *pb = google::protobuf::Arena::CreateMessage<T>(arena);
        if (*pb == NULL) {
            status = StatusProtobufError;
        }
    } catch (std::bad_alloc) {
        status = StatusNoMem;
    } catch (std::exception &e) {
        status = StatusProtobufError;
    }

    return status;
}

static void *
pbArenaAlloc(size_t size)
{
    void *ptr = memAlloc(size);

    if (ptr == NULL) {
        // protobuf arena alloc does not check for NULL return from malloc, so
        // throw here to bubble the exception up and eventually convert to a
        // Status return.
        throw std::bad_alloc();
    }

    return ptr;
}

static void
pbArenaDealloc(void *ptr, size_t size)
{
    memFree(ptr);
}

class XceArenaOpt : public google::protobuf::ArenaOptions
{
  public:
    // Work around C++11 screwyness with member initializers
    XceArenaOpt()
    {
        block_alloc = pbArenaAlloc;
        block_dealloc = pbArenaDealloc;
    };
};

static XceArenaOpt xceArenaOpt;

#endif  // _PROTO_WRAP_H_
