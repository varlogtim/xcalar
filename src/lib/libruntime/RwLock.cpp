// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "runtime/RwLock.h"
#include "Thread.h"
#include "util/System.h"

RwLock::RwLock() {}

RwLock::~RwLock() {}

// There is writer starvation here in this implementation.
void
RwLock::lock(RwLock::Type type)
{
    lock_.lock();
    while (true) {
        if (type == Type::Reader) {
            if (track_ >= 0) {
                // Track readers
                track_++;
                break;
            } else {
                // Outstanding writer here.
                cv_.wait(&lock_);
                continue;
            }
        } else {
            assert(type == Type::Writer);
            if (track_ == 0) {
                // Track writer
                track_ = -1;
                break;
            } else {
                // Outstanding readers/writer here.
                cv_.wait(&lock_);
                continue;
            }
        }
    }
    lock_.unlock();
}

void
RwLock::unlock(RwLock::Type type)
{
    int curTrack;

    lock_.lock();
    if (type == Type::Reader) {
        assert(track_ > 0);
        track_--;
    } else {
        assert(type == Type::Writer);
        assert(track_ == -1);
        track_ = 0;
    }
    curTrack = track_;
    if (!curTrack) {
        // Wake up candidates
        cv_.broadcast();
    }
    lock_.unlock();
}

bool
RwLock::tryLock(RwLock::Type type)
{
    bool ret;

    lock_.lock();
    if (type == Type::Reader) {
        if (track_ >= 0) {
            // Track readers
            track_++;
            ret = true;
        } else {
            ret = false;
        }
    } else {
        assert(type == Type::Writer);
        if (track_ == 0) {
            // Track writer
            track_ = -1;
            ret = true;
        } else {
            ret = false;
        }
    }
    lock_.unlock();

    return ret;
}
