// Copyright 2015, 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonRefCounted.h: Reference counting primitives used by the cluster
//                  monitor

#ifndef _MONREFCOUNT_H_
#define _MONREFCOUNT_H_

#include "monitor/MonLogMsg.h"

template <class T>
class Ref
{
  public:
    Ref() { _ptr = NULL; }
    Ref(T *p)
    {
        _ptr = p;
        _ptr->IncRefCount();
    }
    Ref(T &p)
    {
        _ptr = p._ptr;
        _ptr->IncRefCount();
    }
    Ref(const Ref<T> &ref)
    {
        if (ref._ptr != NULL) {
            _ptr = ref._ptr;
            _ptr->IncRefCount();
        } else {
            _ptr = NULL;
        }
    }
    ~Ref()
    {
        if (_ptr != NULL && _ptr->DecRefCount()) {
            delete _ptr;
        }
    }

    void operator=(T *p) { Assign(p); }

    void operator=(Ref<T> &ref) { Assign(ref._ptr); }

    T *operator->() const { return _ptr; }

    operator T *() const { return _ptr; }

    T *GetPtr() { return _ptr; }

  private:
    void Assign(T *p)
    {
        if (_ptr != NULL) {
            if (_ptr->DecRefCount()) {
                delete _ptr;
            }
            _ptr = NULL;
        }
        if (p != NULL) {
            _ptr = p;
            _ptr->IncRefCount();
        }
    }

    T *_ptr;
};

template <class T>
inline bool
operator==(const Ref<T> &r1, const Ref<T> &r2)
{
    return *r1 == *r2;
}

class RefCounted
{
  public:
    RefCounted() { _refCount = 0; }
    virtual ~RefCounted() {}
    void IncRefCount() { _refCount++; }

    bool DecRefCount()
    {
        _refCount--;
        return _refCount == 0;
    }
    uint32_t GetRefCount() { return _refCount; }

  protected:
    uint32_t _refCount;
};

#endif
