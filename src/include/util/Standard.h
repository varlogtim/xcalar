// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef UTIL_STANDARD_H
#define UTIL_STANDARD_H

#include <memory>

// TODO(Oleg): remove this copy-pasted code once we've enabled C++14.
namespace std
{
template <typename T, typename... Args>
unique_ptr<T>
make_unique(Args &&... args)
{
    return unique_ptr<T>(new T(forward<Args>(args)...));
}

}  // namespace std

#endif  // UTIL_STANDARD_H
