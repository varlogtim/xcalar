// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <type_traits>
#include <stdio.h>

#ifndef BIT_FLAG_ENUMS_H
#define BIT_FLAG_ENUMS_H

template <typename Enum>
struct BitMaskOperators {
    static const bool enable = false;
};

#define enableBmaskOps(x)                \
    template <>                          \
    struct BitMaskOperators<x> {         \
        static const bool enable = true; \
    };

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum>::type
operator|(Enum lhs, Enum rhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    return static_cast<Enum>(static_cast<underlying>(lhs) |
                             static_cast<underlying>(rhs));
}

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum&>::type
operator|=(Enum& lhs, Enum rhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    lhs = static_cast<Enum>(static_cast<underlying>(lhs) |
                            static_cast<underlying>(rhs));
    return lhs;
}

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum>::type operator&(
    Enum lhs, Enum rhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    return static_cast<Enum>(static_cast<underlying>(lhs) &
                             static_cast<underlying>(rhs));
}

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum&>::type
operator&=(Enum& lhs, Enum rhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    lhs = static_cast<Enum>(static_cast<underlying>(lhs) &
                            static_cast<underlying>(rhs));
    return lhs;
}

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum>::type operator~(
    Enum lhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    return static_cast<Enum>(~static_cast<underlying>(lhs));
}

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum>::type
operator^(Enum lhs, Enum rhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    return static_cast<Enum>(static_cast<underlying>(lhs) ^
                             static_cast<underlying>(rhs));
}

template <typename Enum>
typename std::enable_if<BitMaskOperators<Enum>::enable, Enum&>::type
operator^=(Enum& lhs, Enum rhs)
{
    using underlying = typename std::underlying_type<Enum>::type;
    lhs = static_cast<Enum>(static_cast<underlying>(lhs) ^
                            static_cast<underlying>(rhs));
    return lhs;
}

#endif  // BIT_FLAG_ENUMS_H
