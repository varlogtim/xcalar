// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DFPUTILS_H_
#define _DFPUTILS_H_

#include <stdint.h>

#include "primitives/Primitives.h"

#include "XlrDecimal64.h"

struct _decContext;
typedef _decContext decContext;

class DFPUtils final
{
  public:
    // https://people.eecs.berkeley.edu/~wkahan/ieee754status/IEEE754.PDF
    static constexpr size_t ieeeDblStrChars = 25;

    static MustCheck Status init();
    static DFPUtils *get();

    int32_t xlrDfp64Cmp(const XlrDfp *d64Num1, const XlrDfp *d64Num2);
    int32_t xlrDfpNumericToInt32(const XlrDfp *src);
    uint32_t xlrDfpNumericToUInt32(const XlrDfp *src);
    Status xlrDfpInt64ToNumeric(XlrDfp *dst, int64_t src);
    Status xlrDfpUInt64ToNumeric(XlrDfp *dst, uint64_t src);
    MustCheck Status xlrNumericFromString(XlrDfp *dst, const char *src);
    void xlrNumericToString(char *dst, const XlrDfp *src);
    void xlrNumericToStringInternal(char *dst,
                                    const XlrDfp *src,
                                    bool rescale = true);
    void xlrDfpRound(XlrDfp *dst, const XlrDfp *src, int32_t dp);
    void xlrDfpAbs(XlrDfp *dst, const XlrDfp *src);
    void xlrDfpZero(XlrDfp *dst);
    void xlrDfpNan(XlrDfp *dst);
    void xlrDfpAdd(XlrDfp *dstAcc, const XlrDfp *srcAcc);
    void xlrDfpAdd(XlrDfp *dst, const XlrDfp *addend0, const XlrDfp *addend1);
    void xlrDfpSub(XlrDfp *dst,
                   const XlrDfp *minuend,
                   const XlrDfp *subtrahend);
    void xlrDfpMulti(XlrDfp *dst, const XlrDfp *lhs, const XlrDfp *rhs);
    void xlrDfpDiv(XlrDfp *dst, const XlrDfp *dividend, const XlrDfp *divisor);
    void xlrDfpSubMulti(XlrDfp *dst, XlrDfp *srcs, size_t numVals);
    void xlrDfpAddMulti(XlrDfp *dst, XlrDfp *srcs, size_t numVals);
    void xlrDfpMultiMulti(XlrDfp *dst, XlrDfp *srcs, size_t numVals);
    void xlrDfpUInt32ToNumeric(XlrDfp *dst, uint32_t src);
    void xlrDfpInt32ToNumeric(XlrDfp *dst, int32_t src);
    void xlrDfpStringToNumeric(XlrDfp *dst, const char *src);
    bool xlrDfpIsZero(const XlrDfp *src);
    bool xlrDfpIsNan(const XlrDfp *src);
    bool xlrDfpIsInf(const XlrDfp *src);
    Status xlrDfpNumericToFloat64(float64_t *dst, const XlrDfp *src);
    Status xlrDfpFloat64ToNumeric(XlrDfp *dst, const float64_t src);

    void destroy();

  private:
    const int32_t ieeeEncoding;

    DFPUtils();
    DFPUtils(const DFPUtils &) = delete;
    DFPUtils &operator=(const DFPUtils &) = delete;
    ~DFPUtils() = default;

    void xlrDfpContextDefault(decContext *context, int32_t kind);

    static DFPUtils *instance;

    MustCheck MustInline int32_t getScaleDigits();
};

#endif  // _DFPUTILS_H_
