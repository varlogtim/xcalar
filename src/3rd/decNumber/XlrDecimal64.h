#ifndef _XLRDECIMAL_H_
#define _XLRDECIMAL_H_
// We need to define decimal64 separately from the rest of the decNumber
// headers to avoid link failure, induced by the C prototypes and the C linkage
// of their respective modules, in our C++ template code compilation unit that
// might include this.

#include <stdint.h>

// If XLR_DEC_BITS_64 is selected, fieldGetSize and fieldTypeIsFixed can be
// modified to use fixed-length packing
// #define XLR_DEC_BITS_64
#define XLR_DEC_BITS_128

#define DECIMAL64_Bytes  8            /* length                     */
#define DECIMAL64_String 24           /* maximum string length, +1  */

#define DECIMAL128_Bytes  16          /* length                     */
#define DECIMAL128_String 43          /* maximum string length, +1  */

/* Decimal 64-bit type, accessible by bytes                         */
typedef struct _decimal64 {
    uint8_t bytes[DECIMAL64_Bytes];     /* decimal64: 1, 5, 8, 50 bits*/
} decimal64;

typedef union {
    decimal64 dfp;
    uint64_t ieee[1]; // IEEE754-2008 encoded DFP64 word
} XlrDfp64;

/* Decimal 128-bit type, accessible by bytes                        */
typedef struct _decimal128 {
    uint8_t bytes[DECIMAL128_Bytes]; /* decimal128: 1, 5, 12, 110 bits*/
} decimal128;

typedef union {
    decimal128 dfp;
    uint64_t ieee[2]; // IEEE754-2008 encoded DFP128 word lower sig bits
} XlrDfp128;

#if defined(XLR_DEC_BITS_64)
typedef XlrDfp64 XlrDfp;
#define decimalToNumber decimal64ToNumber
#define decimalFromNumber decimal64FromNumber
#define decimalFromString decimal64FromString
#define XLR_DFP_STRLEN (DECIMAL64_String)
#define XLR_DFP_ENCODING (DEC_INIT_DECIMAL64)

#elif defined(XLR_DEC_BITS_128)
typedef XlrDfp128 XlrDfp;
#define decimalToNumber decimal128ToNumber
#define decimalFromNumber decimal128FromNumber
#define decimalFromString decimal128FromString
#define XLR_DFP_STRLEN (DECIMAL128_String)
#define XLR_DFP_ENCODING (DEC_INIT_DECIMAL128)

#else
#error "Unset/unknown DFP bitwidth"
#endif

#ifdef __cplusplus
static_assert(sizeof(decimal64) == sizeof(uint64_t),
        "Invalid DFP64 type size");
static_assert(sizeof(decimal128) == sizeof(__uint128_t),
        "Invalid DFP128 type size");
#endif

#endif // _XLRDECIMAL_H_
