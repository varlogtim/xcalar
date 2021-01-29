// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SYS_CHECK_H_
#define _SYS_CHECK_H_

#include <ostream>
#include <sstream>

#include <primitives/Primitives.h>

// Define an operator for printing the "nullptr" value. We can remove this
// definition once we have enabled C++17.
inline std::ostream &
operator<<(std::ostream &s, std::nullptr_t)
{
    return s << "nullptr";
}

// Internal infra for getting the assertion macros working. Feel free to skip
// all the way to the end of this file, where the macros are actually defined.
namespace assertions
{
// This type is used to explicitly ignore values in the conditional
// logging macros.  This avoids compiler warnings like "value computed
// is not used" and "statement has no effect".
struct LogMessageVoidify {
    LogMessageVoidify() = default;

    // This has to be an operator with a precedence lower than << but
    // higher than ?:
    void operator&(std::ostream &) {}
};

// Minimal std::streambuf specialization that uses the given flat buf. This
// class leaves two bytes at the end of the buffer to allow for a '\n' and '\0'.
class LogStreamBuf : public std::streambuf
{
  public:
    // REQUIREMENTS: "len" must be >= 2 to account for the '\n' and '\0'.
    LogStreamBuf(char *buf, int len) { setp(buf, buf + len - 2); }

    // Legacy public ostrstream method.
    size_t pcount() const { return pptr() - pbase(); }
    char *pbase() const { return std::streambuf::pbase(); }
};

// LogStream is a std::ostream specialization backed by LogStreamBuf.
class LogStream : public std::ostream
{
  public:
    LogStream(char *buf, int len) : std::ostream(nullptr), streambuf_(buf, len)
    {
        rdbuf(&streambuf_);
    }

    LogStream(const LogStream &) = delete;
    LogStream &operator=(const LogStream &) = delete;

    // Legacy std::streambuf methods.
    size_t pcount() const { return streambuf_.pcount(); }
    char *pbase() const { return streambuf_.pbase(); }
    char *str() const { return pbase(); }

  private:
    LogStreamBuf streambuf_;
};

// Internal helper class to create and print the final log message generated
// when an assertion fails.
class LogMessage
{
  public:
    LogMessage(const char *file, int line);
    ~LogMessage() __attribute__((__noreturn__));

    std::ostream &stream();

    LogMessage(const LogMessage &) = delete;
    void operator=(const LogMessage &) = delete;

    static const size_t defaultMessageSize_ = 1024 * 4;

  private:
    const char *file_;
    const int line_;
    LogStream *stream_;
    std::string data_;
};

// A helper class for formatting "expr (V1 vs. V2)" in a CHECK_XX statement.
// Creates a std::stringstream instance and dumps the text there.
class CheckOpMessageBuilder final
{
  public:
    // Inserts "exprtext" and " (" to the stream.
    explicit CheckOpMessageBuilder(const char *exprtext);

    ~CheckOpMessageBuilder();

    // These insert the first and second variables that participated in the
    // failing CHECK() invocation.
    std::ostream *ForVar1();
    std::ostream *ForVar2();

    // Get the result (inserts the closing ")").
    std::string *NewString();

  private:
    std::ostringstream *stream_;
};

// Takes the two values that participated in the failing CHECK, prints them
// into a C++ output stream and returns a string with the resulting data.
template <typename T1, typename T2>
inline std::string *
MakeCheckOpString(const T1 &v1, const T2 &v2, const char *exprtext)
{
    CheckOpMessageBuilder comb(exprtext);
    *comb.ForVar1() << v1;
    *comb.ForVar2() << v2;
    return comb.NewString();
}

// A container for a string pointer which can be evaluated to a bool -
// true IFF the pointer is NULL.
struct CheckOpString {
    CheckOpString(std::string *str) : str_(str) {}

    operator bool() const { return unlikely(str_ != NULL); }

    // No destructor: if str_ is non-NULL, we're about to LOG(FATAL),
    // so there's no point in cleaning up 'str_'.
    std::string *str_;
};

// Helper functions for CHECK_OP macro.
// The (int, int) specialization works around the issue that the compiler
// will not instantiate the template version of the function on values of
// unnamed enum type.
#define DEFINE_CHECK_OP_IMPL(name, op)                                   \
    template <typename T1, typename T2>                                  \
    inline std::string *name##Impl(const T1 &v1,                         \
                                   const T2 &v2,                         \
                                   const char *exprtext)                 \
    {                                                                    \
        if (likely(v1 op v2)) return NULL;                               \
        return MakeCheckOpString(v1, v2, exprtext);                      \
    }                                                                    \
    inline std::string *name##Impl(int v1, int v2, const char *exprtext) \
    {                                                                    \
        return name##Impl<int, int>(v1, v2, exprtext);                   \
    }

DEFINE_CHECK_OP_IMPL(Check_EQ, ==)
DEFINE_CHECK_OP_IMPL(Check_LT, <)
DEFINE_CHECK_OP_IMPL(Check_LE, <=)
DEFINE_CHECK_OP_IMPL(Check_GT, >)
DEFINE_CHECK_OP_IMPL(Check_GE, >=)
#undef DEFINE_CHECK_OP_IMPL

}  // namespace assertions

// A stronger variant of verify() that imposes condition checking in every
// build. It is useful for cases where execution cannot continue as that would
// just crash (or corrupt data) further down the line.
//
// It is possible to log additional data:
//
//  CHECK(x.y.lookup(z)) << "Missing z: " << z;
//
#define CHECK(condition)                                                      \
    static_cast<void>(0),                                                     \
        (condition) ? (void) 0                                                \
                    : assertions::LogMessageVoidify() &                       \
                          assertions::LogMessage(__FILE__, __LINE__).stream() \
                              << "Check failed: " #condition << " "

#if defined(ENABLE_ASSERTIONS)
#define DCHECK(expr) CHECK(expr)
#else
#define DCHECK(expr) \
    while (false) CHECK(expr)
#endif

// The base for all relational op CHECKs. We use CheckOpString to hint to
// compiler that the while condition is unlikely.
#define CHECK_OP_LOG(name, op, val1, val2)                             \
    while (assertions::CheckOpString _result =                         \
               assertions::Check##name##Impl(val1,                     \
                                             val2,                     \
                                             #val1 " " #op " " #val2)) \
    assertions::LogMessage(__FILE__, __LINE__).stream() << *_result.str_

#define CHECK_EQ(val1, val2) CHECK_OP_LOG(_EQ, ==, val1, val2)
#define CHECK_LT(val1, val2) CHECK_OP_LOG(_LT, <, val1, val2)
#define CHECK_LE(val1, val2) CHECK_OP_LOG(_LE, <=, val1, val2)
#define CHECK_GT(val1, val2) CHECK_OP_LOG(_GT, >, val1, val2)
#define CHECK_GE(val1, val2) CHECK_OP_LOG(_GE, >=, val1, val2)

#if defined(ENABLE_ASSERTIONS)
#define DCHECK_EQ(val1, val2) CHECK_OP_LOG(_EQ, ==, val1, val2)
#define DCHECK_LT(val1, val2) CHECK_OP_LOG(_LT, <, val1, val2)
#define DCHECK_LE(val1, val2) CHECK_OP_LOG(_LE, <=, val1, val2)
#define DCHECK_GT(val1, val2) CHECK_OP_LOG(_GT, >, val1, val2)
#define DCHECK_GE(val1, val2) CHECK_OP_LOG(_GE, >=, val1, val2)
#else
#define DCHECK_EQ(val1, val2) \
    while (false) CHECK_OP_LOG(_EQ, ==, val1, val2)
#define DCHECK_LT(val1, val2) \
    while (false) CHECK_OP_LOG(_LT, <, val1, val2)
#define DCHECK_LE(val1, val2) \
    while (false) CHECK_OP_LOG(_LE, <=, val1, val2)
#define DCHECK_GT(val1, val2) \
    while (false) CHECK_OP_LOG(_GT, >, val1, val2)
#define DCHECK_GE(val1, val2) \
    while (false) CHECK_OP_LOG(_GE, >=, val1, val2)
#endif

#endif  // _SYS_CHECK_H_
