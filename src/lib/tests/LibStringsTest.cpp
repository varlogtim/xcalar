// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>
#include <assert.h>

#include "primitives/Primitives.h"
#include "strings/String.h"
#include "test/QA.h"

static Status strMatchTests();
static Status asciiHexTests();
static Status safeBasenameTests();

static TestCase testCases[] = {
    {"Ascii-Hex tests", asciiHexTests, TestCaseEnable, ""},
    {"strMatch Tests", strMatchTests, TestCaseEnable, ""},
    {"safeBasename Tests", safeBasenameTests, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask = TestCaseOptDisableIsPass;

static Status
asciiHexTests()
{
    bool ret;
    uint8_t val;
    const char *tests[] = {"    ltrim", "rtrim    ", "  lrtrim "};

    const char *answers[] = {"ltrim", "rtrim", "lrtrim"};

    char rawLine[255];
    char *line;

    unsigned i;

    for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        strcpy(rawLine, tests[i]);
        line = strTrim(rawLine);
        assert(strcmp(line, answers[i]) == 0);
    }

    ret = strIsAsciiHexDigit('a');
    assert(ret == true);

    ret = strIsAsciiHexDigit('g');
    assert(ret == false);

    val = strAsciiToHexDigit('a');
    assert(val == 0xa);

    return StatusOk;
}

static Status
strMatchTests()
{
    // Test *
    assert(strMatch("*", "abc"));
    assert(strMatch("*", ""));
    assert(strMatch("*", "49583!@$^($^%&#&"));
    assert(strMatch("*", "\"Hel216t34t@^$%"));
    assert(strMatch("*", "\n"));
    assert(strMatch("*", "*"));

    // Test ""
    assert(strMatch("", ""));
    assert(!strMatch("", "Hello"));
    assert(!strMatch("", "\n"));
    assert(!strMatch("", "$#^@#21"));
    assert(!strMatch("", "\t\t"));
    assert(!strMatch("", "*"));

    // Test * at front
    assert(strMatch("*a", "\ta"));
    assert(strMatch("*a", "\n\ta"));
    assert(strMatch("*a", "$@#%^a"));
    assert(strMatch("*a", "2352a"));
    assert(strMatch("*a", "a"));
    assert(strMatch("*a", "baa"));
    assert(strMatch("*a", "aaaaa"));
    assert(strMatch("*a", "*a"));
    assert(!strMatch("*a", "ab"));
    assert(!strMatch("*a", "a\n"));
    assert(!strMatch("*a", "bab"));
    assert(!strMatch("*a", "\ta\t"));
    assert(!strMatch("*a", "2 52@^$@a "));
    assert(!strMatch("*a", " a "));
    assert(!strMatch("*a", "a "));
    assert(!strMatch("*a", "a*"));
    assert(!strMatch("*a", ""));

    // Test * in middle
    assert(strMatch("$*\t", "$gew @$^@ 253\n\n\t"));
    assert(strMatch("$*\t", "$\t"));
    assert(strMatch("$*\t", "$*\t"));
    assert(strMatch("$*\t", "$$**\t\t"));
    assert(!strMatch("$*\t", ""));
    assert(!strMatch("$*\t", "*"));
    assert(!strMatch("$*\t", "a935 602%$"));
    assert(!strMatch("$*\t", "$$$$"));
    assert(!strMatch("$*\t", "\t\t\t\t"));
    assert(!strMatch("$*\t", " $*\t"));
    assert(!strMatch("$*\t", "$*\t\n"));

    // Test * at the end
    assert(strMatch("$1\t*", "$1\t"));
    assert(strMatch("$1\t*", "$1\t\t"));
    assert(strMatch("$1\t*", "$1\t*****"));
    assert(strMatch("$1\t*", "$1\tQ$^3 6@$%@!"));
    assert(!strMatch("$1\t*", ""));
    assert(!strMatch("$1\t*", " $1\t"));
    assert(!strMatch("$1\t*", "$1$1\t"));
    assert(!strMatch("$1\t*", "*****"));
    assert(!strMatch("$1\t*", "*"));
    assert(!strMatch("$1\t*", "hello"));
    assert(!strMatch("$1\t*", "1\t"));

    // Test multiple *
    assert(strMatch("*$1*\t", "$$1\t"));
    assert(strMatch("*$1*\t", "$1\t"));
    assert(strMatch("*$1*\t", "$$11\t\t"));
    assert(strMatch("*$1*\t", "$1*\t"));
    assert(strMatch("*$1*\t", "*$1*\t"));
    assert(!strMatch("*$1*\t", ""));
    assert(!strMatch("*$1*\t", "$1"));
    assert(!strMatch("*$1*\t", "$1\t "));
    assert(!strMatch("*$1*\t", " $2*\t"));
    assert(!strMatch("*$1*\t", "$*1*\t"));
    assert(strMatch("$**\t", "$gew @$^@ 253\n\n\t"));
    assert(strMatch("$**\t", "$\t"));
    assert(strMatch("$**\t", "$*\t"));
    assert(strMatch("$**\t", "$$**\t\t"));
    assert(!strMatch("$**\t", ""));
    assert(!strMatch("$**\t", "*"));
    assert(!strMatch("$**\t", "a935 602%$"));
    assert(!strMatch("$**\t", "$$$$"));
    assert(!strMatch("$**\t", "\t\t\t\t"));
    assert(!strMatch("$**\t", " $*\t"));
    assert(!strMatch("$**\t", "$*\t\n"));

    return StatusOk;
}

static Status
safeBasenameTests()
{
    assert(strcmp("file.txt", strBasename("file.txt")) == 0);
    assert(strcmp("file.txt", strBasename("/file.txt")) == 0);
    assert(strcmp("file.txt", strBasename("b/file.txt")) == 0);
    assert(strcmp("file.txt", strBasename("/b/file.txt")) == 0);
    assert(strcmp("file.txt", strBasename("/home/mynames/b/file.txt")) == 0);
    assert(strcmp("", strBasename("/")) == 0);
    assert(strcmp("", strBasename("//")) == 0);
    assert(strcmp("", strBasename("///")) == 0);
    assert(strcmp("", strBasename("")) == 0);
    assert(strcmp("myfile",
                  strBasename(
                      "123!@#$%^&*()-_=+\\|[]{};:'\"`~,<.>?`/myfile")) == 0);
    assert(strcmp("", strBasename("/b/myfile.txt/")) == 0);
    return StatusOk;
}

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    numTestsFailed =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);
    return numTestsFailed;
}
