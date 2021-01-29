// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <iostream>

#include "sys/Check.h"
#include "sys/XLog.h"

namespace assertions
{
//
// LogMessage implementation.
//
LogMessage::LogMessage(const char* file, int line) : file_(file), line_(line)
{
    data_.resize(defaultMessageSize_, '\0');
    stream_ = new LogStream(&data_[0], data_.size());
    stream() << file_ << ":" << line_ << "] ";
}

LogMessage::~LogMessage()
{
    data_.resize(stream_->pcount());

    // Step 1: push the assertion message into the log and flush it.
    xSyslog("fatal", XlogErr, "%s", data_.c_str());
    xSyslogFlush();

    // Step 2: mirror the message to stderr.
    std::cerr << data_ << std::endl << std::flush;

    // Step 3: crash.
    abort();
}

std::ostream&
LogMessage::stream()
{
    assert(stream_ != nullptr);
    return *stream_;
}

//
// CheckOpMessageBuilder implementation.
//
CheckOpMessageBuilder::CheckOpMessageBuilder(const char* exprtext)
    : stream_(new std::ostringstream)
{
    *stream_ << exprtext << " (";
}

CheckOpMessageBuilder::~CheckOpMessageBuilder()
{
    delete stream_;
}

std::ostream*
CheckOpMessageBuilder::ForVar1()
{
    return stream_;
}

std::ostream*
CheckOpMessageBuilder::ForVar2()
{
    *stream_ << " vs. ";
    return stream_;
}

std::string*
CheckOpMessageBuilder::NewString()
{
    *stream_ << ")";
    return new std::string(stream_->str());
}

}  // namespace assertions
