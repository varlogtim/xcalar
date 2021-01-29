// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _ACCESSOR_H_
#define _ACCESSOR_H_

struct AccessorName {
    AccessorName() = default;
    ~AccessorName()
    {
        if (this->type == Type::Field && this->value.field) {
            memFree(this->value.field);
            this->value.field = NULL;
        }
    }
    enum Type {
        Invalid,
        Field,
        Subscript,
    };
    union Value {
        char *field = NULL;
        int subscript;
    };

    Type type = Type::Invalid;
    Value value;
};

struct Accessor {
    Accessor() = default;
    ~Accessor()
    {
        if (this->names) {
            delete[] this->names;
            this->names = NULL;
        }
    }

    int nameDepth = -1;
    AccessorName *names = NULL;
};

class AccessorNameParser
{
  public:
    AccessorNameParser() = default;
    ~AccessorNameParser()
    {
        if (names_) {
            delete[] names_;
            names_ = NULL;
        }
    }
    MustCheck Status parseAccessor(const char *name, Accessor *acessorOut);

  private:
    static const constexpr char EscapeChar = '\\';

    enum ParserState {
        Invalid,
        Normal,
        Escaping,
        InSubscript,
        FinishedSubscript,
    };

    MustCheck Status parsePass(bool dryRun);
    MustCheck Status addFieldAccessor(bool dryRun);
    MustCheck Status addSubscriptAccessor(bool dryRun);

    // General parser state
    const char *inName_ = NULL;
    int nameLen_ = -1;
    char tmpBuf_[DfMaxFieldNameLen + 1];
    int curLen_ = -1;
    ParserState state_ = ParserState::Invalid;

    // State when building real accessor name array
    int curNumNames_ = -1;
    AccessorName *names_ = NULL;
};

#endif  // _ACCESSOR_H_
