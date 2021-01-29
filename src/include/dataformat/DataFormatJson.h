// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMATJSON_H_
#define _DATAFORMATJSON_H_

#include <jansson.h>
#include <google/protobuf/arena.h>

#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "df/DataFormatTypes.h"
#include "datapage/MemPool.h"
#include "datapage/DataPage.h"
#include "df/Importable.h"

class JsonFormatOps final
{
  public:
    static Status init();
    void destroy();
    static JsonFormatOps *get();

    static MustCheck Status
    convertProtoToJson(const ProtoFieldValue *protoValue, json_t **jsonOut);
    static MustCheck Status convertJsonToProto(json_t *jsonValue,
                                               ProtoFieldValue *protoOut);

  private:
    static JsonFormatOps *instance;
    JsonFormatOps(){};
    ~JsonFormatOps(){};

    JsonFormatOps(const JsonFormatOps &) = delete;
    JsonFormatOps &operator=(const JsonFormatOps &) = delete;
};

#endif  // _DATAFORMATJSON_H_
