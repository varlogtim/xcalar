// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "gvm/GvmTarget.h"

#ifndef _LIB_DATASET_GVM_H_
#define _LIB_DATASET_GVM_H_

#include "dataset/DatasetTypes.h"

class LibDatasetGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        CreateStruct = 1234,
        Unload,
        Finalize,
    };
    static LibDatasetGvm *get();
    static Status init();
    void destroy();

    Status localHandler(uint32_t action,
                        void *payload,
                        size_t *outputSizeOut) override;
    GvmTarget::Index getGvmIndex() const override;

    struct CreateStructInfo {
        LoadDatasetInput input;
        DsDatasetId datasetId;
    };

  private:
    static LibDatasetGvm *instance;
    LibDatasetGvm() {}
    ~LibDatasetGvm() {}
    LibDatasetGvm(const LibDatasetGvm &) = delete;
    LibDatasetGvm &operator=(const LibDatasetGvm &) = delete;
};

#endif  // _LIB_DATASET_GVM_H_
