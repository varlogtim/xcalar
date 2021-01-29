// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APPLOADER_H_
#define _APPLOADER_H_

#include "dataset/DatasetTypes.h"
#include "runtime/Schedulable.h"
#include "ns/LibNsTypes.h"
#include "bc/BufferCache.h"
#include "datapage/DataPageIndex.h"
#include "xcalar/compute/localtypes/SchemaLoad.pb.h"
#include "xcalar/compute/localtypes/Connectors.pb.h"
#include "util/Base64.h"

class App;
class AppInstance;
struct XcalarApiPreviewInput;
struct XcalarApiOutput;
struct XcalarApiBulkLoadOutput;
struct OpStatus;
struct XcalarApiUserId;
struct json_t;
struct XcalarApiUdfContainer;

class AppLoader final
{
  public:
    AppLoader();
    Status init(DsDatasetId datasetId, DatasetRefHandle *dsRefHandle);
    ~AppLoader() = default;

    // Globally run the load app
    MustCheck Status loadDataset(const DfLoadArgs *loadArgs,
                                 const XcalarApiUdfContainer *udfContainer,
                                 OpStatus *opStatus,
                                 XcalarApiBulkLoadOutput *loadOutput,
                                 bool *retAppInternalError);

    static MustCheck Status addDataPage(AppInstance *parentInstance,
                                        DsDatasetId datasetId,
                                        int32_t numFiles,
                                        int32_t numErrors,
                                        bool errorPage,
                                        uint8_t *page,
                                        bool fixedSchemaPage);

    static MustCheck Status listFiles(
        const xcalar::compute::localtypes::Connectors::ListFilesRequest
            *request,
        xcalar::compute::localtypes::Connectors::ListFilesResponse *response);

    static MustCheck Status preview(const XcalarApiPreviewInput *previewInput,
                                    const XcalarApiUserId *user,
                                    XcalarApiOutput **outputOut,
                                    size_t *outputSize);

    static MustCheck Status updateProgress(DsDatasetId datasetId,
                                           int64_t incremental);
    static void setTotalProgress(DsDatasetId datasetId,
                                 int64_t totalWork,
                                 bool downSampled);
    static MustCheck Status
    removeFile(const xcalar::compute::localtypes::Connectors::RemoveFileRequest
                   *removeRequest,
               const XcalarApiUserId *user);

    static MustCheck Status schemaLoad(
        const xcalar::compute::localtypes::SchemaLoad::AppRequest *request,
        xcalar::compute::localtypes::SchemaLoad::AppResponse *response);

  private:
    static constexpr const char *ModuleName = "libds";
    static constexpr const uint64_t AppWaitTimeoutUsecs = 100000;

    AppLoader(const AppLoader &) = delete;
    AppLoader &operator=(const AppLoader &) = delete;

    // This serializes the subset of the bulkLoadInput struct necessary
    // for the load app to do load input. Note that there is a tight contract
    // between this function and the load.py app.
    // Caller is responsible for memFreeing outStr
    static MustCheck Status jsonifyLoadInput(const DfLoadArgs *loadArgs,
                                             const char *udfPath,
                                             const char *udfSource,
                                             char **outStr);

    static MustCheck Status
    parseLoadResults(const char *appOut,
                     const char *appErr,
                     unsigned clusterNumNodes,
                     XcalarApiBulkLoadOutput *loadOutput);

    static MustCheck Status parseLoadOutStr(const char *appOutBlob,
                                            unsigned clusterNumNodes,
                                            size_t *numBuffers,
                                            size_t *numBytes,
                                            size_t *numFiles);

    static MustCheck Status parseLoadErrStr(const char *appErrBlob,
                                            unsigned clusterNumNodes,
                                            char *errorStringBuf,
                                            size_t errorStringBufSize,
                                            char *errorFileBuf,
                                            size_t errorFileBufSize);

    static json_t *jsonifySourceArgs(const DataSourceArgs *sourceArgs);

    static MustCheck Status getUdfOnDiskPath(const char *udfLibNsPath,
                                             char **udfOnDiskPath);

    static MustCheck Status getUdfSrc(const char *udfModuleName,
                                      XcalarApiUdfContainer *udfContainer,
                                      char **udfSource);

    MustCheck Status runLoadApp(App *loadApp,
                                const XcalarApiUserId *user,
                                OpStatus *opStatus,
                                const char *inObj,
                                XcalarApiBulkLoadOutput *loadOutput,
                                bool *retAppInternalError);

    DsDatasetId datasetId_;
    DsDataset *dataset_;
    DatasetRefHandle dsRefHandle_;
};

struct BufferResult {
    size_t numBytes;
    int64_t numFiles;
    Status status;
};

class PageLoader final : public Schedulable
{
  public:
    PageLoader(AppInstance *parentInstance);
    // Takes ownership of reservation
    void init(DsDataset *dataset,
              DataPageIndex::RangeReservation *reservation,
              int32_t numFiles,
              bool errorPage,
              uint8_t *page,
              size_t pageSize,
              bool fixedSchemaPage);
    ~PageLoader()
    {
        if (reservation_) {
            delete reservation_;
            reservation_ = NULL;
        }
    }

    void run() override;
    void done() override;

  private:
    static constexpr const char *ModuleName = "libds";

    MustCheck Status loadOnePage(uint8_t *page, int32_t pageSize);

    DataPageIndex::RangeReservation *reservation_ = NULL;
    AppInstance *parentInstance_;
    DsDataset *dataset_;
    int32_t numFiles_;
    bool errorPage_;
    uint8_t *page_;
    int64_t pageSize_;
    bool fixedSchemaPage_;
};

#endif  // _APPLOADER_H_
