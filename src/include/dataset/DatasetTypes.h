// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATASETTYPES_H_
#define _DATASETTYPES_H_

#include <semaphore.h>

#include "operators/GenericTypes.h"
#include "primitives/Primitives.h"
#include "xdb/TableTypes.h"
#include "df/DataFormatTypes.h"
#include "util/AtomicTypes.h"
#include "dag/DagTypes.h"
#include "runtime/Semaphore.h"
#include "util/IntHashTable.h"
#include "util/StringHashTable.h"
#include "ns/LibNsTypes.h"
#include "newtupbuf/NewTupleTypes.h"

class DataPageIndex;

typedef Xid XcalarApiDatasetId;
typedef XcalarApiDatasetId DsDatasetId;

#define DsDatasetIdInvalid ((DsDatasetId) 0)

enum {
    XcalarApiDsDatasetNameLen = 255,
};
#define DsDatasetNameLen (XcalarApiDsDatasetNameLen)

#define XcalarApiDsTempDatasetPrefix "dataset-"
#define DsTempDatasetPrefix (XcalarApiDsTempDatasetPrefix)

static constexpr const char *DsDefaultDatasetKeyName = "xcalarRecordNum";

struct DsDatasetMeta {
    DsDatasetId datasetId;
    DfLoadArgs loadArgs;
    char name[XcalarApiDsDatasetNameLen + 1];
    bool loadIsComplete;
    bool isListable;
};

struct XcalarApiBulkLoadInput {
    char datasetName[XcalarApiDsDatasetNameLen + 1];
    DfLoadArgs loadArgs;
    DagTypes::NodeId dagNodeId;
    XdbId dstXdbId;
    bool reuseDatasetIfExists;
};

struct LoadDatasetInput {
    XcalarApiBulkLoadInput bulkLoadInput;
    DagTypes::DagId dagId;
};

// Handle to a dataset which carries with it information related
// to managing the reference to the dataset.  The dataset is GVM'd
// to all the nodes but the references are only kept on a single (dlm)
// node.  The datasetId is hashed to determine the dlm node.
struct DatasetRefHandle {
    DsDatasetId datasetId;
    LibNsTypes::NsHandle nsHandle;
    char userIdName[LOGIN_NAME_MAX + 1];
};

// Used to update dataset reference table on dlm node
enum DsReferenceOp { AddReference, ListReferences, DeleteReference };
struct DsReferenceMsg {
    DsReferenceOp op;
    DsDatasetId datasetId;
    char userIdName[LOGIN_NAME_MAX + 1];
};

struct DsReferenceResponse {
    DsReferenceOp op;
    Status status = StatusOk;
    size_t outputSize = 0;
    void *output = NULL;
};

enum DsLoadStatus : uint32_t {
    DsLoadStatusInvalid,
    DsLoadStatusNotStarted,
    DsLoadStatusInProgress,
    DsLoadStatusComplete,
    DsLoadStatusFailed,
    DsLoadStatusCancelled,
};

struct Xdb;
struct XdbMeta;

// Needs PageSize alignment for Sparse copy.
class __attribute__((aligned(PageSize))) DsDataset
{
  public:
    // XXX this is identical to XcalarApiColumnInfo, but we cannot use that here
    // because it would create a circular dependency between these .h files
    struct ColumnInfo {
        char name[DfMaxFieldNameLen + 1];
        DfFieldType type;
    };

    DsDataset() = default;
    ~DsDataset() = default;

    MustCheck Status init(const LoadDatasetInput *input, DsDatasetId datasetId);

    IntHashTableHook intHook_;
    StringHashTableHook stringHook_;

    DsDatasetId datasetId_;
    DagTypes::DagId dagId_;
    DagTypes::NodeId nodeId_;
    DfRecordId startRecordId_;
    bool downSampled_;
    DataPageIndex *pageIndex_ = NULL;
    DataPageIndex *errorPageIndex_ = NULL;
    char name_[XcalarApiDsDatasetNameLen + 1];
    // transient; updated during load
    Atomic64 numErrors_;
    int64_t finalTotalNumErrors_;
    // The dataset size once it is done loading.  We only have to get this
    // from libNs the first time and then save it here.
    uint64_t datasetSize_;
    DfLoadArgs loadArgs_;
    // Keep track of the column names in the order they came in at dataset
    // ingest time.  We only have to get this from libNs the first time and
    // then save it here.
    unsigned numColumns_ = 0;
    ColumnInfo columns_[TupleMaxNumValuesPerRecord];

    MustCheck DsDatasetId getDatasetId() const;
    void doDelete();
    MustCheck const char *getDatasetName() const;

    // Hash table to keep track of references to the data set.  This is useful
    // when XD wants to see which users prevent a dataset from being deleted.

    struct DsReferenceUserEntry {
        char userName[LOGIN_NAME_MAX + 1];
        uint64_t referenceCount;
        StringHashTableHook hook;
        const char *getUserName() const { return userName; };
        void del() { delete this; }
    };

    static constexpr uint64_t NumReferenceUserSlots = 11;

    typedef StringHashTable<DsReferenceUserEntry,
                            &DsReferenceUserEntry::hook,
                            &DsReferenceUserEntry::getUserName,
                            NumReferenceUserSlots,
                            hashStringFast>
        ReferenceUserTable;

    ReferenceUserTable referenceUserTable_;
    uint64_t userCount_;
    XdbMeta *dstMeta_ = NULL;
    Xdb *dstXdb_ = NULL;

    static MustCheck DsDataset *allocDsDataset();
    static void freeDsDataset(DsDataset *dataset);

  private:
    DsDataset(const DsDataset &) = delete;
    DsDataset &operator=(const DsDataset &) = delete;
};

struct FatptrToJsonBuf {
    FatptrToJsonBuf() : buf(NULL), bytesCopied(0), status(StatusUnknown) {}

    char *buf;
    size_t bytesCopied;
    Status status;
};

#endif  // _DATASETTYPES_H_
