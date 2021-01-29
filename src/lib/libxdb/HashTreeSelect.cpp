// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "xdb/HashTree.h"
#include "operators/OperatorsXdbPageOps.h"
#include "operators/OperatorsEvalTypes.h"
#include "queryparser/QueryParser.h"
#include "xdb/Xdb.h"
#include "xdb/XdbInt.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"
#include "msg/Xid.h"

static constexpr char moduleName[10] = "HashTree";

struct SelectOpContext {
    const char *filterString = NULL;

    unsigned numMaps = 0;
    const char *mapStrings[TupleMaxNumValuesPerRecord];
    const char *mapFields[TupleMaxNumValuesPerRecord];

    // these fields are used to perform group evaluations.
    // Each slot of the scatchXdb is fully populated before it undergoes
    // groupEvals determined by grpCtxs.
    // The accumulated results are put into dstXdb
    unsigned numGroupEvals = 0;
    GroupEvalContext grpCtxs[TupleMaxNumValuesPerRecord];

    unsigned numGroupKeys = 0;
    const char *groupKeys[TupleMaxNumValuesPerRecord];

    XdbId scratchXdbId = XidInvalid;
    Xdb *scratchXdb = NULL;

    // used to convert from scratchXdb to dstXdb
    OpKvEntryCopyMapping *scratchMapping = NULL;

    // original source augmented with maps and join cols
    const char *srcAugCols[TupleMaxNumValuesPerRecord];
    NewTupleMeta srcAugMeta;

    XcalarApiRenameMap *joinCols = NULL;
    unsigned numJoinCols;
    unsigned numSrcWithJoinCols;
    NewTupleMeta srcWithJoinMeta;

    json_t *evalInputJson = NULL;

    ~SelectOpContext()
    {
        if (this->scratchXdb) {
            XdbMgr::get()->xdbDropLocalInternal(scratchXdbId);
        }

        if (evalInputJson) {
            json_decref(evalInputJson);
            evalInputJson = NULL;
        }

        if (scratchMapping) {
            memFree(scratchMapping);
            scratchMapping = NULL;
        }

        if (joinCols) {
            delete[] joinCols;
        }
    }

    Status setup(LibHashTreeGvm::GvmSelectInput *input,
                 XdbMeta *srcMeta,
                 XdbMeta *dstMeta)
    {
        Status status = StatusOk;
        json_t *val;
        int ret;
        json_error_t err;
        unsigned ii;
        unsigned srcNumFields =
            srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
        unsigned dstNumFields =
            dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
        json_t *maps = NULL, *groupBys = NULL, *groupByKeys = NULL,
               *join = NULL;

        evalInputJson = json_loads((char *) input->evalInput, 0, &err);

        if (evalInputJson) {
            maps = json_object_get(evalInputJson, QpSelect::MapsKey);
            groupBys = json_object_get(evalInputJson, QpSelect::GroupBysKey);
            groupByKeys =
                json_object_get(evalInputJson, QpSelect::GroupByKeysKey);
            join = json_object_get(evalInputJson, QpSelect::JoinKey);
        }

        filterString = input->filterString;

        if (maps) {
            numMaps = json_array_size(maps);
        }

        if (groupBys) {
            numGroupEvals = json_array_size(groupBys);
        }

        if (groupByKeys) {
            numGroupKeys = json_array_size(groupByKeys);
        }

        if (join) {
            json_t *cols = json_object_get(join, QpSelect::ColumnsKey);
            numJoinCols = json_array_size(cols);
        }

        numSrcWithJoinCols = numJoinCols + srcNumFields;

        // setup src fields
        for (unsigned ii = 0; ii < srcNumFields; ii++) {
            srcAugCols[ii] = srcMeta->kvNamedMeta.valueNames_[ii];
            srcAugMeta.setFieldType(srcMeta->kvNamedMeta.kvMeta_.tupMeta_
                                        ->getFieldType(ii),
                                    ii);
        }

        srcAugMeta.setNumFields(srcNumFields);

        // setup join fields
        if (numJoinCols > 0) {
            joinCols = new (std::nothrow) XcalarApiRenameMap[numJoinCols];
            BailIfNull(joinCols);

            json_t *colJson = json_object_get(join, QpSelect::ColumnsKey);

            status = QueryCmdParser::parseColumnsArray(colJson, &err, joinCols);
            BailIfFailed(status);

            for (unsigned ii = 0; ii < numJoinCols; ii++) {
                srcAugMeta.addField(srcAugCols,
                                    joinCols[ii].newName,
                                    joinCols[ii].type);
            }
        }
        srcWithJoinMeta = srcAugMeta;

        // setup map fields
        if (numMaps > 0) {
            json_array_foreach (maps, ii, val) {
                ret = json_unpack_ex(val,
                                     &err,
                                     0,
                                     QueryCmdParser::JsonEvalFormatString,
                                     QueryCmdParser::NewFieldKey,
                                     &mapFields[ii],
                                     QueryCmdParser::EvalStringKey,
                                     &mapStrings[ii]);
                BailIfFailedWith(ret, StatusJsonQueryParseError);

                DfFieldType outputType =
                    XcalarEval::get()->getOutputTypeXdf(mapStrings[ii]);

                srcAugMeta.addField(srcAugCols, mapFields[ii], outputType);
            }
        }

        if (numGroupEvals > 0) {
            // need to create a scratchPadXdb to perform GroupBy
            scratchXdbId = XidMgr::get()->xidGetNext();

            DfFieldType keyTypes[TupleMaxNumValuesPerRecord];
            int keyIndexes[TupleMaxNumValuesPerRecord];
            Ordering keyOrderings[TupleMaxNumValuesPerRecord];
            const char *keyNames[TupleMaxNumValuesPerRecord];

            // contains columns required for gbEval
            const char *scratchCols[TupleMaxNumValuesPerRecord];
            NewTupleMeta scratchMeta;

            // setup keys
            if (numGroupKeys == 0) {
                // no keys specified, use dummy key
                numGroupKeys = 1;
                keyNames[0] = DsDefaultDatasetKeyName;
                keyTypes[0] = DfInt64;
                keyIndexes[0] = InvalidIdx;
                keyOrderings[0] = Random;
            } else {
                json_array_foreach (groupByKeys, ii, val) {
                    keyNames[ii] = json_string_value(val);

                    unsigned jj;
                    for (jj = 0; jj < srcAugMeta.getNumFields(); jj++) {
                        if (strcmp(keyNames[ii], srcAugCols[jj]) == 0) {
                            keyTypes[ii] = srcAugMeta.getFieldType(jj);
                            keyIndexes[ii] = ii;
                            keyOrderings[ii] = Random;

                            scratchMeta.addField(scratchCols,
                                                 keyNames[ii],
                                                 keyTypes[ii]);
                            break;
                        }
                    }

                    if (jj == srcAugMeta.getNumFields()) {
                        xSyslogTxnBuf(moduleName,
                                      XlogErr,
                                      "Key %s not found",
                                      keyNames[ii]);
                        status = StatusInval;
                        goto CommonExit;
                    }
                }
            }

            json_array_foreach (groupBys, ii, val) {
                const char *newFieldName = "", *accName = "", *groupField = "";

                json_unpack_ex(val,
                               NULL,
                               0,
                               QpSelect::GroupByFormatString,
                               QpSelect::FunctionKey,
                               &accName,
                               QpSelect::ArgKey,
                               &groupField,
                               QueryCmdParser::NewFieldKey,
                               &newFieldName);

                grpCtxs[ii].accType = strToAccumulatorType(accName);
                if (!isValidAccumulatorType(grpCtxs[ii].accType)) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "%s in not a valid select group eval",
                                  accName);
                    status = StatusInval;
                    goto CommonExit;
                }

                // check if field can be found in source + maps and
                // populate type
                status = GroupEvalContext::getArgType(groupField,
                                                      srcAugCols,
                                                      &srcAugMeta,
                                                      grpCtxs[ii].argType);
                BailIfFailed(status);

                status = grpCtxs[ii].initArgVal(groupField,
                                                scratchCols,
                                                &scratchMeta);
                BailIfFailed(status);

                // find result idx in dst xdb
                unsigned jj;
                for (jj = 0; jj < dstNumFields; jj++) {
                    if (strcmp(newFieldName,
                               dstMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                        grpCtxs[ii].resultIdx = jj;
                        break;
                    }
                }

                if (jj == dstNumFields) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "could not find column %s in dest",
                                  newFieldName);
                    status = StatusInval;
                    goto CommonExit;
                }
            }

            status = XdbMgr::get()->xdbCreate(scratchXdbId,
                                              numGroupKeys,
                                              keyNames,
                                              keyTypes,
                                              keyIndexes,
                                              keyOrderings,
                                              &scratchMeta,
                                              NULL,
                                              0,
                                              scratchCols,
                                              scratchMeta.getNumFields(),
                                              NULL,
                                              0,
                                              XdbLocal,
                                              DhtInvalidDhtId);
            BailIfFailed(status);

            status = XdbMgr::get()->xdbGet(scratchXdbId, &scratchXdb, NULL);
            assert(status == StatusOk);

            scratchMapping = Operators::getOpKvEntryCopyMapping(dstNumFields);
            BailIfNull(scratchMapping);

            Operators::initKvEntryCopyMapping(scratchMapping,
                                              scratchXdb->meta,
                                              dstMeta,
                                              0,
                                              dstNumFields,
                                              false,
                                              input->columns,
                                              input->numColumns);
        }

    CommonExit:
        return status;
    }
};

class SelectWork : public Schedulable
{
  public:
    SelectWork(TrackHelpers::WorkerType workerTypeIn,
               unsigned workerIdIn,
               TrackHelpers *trackHelpers,
               Xdb *dstXdb,
               Xdb *joinXdb,
               int64_t minBatchId,
               int64_t maxBatchId,
               bool commitCoalesce,
               OpKvEntryCopyMapping *mapping,
               OpKvEntryCopyMapping *joinMapping,
               SelectOpContext *opCtx,
               HashTree::Index *index,
               OpStatus *opStatus,
               uint64_t limitRows,
               HashTree *hashTree)
        : Schedulable("SelectWork"),
          trackHelpers_(trackHelpers),
          dstXdb_(dstXdb),
          joinXdb_(joinXdb),
          hashTree_(hashTree),
          minBatchId_(minBatchId),
          maxBatchId_(maxBatchId),
          commitCoalesce_(commitCoalesce),
          mapping_(mapping),
          joinMapping_(joinMapping),
          opCtx_(opCtx),
          index_(index),
          opStatus_(opStatus),
          limitRows_(limitRows),
          workerType_(workerTypeIn),
          workerId_(workerIdIn)
    {
        xdbMgr = XdbMgr::get();
        txn = Txn::currentTxn();

        if (joinXdb_) {
            joinKv_.kvMeta_ = &joinXdb_->meta->kvNamedMeta.kvMeta_;
        }
    }

    // constructor without any operation context
    SelectWork(TrackHelpers::WorkerType workerTypeIn,
               unsigned workerIdIn,
               TrackHelpers *trackHelpers,
               int64_t minBatchId,
               int64_t maxBatchId,
               bool commitCoalesce,
               HashTree *hashTree)
        : Schedulable("SelectWork"),
          trackHelpers_(trackHelpers),
          hashTree_(hashTree),
          minBatchId_(minBatchId),
          maxBatchId_(maxBatchId),
          commitCoalesce_(commitCoalesce),
          workerType_(workerTypeIn),
          workerId_(workerIdIn)
    {
        xdbMgr = XdbMgr::get();
        txn = Txn::currentTxn();
    }

    virtual ~SelectWork() = default;

    virtual void run();
    virtual void done() { delete this; }

    TrackHelpers *trackHelpers_;

    Xdb *dstXdb_ = NULL;
    Xdb *joinXdb_ = NULL;

    HashTree *hashTree_;
    int64_t minBatchId_;
    int64_t maxBatchId_;
    bool commitCoalesce_;
    OpKvEntryCopyMapping *mapping_ = NULL;
    OpKvEntryCopyMapping *joinMapping_ = NULL;
    SelectOpContext *opCtx_ = NULL;

    OpInsertHandle insertHandle_;
    bool insertHandleInit_ = false;
    XdbMeta *outputXdbMeta_;

    unsigned numMaps_ = 0;
    EvalContext *mapCtxs_ = NULL;
    EvalContext filterCtx_;

    HashTree::Index *index_ = NULL;

    TableCursor joinCursor_;
    NewKeyValueEntry joinKv_;
    bool joinKvInit_ = false;

    TableCursor mainXdbCursor_;
    bool mainCursorInit_ = false;
    FilterRange filterRange_;

    OpStatus *opStatus_ = NULL;
    uint64_t limitRows_ = 0;
    uint64_t selectedRowCount_ = 0;

    TrackHelpers::WorkerType workerType_;
    unsigned workerId_;

    XdbMgr *xdbMgr;
    Txn txn;

  private:
    Status setup();
    void tearDown();
    Status processSlotForInsert(unsigned slot);
    Status selectSlot(unsigned slot);
    Status selectIndexBatch(unsigned slot);

    Status coalescePages(uint64_t slot, uint64_t numPages, XdbPage **pagesOut);

    Status processSelectedKv(NewKeyValueEntry *selectKv,
                             NewTupleMeta *selectTupMeta,
                             XdbPage **coalescedPages);
};

Status
HashTree::select(Xdb *dstXdb, LibHashTreeGvm::GvmSelectInput *input)
{
    Status status = StatusOk;
    Status workerStatus = StatusOk;
    OpStatus *opStatus;
    unsigned numScheds;
    if (input->limitRows > 0) {
        // satisfy LIMIT requests with 1 worker only
        numScheds = 1;
    } else {
        numScheds =
            Operators::getNumWorkers(updateXdb_->numRows + mainXdb_->numRows);
    }

    OpKvEntryCopyMapping *mapping = NULL;
    OpKvEntryCopyMapping *joinMapping = NULL;
    SelectOpContext *opCtx = NULL;
    Index *index = NULL;
    Xdb *joinXdb = NULL;
    SelectWork **scheds = NULL;

    TrackHelpers *trackHelpers = NULL;
    // each slot is a chunk
    int64_t numChunks;
    Dag *dag = DagLib::get()->getDagLocal(input->dagId);

    if (strlen(input->joinTable.tableName) > 0) {
        status = XdbMgr::get()->xdbGet(input->joinTable.xdbId, &joinXdb, NULL);
        assert(status == StatusOk);
    }

    status = dag->getOpStatusFromXdbId(dstXdb->xdbId, &opStatus);
    assert(status == StatusOk);
    BailIfFailed(status);

    opCtx = new (std::nothrow) SelectOpContext();
    BailIfNull(opCtx);

    status = opCtx->setup(input, xdbMeta_, dstXdb->meta);
    BailIfFailed(status);

    // can't use index optimization and lookup join at the same time
    if (strlen(opCtx->filterString) > 0 && joinXdb == NULL) {
        EvalContext filterCtx;
        FilterRange filterRange;

        status = filterCtx.setupAst(opCtx->filterString, getXdbMeta());
        BailIfFailed(status);

        if (filterRange.init(&filterCtx, xdbMeta_)) {
            index = getIndex(filterRange.keyName_);
        }
    }

    if (index) {
        numChunks = index->numBatches;
    } else {
        // if we aren't using an Index, the hashing of the src and dst table
        // match, and we can construct the dst table in a way
        // that preserves the Hash/Ordering of the src table
        dstXdb->meta->prehashed = true;

        numChunks = hashSlots_;
    }

    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    {
        unsigned numDstFields;
        unsigned numRenameColumns;
        XcalarApiRenameMap *renameMap;
        XdbMeta *meta;

        if (opCtx->scratchXdb) {
            numDstFields = opCtx->scratchXdb->tupMeta.getNumFields();
            numRenameColumns = 0;
            renameMap = NULL;
            meta = opCtx->scratchXdb->meta;
        } else {
            numDstFields = dstXdb->tupMeta.getNumFields();
            numRenameColumns = input->numColumns;
            renameMap = input->columns;
            meta = dstXdb->meta;
        }

        if (joinXdb) {
            // setup mapping from joinXdb to augmented src
            joinMapping = Operators::getOpKvEntryCopyMapping(
                opCtx->srcAugMeta.getNumFields());
            BailIfNull(joinMapping);

            const char *valueNames[joinXdb->tupMeta.getNumFields()];

            for (unsigned ii = 0; ii < joinXdb->tupMeta.getNumFields(); ii++) {
                valueNames[ii] = joinXdb->meta->kvNamedMeta.valueNames_[ii];
            }

            Operators::initKvEntryCopyMapping(joinMapping,
                                              &joinXdb->tupMeta,
                                              valueNames,
                                              &opCtx->srcAugMeta,
                                              opCtx->srcAugCols,
                                              0,
                                              opCtx->srcAugMeta.getNumFields(),
                                              false,
                                              opCtx->joinCols,
                                              opCtx->numJoinCols,
                                              true);
        }

        // setup mapping from augmented src to dest
        mapping = Operators::getOpKvEntryCopyMapping(numDstFields);
        BailIfNull(mapping);

        Operators::initKvEntryCopyMapping(mapping,
                                          &opCtx->srcAugMeta,
                                          opCtx->srcAugCols,
                                          meta,
                                          0,
                                          numDstFields,
                                          false,
                                          renameMap,
                                          numRenameColumns);
    }

    scheds = new (std::nothrow) SelectWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = new (std::nothrow)
            SelectWork(ii == 0 ? TrackHelpers::Master : TrackHelpers::NonMaster,
                       ii,
                       trackHelpers,
                       dstXdb,
                       joinXdb,
                       input->minBatchId,
                       input->maxBatchId,
                       false,
                       mapping,
                       joinMapping,
                       opCtx,
                       index,
                       opStatus,
                       input->limitRows,
                       this);
        BailIfNull(scheds[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) scheds, numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    if (status == StatusSelectLimitReached) {
        status = StatusOk;
    }
    BailIfFailed(status);

    selectHead_ = new (std::nothrow) SelectMeta(&input->dstTable,
                                                input->minBatchId,
                                                input->maxBatchId,
                                                selectHead_);
    BailIfNull(selectHead_);

    numSelects_++;

CommonExit:
    if (scheds) {
        for (unsigned jj = 0; jj < numScheds; jj++) {
            if (scheds[jj]) {
                delete scheds[jj];
                scheds[jj] = NULL;
            }
        }
        delete[] scheds;
        scheds = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (mapping) {
        memFree(mapping);
        mapping = NULL;
    }

    if (joinMapping) {
        memFree(joinMapping);
        joinMapping = NULL;
    }

    if (opCtx) {
        delete opCtx;
        opCtx = NULL;
    }

    return status;
}

Status
HashTree::coalesce()
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    unsigned numScheds =
        Operators::getNumWorkers(updateXdb_->numRows + mainXdb_->numRows);
    SelectWork **scheds = NULL;
    TrackHelpers *trackHelpers = NULL;
    // each slot is a chunk
    uint64_t numChunks = hashSlots_;
    needsSizeRefresh_ = true;

    // Coalesce will modify mainXdb making our Indices inconsistent,
    // delete them to force access to the mainXdb
    // XXX: re-create Indices instead of removing them
    indexHashTable_.removeAll(&Index::destroy);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    scheds = new (std::nothrow) SelectWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = new (std::nothrow)
            SelectWork(ii == 0 ? TrackHelpers::Master : TrackHelpers::NonMaster,
                       ii,
                       trackHelpers,
                       0,
                       currentBatchId_,
                       true,
                       this);
        BailIfNull(scheds[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) scheds, numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    if (status == StatusSelectLimitReached) {
        status = StatusOk;
    }
    BailIfFailed(status);

    // we've successly migrated the rows from the update xdb to the main xdb
    // bump up the main batch id
    mainBatchId_ = currentBatchId_;
CommonExit:
    if (scheds) {
        for (unsigned jj = 0; jj < numScheds; jj++) {
            if (scheds[jj]) {
                delete scheds[jj];
                scheds[jj] = NULL;
            }
        }
        delete[] scheds;
        scheds = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    fixupXdbSlotAugs();
    XdbMgr::xdbUpdateCounters(mainXdb_);
    XdbMgr::xdbUpdateCounters(updateXdb_);

    return status;
}

Status
SelectWork::setup()
{
    Status status;

    // we either output to a temporary scratchXdb or the final dstXdb depending
    // on whether or not we need to do groupEvals
    if (opCtx_ && opCtx_->scratchXdb) {
        outputXdbMeta_ = opCtx_->scratchXdb->meta;
    } else if (dstXdb_) {
        outputXdbMeta_ = dstXdb_->meta;
    } else {
        outputXdbMeta_ = NULL;
    }

    // setup filters based off src hash tree + join
    if (opCtx_ && opCtx_->filterString != NULL &&
        opCtx_->filterString[0] != '\0') {
        status = filterCtx_.setupAst(opCtx_->filterString,
                                     opCtx_->numSrcWithJoinCols,
                                     opCtx_->srcAugCols);
        BailIfFailed(status);

        if (index_) {
            // we have an index for this range, prepare for index processing
            status = CursorManager::get()->createOnTable(hashTree_->mainXdb_,
                                                         Unordered,
                                                         Cursor::Untracked,
                                                         &mainXdbCursor_);
            BailIfFailed(status);

            mainCursorInit_ = true;

            filterRange_.init(&filterCtx_, hashTree_->getXdbMeta());
        }
    }

    // setup maps based off of src hash tree + join
    if (opCtx_ && opCtx_->numMaps > 0) {
        numMaps_ = opCtx_->numMaps;

        mapCtxs_ = new (std::nothrow) EvalContext[opCtx_->numMaps];
        BailIfNull(mapCtxs_);

        for (unsigned ii = 0; ii < opCtx_->numMaps; ii++) {
            status = mapCtxs_[ii].setupAst(opCtx_->mapStrings[ii],
                                           opCtx_->numSrcWithJoinCols,
                                           opCtx_->srcAugCols);
            BailIfFailed(status);

            if (outputXdbMeta_) {
                mapCtxs_[ii].setupResultIdx(opCtx_->mapFields[ii],
                                            opCtx_->srcAugMeta.getNumFields(),
                                            opCtx_->srcAugCols);
            }
        }
    }

    if (outputXdbMeta_) {
        // must use slotHash to populate slots directly in order for the
        // pipelined groupSlot operation to work
        status = opGetInsertHandle(&insertHandle_,
                                   &outputXdbMeta_->loadInfo,
                                   XdbInsertSlotHash);
        BailIfFailed(status);
        insertHandleInit_ = true;
    }

CommonExit:
    return status;
}

void
SelectWork::tearDown()
{
    if (insertHandleInit_) {
        opPutInsertHandle(&insertHandle_);
        insertHandleInit_ = false;
    }

    if (mainCursorInit_) {
        CursorManager::get()->destroy(&mainXdbCursor_);
        mainCursorInit_ = false;
    }

    if (mapCtxs_) {
        delete[] mapCtxs_;
        mapCtxs_ = NULL;
    }
}

void
SelectWork::run()
{
    Status status = StatusOk;

    status = trackHelpers_->helperStart();

    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    status = setup();

    for (uint64_t ii = 0; ii < trackHelpers_->getWorkUnitsTotal(); ii++) {
        if (!trackHelpers_->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            trackHelpers_->workUnitComplete(ii);
            continue;
        }

        int64_t slotId = ii;
        insertHandle_.slotId = slotId;

        if (index_) {
            status = selectIndexBatch(ii);
        } else if (joinXdb_) {
            status = CursorManager::get()->createOnSlot(joinXdb_->xdbId,
                                                        slotId,
                                                        joinXdb_->hashSlotInfo
                                                            .hashBaseAug[slotId]
                                                            .startRecord,
                                                        PartialAscending,
                                                        &joinCursor_);
            if (status == StatusOk) {
                status = joinCursor_.getNext(&joinKv_);
                assert(status == StatusOk);

                if (status == StatusOk) {
                    joinKvInit_ = true;
                    status = selectSlot(ii);
                }
            } else if (status == StatusNoData) {
                // this slot for join is empty, skip it
                status = StatusOk;
            }
        } else {
            status = selectSlot(ii);
        }

        if (status == StatusOk) {
            if (opCtx_ && opCtx_->scratchXdb) {
                // perform group evals on scratchXdb,
                // saving the results in dstXdb
                status = groupSlot(opCtx_->scratchXdb,
                                   dstXdb_,
                                   opCtx_->scratchMapping,
                                   slotId,
                                   opCtx_->numGroupEvals,
                                   opCtx_->grpCtxs);

                if (status == StatusOk) {
                    // we no longer need the slot in scratchXdb
                    XdbMgr::get()->xdbDropSlot(opCtx_->scratchXdb,
                                               slotId,
                                               true);
                }
            }
        }

        if (status == StatusOk) {
            if (limitRows_ > 0 && selectedRowCount_ >= limitRows_) {
                status = StatusSelectLimitReached;
            }
        }

        if (dstXdb_ && !index_) {
            // select without index maintains the sortedness of the keys
            XdbMgr::xdbSetSlotSortState(dstXdb_, slotId, SortedFlag);
        }

        if (opStatus_) {
            if (opStatus_->atomicOpDetails.cancelled) {
                status = StatusCanceled;
            }

            // each slot is a piece of work
            atomicAdd64(&opStatus_->atomicOpDetails.numWorkCompletedAtomic, 1);
        }

        trackHelpers_->workUnitComplete(ii);
    }

CommonExit:
    tearDown();
    // status is managed outside of trackHelpers
    trackHelpers_->helperDone(status);
}

Status
SelectWork::selectSlot(unsigned slotId)
{
    Status status = StatusOk;
    XdbPage *xdbPage = NULL;
    XdbMgr *xdbMgr = XdbMgr::get();

    XdbAtomicHashSlot *hashSlot =
        &hashTree_->updateXdb_->hashSlotInfo.hashBase[slotId];
    XdbHashSlotAug *hashSlotAug =
        &hashTree_->updateXdb_->hashSlotInfo.hashBaseAug[slotId];
    bool slotLocked = false;

    if (commitCoalesce_ || maxBatchId_ > hashTree_->mainBatchId_) {
        // use slot locks to protect the updateXdb during sort
        xdbMgr->lockSlot(&hashTree_->updateXdb_->hashSlotInfo, slotId);
        slotLocked = true;

        // need to sort the updates in order to efficiently coalesce them
        if (likely(!hashSlot->sortFlag)) {
            status =
                xdbMgr->sortHashSlotEx(hashTree_->updateXdb_, slotId, &xdbPage);
            if (status == StatusNoData) {
                // no updates
                status = StatusOk;
            }
            BailIfFailed(status);
        } else {
            xdbPage = xdbMgr->getXdbHashSlotNextPage(hashSlot);
            if (xdbPage == NULL) {
                // no updates
                status = StatusOk;
            }
        }
    }

    // don't need the slot lock at this point as we are just cursoring the xdbs
    if (slotLocked) {
        xdbMgr->unlockSlot(&hashTree_->updateXdb_->hashSlotInfo, slotId);
        slotLocked = false;
    }

    if (commitCoalesce_) {
        // XXX: we could have an optimization where we take the updates and
        // directly insert them in place into the main xdb. This will only
        // be efficient if the updates are .1% of the main xdb

        XdbPage *coalescedPages = NULL;

        status = coalescePages(slotId, hashSlotAug->numPages, &coalescedPages);
        if (status != StatusOk) {
            XdbPage *xdbPageTemp;
            XdbPage *xdbPage = coalescedPages;

            while (xdbPage) {
                // Save next page state before deleting page
                xdbPageTemp = (XdbPage *) xdbPage->hdr.nextPage;
                xdbMgr->xdbFreeXdbPage(xdbPage);

                xdbPage = xdbPageTemp;
            }

            goto CommonExit;
        }

        // take the coalesced pages and write them into the main xdb.
        // slotAug will be fixed up during fixupMainXdbSlotAugs at the end of
        // coalesce
        XdbAtomicHashSlot *mainSlot =
            &hashTree_->mainXdb_->hashSlotInfo.hashBase[slotId];

        xdbMgr->xdbDropSlot(hashTree_->mainXdb_, slotId, true);
        xdbMgr->setXdbHashSlotNextPage(mainSlot,
                                       hashTree_->mainXdb_->xdbId,
                                       slotId,
                                       coalescedPages);

        // clear the slot from the updateXdb
        xdbMgr->xdbDropSlot(hashTree_->updateXdb_, slotId, true);
    } else {
        status = coalescePages(slotId, hashSlotAug->numPages, NULL);
        BailIfFailed(status);
    }

CommonExit:
    if (slotLocked) {
        xdbMgr->unlockSlot(&hashTree_->updateXdb_->hashSlotInfo, slotId);
        slotLocked = false;
    }

    return status;
}

Status
SelectWork::selectIndexBatch(unsigned batchNum)
{
    Status status;
    XdbPageBatch *batch = &index_->batches[batchNum];
    XdbPage *xdbPage;

    bool valid = batch->minValid && batch->maxValid;
    NewTuplesCursor tupCursor;
    XdbPgCursor pageCursor;

    const NewKeyValueMeta *kvMeta =
        &index_->indexXdb->meta->kvNamedMeta.kvMeta_;
    const NewKeyValueMeta *mainKvMeta =
        &hashTree_->mainXdb_->meta->kvNamedMeta.kvMeta_;

    // create a local copy of kvMeta for the selected kv. This meta will be
    // modified with the results of the map evals
    NewTupleMeta selectTupMeta = *mainKvMeta->tupMeta_;
    NewKeyValueMeta selectKvMeta = *mainKvMeta;
    selectKvMeta.tupMeta_ = &selectTupMeta;

    NewKeyValueEntry indexEntry(kvMeta);
    NewKeyValueEntry selectKv(&selectKvMeta);

    // check if any row in this batch is in the filter range
    if (!valid || !filterRange_.inRange(batch->min, batch->max)) {
        // We're using filters to skip batches.
        status = StatusOk;
        goto CommonExit;
    }

    // Ideally, XdbPgCursor constructor itself should be grabbing
    // a ref to xdbPage. However, constructors can't handle failure, so we
    // grab the ref outside, and have the constructor steal the ref
    xdbPage = batch->start;
    status = xdbPage->getRef(index_->indexXdb);
    BailIfFailed(status);
    new (&pageCursor)
        XdbPgCursor(index_->indexXdb,
                    batch->startSlot,
                    xdbPage,
                    XdbPgCursor::CanCrossSlotBoundary | XdbPgCursor::NoLocking);

    while (xdbPage != NULL) {
        new (&tupCursor) NewTuplesCursor(pageCursor.xdbPage_->tupBuf);

        for (uint64_t ii = 0; ii < xdbPage->tupBuf->getNumTuples(); ii++) {
            status = tupCursor.getNext(kvMeta->tupMeta_, &indexEntry.tuple_);
            assert(status == StatusOk);

            DfFieldValue keyVal = indexEntry.tuple_.get(0, &valid);

            // check if this key is in the filter range
            if (!valid || !filterRange_.inRange(keyVal, keyVal)) {
                continue;
            }

            status = HashTree::Index::restoreCursorFromIndexKv(&indexEntry,
                                                               &mainXdbCursor_);
            BailIfFailed(status);

            status = mainXdbCursor_.getNext(&selectKv);
            assert(status == StatusOk);
            BailIfFailed(status);

            status = processSelectedKv(&selectKv, &selectTupMeta, NULL);
            BailIfFailed(status);

            if (limitRows_ > 0 && selectedRowCount_ >= limitRows_) {
                goto CommonExit;
            }
        }

        if (xdbPage == batch->end) {
            break;
        }

        // This is actually a really screwed up pattern.
        // xdbPage has a "borrowed" reference from pageCursor. If pageCursor
        // advances, then the previous xdbPage is stale.
        status = pageCursor.getNextPage(&xdbPage);
        BailIfFailed(status);
    }

CommonExit:
    if (status == StatusNoData) {
        // no data to process, non-fatal
        status = StatusOk;
    }

    return status;
}

Status
SelectWork::coalescePages(uint64_t slotId,
                          uint64_t numPages,
                          XdbPage **pagesOut)
{
    struct BatchAccumulator {
        bool init = false;
        DfFieldValue maxBatchId;
        DfFieldValue maxRank;
    };

    Status status = StatusOk;

    NewKeyValueMeta *kvMeta = &hashTree_->xdbMeta_->kvNamedMeta.kvMeta_;
    const NewTupleMeta *tupMeta = kvMeta->tupMeta_;

    // create a local copy of kvMeta for the selected kv. This meta will be
    // modified with the results of the map evals
    NewTupleMeta selectTupMeta;
    NewKeyValueMeta selectKvMeta = *kvMeta;
    selectKvMeta.tupMeta_ = &selectTupMeta;

    if (opCtx_) {
        selectTupMeta = opCtx_->srcWithJoinMeta;
    } else {
        selectTupMeta = *tupMeta;
    }

    unsigned numFields = kvMeta->tupMeta_->getNumFields();

    TableCursor mainCursor;
    bool mainExhausted = true;

    TableCursor updateCursor;
    bool updateExhausted = true;

    NewKeyValueEntry *mainKv = NULL;
    NewKeyValueEntry *updateKv = NULL;
    NewKeyValueEntry *curKv = NULL;
    NewKeyValueEntry *selectKv = NULL;

    bool commitCoalesce = (pagesOut != NULL);

    BatchAccumulator acc[numFields];

    mainKv = new (std::nothrow) NewKeyValueEntry(kvMeta);
    BailIfNull(mainKv);

    updateKv = new (std::nothrow) NewKeyValueEntry(kvMeta);
    BailIfNull(updateKv);

    curKv = new (std::nothrow) NewKeyValueEntry(kvMeta);
    BailIfNull(curKv);

    selectKv = new (std::nothrow) NewKeyValueEntry(&selectKvMeta);
    BailIfNull(selectKv);

    status = XdbMgr::get()->createCursorFast(hashTree_->mainXdb_,
                                             slotId,
                                             &mainCursor);
    if (status == StatusOk) {
        status = mainCursor.getNext(mainKv);
    }
    if (status == StatusOk) {
        mainExhausted = false;
    } else if (status == StatusNoData) {
        status = StatusOk;
    } else if (status != StatusNoData) {
        goto CommonExit;
    }

    if (commitCoalesce || maxBatchId_ > hashTree_->mainBatchId_) {
        // only look at the updates if we want something later than main
        status = XdbMgr::get()->createCursorFast(hashTree_->updateXdb_,
                                                 slotId,
                                                 &updateCursor);
        if (status == StatusOk) {
            status = updateCursor.getNext(updateKv);
        }
        if (status == StatusOk) {
            updateExhausted = false;
        } else if (status == StatusNoData) {
            status = StatusOk;
        } else if (status != StatusNoData) {
            goto CommonExit;
        }
    }

    while (!mainExhausted || !updateExhausted) {
        int ret;
        bool advanceMain = false;
        bool applyUpdates = false;
        if (mainExhausted) {
            ret = 1;
        } else if (updateExhausted) {
            ret = -1;
        } else {
            // XXX need to respect keys' entry in the valid maps
            // key count and type match has been verfied already
            ret =
                DataFormat::fieldArrayCompare(hashTree_->xdbMeta_->numKeys,
                                              NULL,
                                              hashTree_->xdbMeta_->keyIdxOrder,
                                              tupMeta,
                                              &mainKv->tuple_,
                                              hashTree_->xdbMeta_->keyIdxOrder,
                                              tupMeta,
                                              &updateKv->tuple_);
        }

        if (ret > 0) {
            // key exists only in update, apply updates onto empty kv
            selectKv->tuple_.setInvalid(0, numFields);
            applyUpdates = true;
        } else if (ret < 0) {
            // key exists only in main, process main and continue
            mainKv->tuple_.cloneTo(tupMeta, &selectKv->tuple_);
            advanceMain = true;
        } else {
            // key exists in both main and update, apply updates onto main
            mainKv->tuple_.cloneTo(tupMeta, &selectKv->tuple_);
            advanceMain = true;
            applyUpdates = true;
        }

        if (applyUpdates) {
            updateKv->tuple_.cloneTo(kvMeta->tupMeta_, &curKv->tuple_);

            // initialize accs
            memZero(acc, sizeof(acc));
        }

        // iterate updates and apply
        while (applyUpdates) {
            bool retIsValid;
            DfFieldValue batchId = updateKv->tuple_.get(hashTree_->batchIdx_,
                                                        numFields,
                                                        DfInt64,
                                                        &retIsValid);
            assert(retIsValid);
            DfFieldValue rank = updateKv->tuple_.get(hashTree_->rankOverIdx_,
                                                     numFields,
                                                     DfInt64,
                                                     &retIsValid);
            assert(retIsValid);

            if (batchId.int64Val > maxBatchId_ ||
                batchId.int64Val < minBatchId_) {
                // skip batches that do not fall within our range
                goto AdvanceUpdate;
            }

            for (unsigned ii = 0; ii < numFields; ii++) {
                bool retIsValid;
                DfFieldType type = kvMeta->tupMeta_->getFieldType(ii);
                DfFieldValue val =
                    updateKv->tuple_.get(ii, numFields, type, &retIsValid);
                // process valid fields only
                if (!retIsValid) {
                    continue;
                }

                int ret = -1;
                if (!acc[ii].init) {
                    acc[ii].init = true;
                } else {
                    // compare batchIds and set acc accordingly
                    ret = DataFormat::fieldCompare(DfInt64,
                                                   acc[ii].maxBatchId,
                                                   batchId);
                    if (ret == 0) {
                        // these came from the same batch, compare ranks
                        ret = DataFormat::fieldCompare(DfInt64,
                                                       acc[ii].maxRank,
                                                       rank);
                    }
                }

                if (ret < 0) {
                    // encountered a more recent update
                    acc[ii].maxBatchId = batchId;
                    acc[ii].maxRank = rank;
                    selectKv->tuple_.set(ii, val, type);
                }
            }

        AdvanceUpdate:
            status = updateCursor.getNext(updateKv);
            if (status == StatusNoData) {
                updateExhausted = true;
                break;
            }
            BailIfFailed(status);

            applyUpdates =
                DataFormat::fieldArrayCompare(hashTree_->xdbMeta_->numKeys,
                                              NULL,
                                              hashTree_->xdbMeta_->keyIdxOrder,
                                              kvMeta->tupMeta_,
                                              &curKv->tuple_,
                                              hashTree_->xdbMeta_->keyIdxOrder,
                                              kvMeta->tupMeta_,
                                              &updateKv->tuple_) == 0;
        }

        status = processSelectedKv(selectKv, &selectTupMeta, pagesOut);
        BailIfFailed(status);

        if (limitRows_ > 0 && selectedRowCount_ >= limitRows_) {
            goto CommonExit;
        }

        if (advanceMain) {
            status = mainCursor.getNext(mainKv);
            if (status == StatusNoData) {
                mainExhausted = true;
            } else if (status != StatusOk) {
                goto CommonExit;
            }
        }
    }

    status = StatusOk;

CommonExit:
    if (mainKv) {
        delete mainKv;
    }

    if (updateKv) {
        delete updateKv;
    }

    if (curKv) {
        delete curKv;
    }

    if (selectKv) {
        delete selectKv;
    }

    return status;
}

Status
SelectWork::processSelectedKv(NewKeyValueEntry *selectKv,
                              NewTupleMeta *selectTupMeta,
                              XdbPage **coalescedPages)
{
    Status status = StatusOk;

    bool returnPages = (coalescedPages != NULL);

    bool retIsValid;
    {
        DfFieldValue finalOp =
            selectKv->tuple_.get(hashTree_->opCodeIdx_, &retIsValid);
        if (!retIsValid) {
            goto CommonExit;
        }

        if (finalOp.int64Val == DeleteOpCode.int64Val) {
            // skip rows that have been marked as deleted
            goto CommonExit;
        }
    }

    {
        DfFieldValue batchId =
            selectKv->tuple_.get(hashTree_->batchIdx_, &retIsValid);
        if (!retIsValid) {
            goto CommonExit;
        }

        if (batchId.int64Val > maxBatchId_ || batchId.int64Val < minBatchId_) {
            // skip batches that do not fall within our range
            goto CommonExit;
        }
    }

    // execute join based slot lookup
    if (joinXdb_) {
        bool foundMatch = false;
        while (joinKvInit_) {
            int ret =
                DataFormat::fieldArrayCompare(hashTree_->xdbMeta_->numKeys,
                                              NULL,
                                              hashTree_->xdbMeta_->keyIdxOrder,
                                              selectTupMeta,
                                              &selectKv->tuple_,
                                              joinXdb_->meta->keyIdxOrder,
                                              &joinXdb_->tupMeta,
                                              &joinKv_.tuple_);

            if (ret < 0) {
                // current key smaller than join key, skip current
                goto CommonExit;
            } else if (ret == 0) {
                // process current key
                foundMatch = true;
                break;
            } else {
                // if ret > 0, current key is larger than join key
                // continue loop until we find a match or joinCursor is
                // exhausted
                status = joinCursor_.getNext(&joinKv_);
                if (status == StatusNoData) {
                    joinKvInit_ = false;
                    status = StatusOk;
                } else if (status != StatusOk) {
                    goto CommonExit;
                }
            }
        }

        if (!foundMatch) {
            // could not find a matching key in Join table, skip current key
            goto CommonExit;
        }

        // fill selectkv with join columns
        Operators::shallowCopyKvEntry(selectKv,
                                      opCtx_->srcAugMeta.getNumFields(),
                                      &joinKv_,
                                      joinMapping_,
                                      0,
                                      opCtx_->numSrcWithJoinCols);
    }

    // execute filter based on original fields
    if (filterCtx_.astInit_) {
        bool result = false;

        status = XcalarEval::get()->executeFilterAst(selectKv,
                                                     opCtx_->numSrcWithJoinCols,
                                                     &filterCtx_.ast_,
                                                     filterCtx_.argIndices_,
                                                     filterCtx_.scalars_,
                                                     &result);
        BailIfFailed(status);

        if (result == false) {
            // skip rows that do not satisfy the filter condition
            goto CommonExit;
        }
    }

    // add map results to selected kv
    for (unsigned ii = 0; ii < numMaps_; ii++) {
        if (mapCtxs_[ii].resultIdx_ != InvalidIdx) {
            status =
                XcalarEval::get()->executeEvalAst(selectKv,
                                                  opCtx_->numSrcWithJoinCols,
                                                  &mapCtxs_[ii].ast_,
                                                  mapCtxs_[ii].argIndices_,
                                                  mapCtxs_[ii].scalars_,
                                                  &mapCtxs_[ii].result_,
                                                  &mapCtxs_[ii].resultType_);
            if (XcalarEval::isFatalError(status)) {
                goto CommonExit;
            }

            mapCtxs_[ii].resultValid_ = (status == StatusOk);
        }
    }

    for (unsigned ii = 0; ii < numMaps_; ii++) {
        if (mapCtxs_[ii].resultValid_) {
            selectKv->tuple_.set(mapCtxs_[ii].resultIdx_,
                                 mapCtxs_[ii].result_,
                                 mapCtxs_[ii].resultType_);
        } else {
            selectKv->tuple_.setInvalid(mapCtxs_[ii].resultIdx_);
        }

        selectTupMeta->addOrReplaceField(mapCtxs_[ii].resultIdx_,
                                         mapCtxs_[ii].resultType_);
    }

    if (mapping_ == NULL || mapping_->isReplica) {
        // if we do not need to remap the selectKv, just work with it directly
        if (insertHandleInit_) {
            status = opPopulateInsertHandle(&insertHandle_, selectKv);
            BailIfFailed(status);
        }

        if (returnPages) {
            status =
                opInsertPageChain(coalescedPages,
                                  NULL,
                                  &hashTree_->xdbMeta_->kvNamedMeta.kvMeta_,
                                  &selectKv->tuple_,
                                  hashTree_->updateXdb_);
            BailIfFailed(status);
        }
    } else {
        assert(insertHandleInit_ && !returnPages);
        // we don't need all the fields in selectKv, apply the field mapping_

        NewKeyValueMeta *dstKvMeta =
            &insertHandle_.loadInfo->dstXdb->meta->kvNamedMeta.kvMeta_;
        unsigned dstNumFields = dstKvMeta->tupMeta_->getNumFields();
        NewKeyValueEntry tmpKvEntry(dstKvMeta);

        for (unsigned ii = 0; ii < dstNumFields; ii++) {
            size_t idx = mapping_->srcIndices[ii];
            if (unlikely(idx == (size_t) NewTupleMeta::DfInvalidIdx)) {
                // this means we won't be getting this field from src
                continue;
            }

            bool retIsValid;
            DfFieldValue valueTmp = selectKv->tuple_.get(idx, &retIsValid);
            if (retIsValid) {
                tmpKvEntry.tuple_.set(ii,
                                      valueTmp,
                                      dstKvMeta->tupMeta_->getFieldType(ii));
            } else {
                tmpKvEntry.tuple_.setInvalid(ii);
            }
        };

        status = opPopulateInsertHandle(&insertHandle_, &tmpKvEntry);
        BailIfFailed(status);
    }

    selectedRowCount_ += 1;

    // reset tup meta for fields replaced by map
    for (unsigned ii = 0; ii < numMaps_; ii++) {
        unsigned resultIdx = mapCtxs_[ii].resultIdx_;

        if (resultIdx < opCtx_->numSrcWithJoinCols) {
            selectTupMeta->setFieldType(opCtx_->srcWithJoinMeta.getFieldType(
                                            resultIdx),
                                        resultIdx);
        }
    }

CommonExit:
    return status;
}
