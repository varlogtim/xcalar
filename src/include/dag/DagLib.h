// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DAGLIB_H_
#define _DAGLIB_H_

#include <jansson.h>

#include "util/Archive.h"
#include "xdb/TableTypes.h"
#include "dataset/DatasetTypes.h"
#include "libapis/LibApisCommon.h"
#include "msg/MessageTypes.h"
#include "dag/DagTypes.h"
#include "dag/DagNodeTypes.h"
#include "operators/OperatorsTypes.h"
#include "util/IntHashTable.h"
#include "bc/BufferCache.h"
#include "dag/Dag.h"
#include "gvm/GvmTarget.h"
#include "ns/LibNsTypes.h"

typedef Xid RetinaId;

class DagLib final : public DagTypes
{
#ifdef DEBUG
    friend class LibDagTest;
#endif
    friend class Optimizer;
    friend class Dag;
    friend class OperatorHandlerExecuteRetina;
    friend class HashTreeMgr;
    friend class QueryManager;

  public:
    // static method
    static Status init();
    static DagLib *get();
    void destroy();

    static constexpr const char *SyntheticRetinaNameDelim = "#";
    MustCheck Status isValidRetinaName(const char *retinaName);
    MustCheck Status
    copyDagNodesToDagOutput(XcalarApiDagOutput *dagOutput,
                            size_t bufSize,
                            void *dagNodesSelected[],
                            uint64_t numNodes,
                            XcalarApiUdfContainer *udfContainer,
                            DagNodeType dagNodeType);
    MustCheck Status
    copyDagNodeToXcalarApiDagNode(XcalarApiDagNode *apiDagNodeOut,
                                  size_t bufSize,
                                  DagNodeTypes::Node *dagNodeIn,
                                  XcalarApiUdfContainer *udfContainer,
                                  size_t *bytesCopiedOut);
    static MustCheck Status convertNamesToImmediate(Dag *dag,
                                                    XcalarApis api,
                                                    XcalarApiInput *input);
    MustCheck Status convertNamesFromImmediate(XcalarApis api,
                                               XcalarApiInput *input);
    MustCheck Status
    convertUDFNamesToRelative(XcalarApis api,
                              XcalarApiUdfContainer *udfContainer,
                              XcalarApiInput *input);

    MustCheck Status copyUDFs(XcalarApis api,
                              XcalarApiInput *input,
                              XcalarApiUdfContainer *fromUdfContainer,
                              XcalarApiUdfContainer *toUdfContainer);
    MustCheck SourceType getSourceTypeFromApi(XcalarApis api);
    MustCheck size_t sizeOfXcalarApiDagNode(size_t apiInputSize,
                                            unsigned numParents,
                                            unsigned numChildren);

    // static Retina functions
    static void destroyRetinaInfo(RetinaInfo *retinaInfo);
    static MustCheck Status parseRetinaFileToJson(void *buf,
                                                  size_t bufSize,
                                                  const char *retinaName,
                                                  json_t **retJsonOut);
    static MustCheck Status parseRetinaFile(void *buf,
                                            size_t bufSize,
                                            const char *retinaName,
                                            RetinaInfo **retinaInfoOut);
    static MustCheck Status loadRetinaFromFile(const char *retinaPath,
                                               uint8_t **retinaBufOut,
                                               size_t *retinaBufSizeOut,
                                               struct stat *statBuf);
    static MustCheck Status retinaUpdateDlm(MsgEphemeral *ephIn, void *payload);
    static MustCheck Status setParamInput(XcalarApiParamInput *paramInput,
                                          XcalarApis paramType,
                                          const char *inputStr);
    MustCheck Status createNewDagLocal(void *args);
    MustCheck Status destroyDagLocal(void *args);
    MustCheck Status prepareDeleteDagLocal(void *args);

    // non-static method
    MustCheck Status createNewDag(uint64_t numSlot,
                                  DagLib::GraphType graphType,
                                  XcalarApiUdfContainer *udfContainer,
                                  Dag **dag);

    MustCheck Status destroyDag(Dag *dag, DestroyOpts destroyOpts);

    // caller must ensure a global reference is held on the dag handle before
    // calling this function.  It is guaranteed to return non-NULL
    MustCheck Dag *getDagLocal(DagId dagId);
    void destroyDagNode(DagNodeTypes::Node *dagNode);
    MustCheck Dag *lookupDag(DagId dagId);
    void getXcalarApiOpStatusComplete(MsgEphemeral *eph, void *payload);
    void cancelOpLocal(MsgEphemeral *eph, void *payload);
    void createOpLocal(MsgEphemeral *eph, void *payload);
    void getXcalarApiOpStatusLocal(MsgEphemeral *eph, void *payload);

    // Retina functions
    MustCheck Status importRetina(XcalarApiImportRetinaInput *importRetinaInput,
                                  XcalarApiOutput **importRetinaOutputOut,
                                  size_t *importRetinaOutputSizeOut);

    MustCheck Status
    importRetinaFromExecute(XcalarApiExecuteRetinaInput *executeRetinaInput);

    MustCheck Status exportRetina(XcalarApiExportRetinaInput *exportRetinaInput,
                                  XcalarApiOutput **outputOut,
                                  size_t *outputSizeOut);
    MustCheck Status makeRetina(Dag *dag,
                                XcalarApiMakeRetinaInput *makeRetinaInput,
                                size_t inputSize,
                                bool hasUDFs);
    MustCheck Status deleteRetina(const char *retinaName);
    MustCheck Status listRetinas(const char *namePattern,
                                 XcalarApiOutput **apiOutputOut,
                                 size_t *outputSizeOut);
    MustCheck Status getRetina(const char *retinaName,
                               XcalarApiOutput **outputOut,
                               size_t *outputSizeOut);

    MustCheck Status getRetinaJson(const char *retinaName,
                                   XcalarApiUdfContainer *udfContainer,
                                   XcalarApiOutput **outputOut,
                                   size_t *outputSizeOut);
    MustCheck Status updateRetina(const char *retinaName,
                                  const char *retinaJson,
                                  size_t retinaJsonLen,
                                  XcalarApiUdfContainer *udfContainer);
    MustCheck Status
    executeRetina(const XcalarApiUserId *userId,
                  XcalarApiExecuteRetinaInput *executeRetinaInput,
                  Dag **outputDag);
    MustCheck Status listParametersInRetina(const char *retinaName,
                                            XcalarApiOutput **output,
                                            size_t *outputSizeOut);
    MustCheck Status getStrFromParamInput(XcalarApiParamInput *paramInput,
                                          char *stringsOut[],
                                          unsigned *numStringsOut);
    MustCheck Status getParamStrFromXcalarApiInput(XcalarApiInput *input,
                                                   XcalarApis api,
                                                   char ***stringsOut,
                                                   size_t **bufSizesOut,
                                                   unsigned *numStringsOut);
    MustCheck Status updateXcalarApiInputFromParamInput(
        XcalarApiInput *apiInput, XcalarApiParamInput *paramInput);

    static void renameDstApiInput(DagNodeTypes::Node *dagNode,
                                  const char *oldName,
                                  const char *newName);

    static void renameSrcApiInput(const char *oldName,
                                  const char *newName,
                                  DagNodeTypes::Node *dagNode);

    MustCheck Status copyNodesFromRetinaDag(Dag *retinaDag, Dag *dagOut);
    MustCheck Status copyRetinaToNewDag(
        const XcalarApiExecuteRetinaInput *executeRetinaInput, Dag *dstDag);
    MustCheck Status
    populateJsonUdfModulesArray(const char *retinaName,
                                const char *modulePathPrefix,
                                json_t *retinaInfoJson,
                                ArchiveManifest *manifest,
                                XcalarApiUdfContainer *udfContainer,
                                EvalUdfModuleSet *udfModules,
                                json_t *archiveChecksum);
    static MustCheck Status
    populateUdfModulesArray(json_t *jsonRecord,
                            const char *retinaName,
                            RetinaInfo *retinaInfo,
                            ArchiveManifest *manifest,
                            const json_t *archiveChecksum);
    MustCheck Status fixupQueryGraphForWorkbookUpload(Dag *queryGraph);

    // twoPc functions
    void dlmRetinaTemplateMsg(MsgEphemeral *eph, void *payload);
    void dlmRetinaTemplateCompletion(MsgEphemeral *eph, void *payload);

    // Retina related stuff
    static MustCheck size_t sizeOfImportRetinaOutputBuf(
        uint64_t numUdfModules, size_t totalUdfModulesStatusesSize);

    enum RetinaTemplateOp {
        AddTemplate = 200,
        GetTemplate,
        UpdateTemplate,
        DeleteTemplate,
    };

    // Result from twoPc
    struct RetinaTemplateMsgResult {
        // The operation is used by the completion handler to determine
        // whether or not to expect a returned payload
        RetinaTemplateOp operation;
        Status status = StatusOk;
        size_t outputSize = 0;
        void *retinaTemplate = NULL;
    };

    // Format of returned retina template payload
    struct RetinaTemplateReturnedPayload {
        RetinaId retinaId;
        size_t templateSize;
        uint8_t retinaTemplate[0];
    };

  private:
    static constexpr uint64_t RetinaTemplateHTSlots = 11;

    struct DgRetina;

    struct ParameterHashEltByName {
        StringHashTableHook hook;
        const char *getName() const { return parameter.parameterName; };
        RefCount refCount;
        DgRetina *retina;
        XcalarApiParameter parameter;
        void del();
    };

    static constexpr uint64_t ParamHashTableByNameSlots = 11;
    typedef StringHashTable<ParameterHashEltByName,
                            &ParameterHashEltByName::hook,
                            &ParameterHashEltByName::getName,
                            ParamHashTableByNameSlots,
                            hashStringFast>
        ParamHashTableByName;

    static constexpr uint64_t DagHashTableByIdSlots = 13;
    typedef IntHashTable<NodeId,
                         Dag,
                         &Dag::hook,
                         &Dag::getId,
                         DagHashTableByIdSlots,
                         hashIdentity>
        DagHashTableById;

    static constexpr uint64_t DagNodeTableByIdCount = 101;
    typedef IntHashTable<DagTypes::NodeId,
                         DagNodeTypes::Node,
                         &DagNodeTypes::Node::globalIdHook,
                         &DagNodeTypes::Node::getId,
                         DagNodeTableByIdCount,
                         hashIdentity>
        DagNodeTableById;
    DagNodeTableById dagNodeTableById_;
    Mutex dagNodeTableIdLock_;

    struct DgRetina {
        DgRetina(){};
        ~DgRetina(){};

        Mutex lock;
        RetinaId id = XidInvalid;

        uint64_t numParameters = 0;
        ParamHashTableByName *parameterHashTableByName = NULL;

        Dag *dag = NULL;
        uint64_t numColumnHints = 0;
        Column *columnHints = NULL;

        uint64_t numTargets = 0;

        // filled during executeRetina
        Dag *sessionDag = NULL;

        // This field must be the last field
        NodeId targetNodeId[0];
    };

    struct MakeRetinaParam {
        RetinaId retinaId;
        DagId srcDagId;
        DagId dstDagId;
        XcalarApiMakeRetinaInput retinaInput;
    };

    struct RetinaUpdate {
        struct RetinaUpdate *next;
        size_t serializedDagSize;
        uint8_t serializedDag[0];
    };

    // The retina template is the essence of a retina and be used to
    // "hydrate" a retina.  This structure is passed over the wire.
    // Updates is a LIFO stack which is applied to the serialized dag
    // is not sent across the wire
    struct RetinaTemplate {
        RetinaId retinaId;
        size_t totalSize;
        size_t makeRetinaParamSize;
        size_t serializedDagSize;
        uint64_t numRetinaUpdates = 0;
        RetinaUpdate *updates = NULL;
        // The content consists of:
        //  * makeRetinaParam with serialized retinaInput
        //  * serialized src Dag
        uint8_t serializedContent[0];
    };

    // Hash table used by twoPc handlers to add/delete retina templates.
    // Each node is the dlm for a subset of the overall retina template
    // population.
    struct RetinaIdToTemplateHTEntry {
        IntHashTableHook hook;
        RetinaId getRetinaId() const { return retinaId; }
        RetinaId retinaId;
        RetinaTemplate *retinaTemplate;
        void del();
    };

    IntHashTable<RetinaId,
                 RetinaIdToTemplateHTEntry,
                 &RetinaIdToTemplateHTEntry::hook,
                 &RetinaIdToTemplateHTEntry::getRetinaId,
                 RetinaTemplateHTSlots,
                 hashIdentity>
        retinaIdToTemplateHT_;
    Mutex retinaIdToTemplateHTLock_;

    // Serialize operations through dlm
    Mutex retinaTemplateDlmLock_;

    struct RetinaUpdateMsgHdr {
        RetinaId retinaId;
    };

    struct RetinaDeleteInput {
        RetinaUpdateMsgHdr retinaMsgHdr;
    };

    struct RetinaAddInput {
        size_t retinaTemplateSize;
        uint8_t retinaTemplate[0];
    };

    struct RetinaUpdateInput {
        RetinaUpdateMsgHdr retinaMsgHdr;
        size_t serializedDagSize;
        uint8_t serializedDag[0];
    };

    struct RetinaInstantiateMsg {
        RetinaUpdateMsgHdr retinaMsgHdr;
        DagId instanceDagId;
        uint64_t numParameters;
        XcalarApiParameter parameters[0];
    };

    struct RetinaTemplateMsg {
        RetinaTemplateOp operation;
        RetinaId retinaId;
        // Only used for UpdateTemplate
        RetinaUpdateInput retinaUpdateInput;
        // Only used for AddTemplate
        RetinaAddInput retinaAddInput;
    };

    // Static member
    static bool dagInited;
    static DagLib *dagLib;

    // Non static member
    DagHashTableById dagHTbyId_;
    Mutex dagTableIdLock_;
    StatGroupId statsGrpId_;
    StatHandle numDagNodesAlloced_;
    StatHandle numDagNodesFreed_;
    StatHandle dagNodeBytesPhys_;
    StatHandle dagNodeBytesVirt_;

    // Non static methods
    MustCheck Status createInternal();
    MustCheck bool isValidHeader(PersistedDagHeader *header);
    MustCheck bool isValidFooter(PersistedDagFooter *footer,
                                 PersistedDagHeader *header);

    // retina func
    MustCheck Status instantiateRetina(DgRetina *retina,
                                       unsigned numParameters,
                                       XcalarApiParameter *parameters,
                                       XcalarApiUdfContainer *udfContainer,
                                       Dag **instanceDagOut);
    MustCheck Status
    transplantRetinaDag(const XcalarApiExecuteRetinaInput *executeRetinaInput,
                        Dag *instanceDag);
    void removeParentNode(DagNodeTypes::Node *childNode,
                          DagNodeTypes::Node *parentNode);
    void destroyRetina(DgRetina *retina);

    // NOTE: If a retina imported from json (aka 'json-retina') is being
    // gotten, the UDF container must be passed in since a json-retina has no
    // UDFs of its own. Typically, this param is NULL since json-retina is rare
    // (typically for DF2.0 optimized execution).  This is needed so that the
    // parsing of the serialized dag in the retina's golden template, is
    // successful (parsing validates UDF references)

    MustCheck Status getRetinaInt(const RetinaId retinaId,
                                  XcalarApiUdfContainer *udfContainer,
                                  DgRetina **retinaOut);
    MustCheck Status
    getRetinaIntFromSerializedDag(const char *retinaStr,
                                  size_t retinaSize,
                                  char *retinaName,
                                  XcalarApiUdfContainer *sessionUdfContainer,
                                  DgRetina **retinaOut);

    void putRetinaInt(DgRetina *retina);
    // Caller must free *paramString
    static MustCheck Status makeDefaultExportParams(const char *exportName,
                                                    char **paramString);
    MustCheck Status appendExportNode(Dag *dag,
                                      unsigned numTables,
                                      RetinaDst **targetArray);
    MustCheck Status convertSynthesizeNodes(
        Dag *dstDag, Dag *srcDag, XcalarApiMakeRetinaInput *retinaInput);
    static MustCheck Status allocRetinaInfo(const char *retinaName,
                                            RetinaInfo **retinaInfoOut);
    static MustCheck Status populateTableArray(json_t *jsonRecord,
                                               const char *retinaName,
                                               RetinaInfo *retinaInfo);
    static MustCheck Status populateSchemaHints(json_t *jsonRecord,
                                                const char *retinaName,
                                                RetinaInfo *retinaInfo);
    static MustCheck Status populateRetinaInfo(json_t *retinaJson,
                                               const char *retinaName,
                                               RetinaInfo *retinaInfo,
                                               ArchiveManifest *manifest);
    MustCheck Status
    populateImportRetinaOutput(XcalarApiOutput *output,
                               size_t outputSize,
                               uint64_t numUdfModules,
                               XcalarApiOutput *uploadUdfOutputArray[],
                               size_t uploadUdfOutputSizeArray[]);
    MustCheck Status
    searchQueryGraph(Dag *queryGraph,
                     EvalUdfModuleSet *udfModules,
                     uint64_t *numUdfModules,
                     Dag::DagNodeListElt *exportNodesListAnchor,
                     Dag::DagNodeListElt *synthesizeNodesListAnchor,
                     Dag::DagNodeListElt *loadNodesListAnchor);
    MustCheck Status
    populateJsonSrcTables(const char *retinaName,
                          json_t *retinaInfoJson,
                          Dag::DagNodeListElt *synthesizeNodesListAnchor,
                          unsigned numSrcTables,
                          RetinaSrcTable *srcTables);
    MustCheck Status
    populateJsonTableArray(const char *retinaName,
                           json_t *retinaInfoJson,
                           Dag::DagNodeListElt *exportNodesListAnchor);
    MustCheck Status
    populateJsonSchemaHints(const char *retinaName,
                            json_t *retinaInfoJson,
                            Dag::DagNodeListElt *loadNodesListAnchor,
                            unsigned numColumnHints,
                            Column *columnHints);
    MustCheck Status populateJsonQueryStr(const char *retinaName,
                                          json_t *retinaInfoJson,
                                          DgRetina *retina);
    MustCheck Status addRetinaInfoJsonToManifest(const char *retinaName,
                                                 json_t *retinaInfoJson,
                                                 ArchiveManifest *manifest);
    void writeToFile(XcalarApiExportRetinaInput *exportRetinaInput,
                     XcalarApiExportRetinaOutput *exportRetinaOutput);
    MustCheck Status populateParamHashTables(DgRetina *retina);
    static void freeParameterHashEltByName(RefCount *refCount);
    MustCheck Status variableSubst(Dag *dag,
                                   unsigned numParameters,
                                   const XcalarApiParameter *parameters);
    MustCheck Status
    variableSubst(XcalarApiInput *input,
                  XcalarApis paramType,
                  uint64_t numParameters,
                  const XcalarApiParameter parameters[],
                  ParamHashTableByName *defaultParametersHashTable);
    MustCheck Status
    copyXcalarApiDagNodeToXcalarApiDagNode(XcalarApiDagNode *apiDagNodeOut,
                                           size_t bufSize,
                                           XcalarApiDagNode *apiDagNodeIn,
                                           size_t *bytesCopiedOut);
    MustCheck Status addRetinaTemplate(const RetinaId retinaId,
                                       const RetinaTemplate *retinaTemplate);
    MustCheck Status getRetinaTemplate(const RetinaId retinaId,
                                       RetinaTemplate **templateOut);
    MustCheck Status deleteRetinaTemplate(const RetinaId);
    MustCheck Status updateRetinaTemplate(
        const RetinaId, const RetinaUpdateInput *retinaUpdateInput);

    MustCheck Status
    addRetinaTemplateLocal(RetinaTemplateMsg *retinaTemplateMsg);
    MustCheck Status
    getRetinaTemplateLocal(void *payload,
                           RetinaTemplateReturnedPayload **payloadOut,
                           size_t *payloadOutSize);

    MustCheck Status
    updateRetinaTemplateLocal(RetinaTemplateMsg *retinaTemplateMsg);

    MustCheck Status
    deleteRetinaTemplateLocal(RetinaTemplateMsg *retinaTemplateMsg);

    MustCheck Status updateTemplateInHT(
        const RetinaId retinaId, const RetinaUpdateInput *retinaUpdateInput);

    MustCheck Status getUdfModulesFromRetina(char *retinaName,
                                             EvalUdfModuleSet *udfModules);
    void copyOpDetailsToXcalarApiLocalData(
        XcalarApiDagNodeLocalData *apiDataDst, OpDetails *opDetailsSrc);

    MustCheck Status
    executeRetinaInt(DgRetina *retina,
                     const XcalarApiUserId *userId,
                     XcalarApiExecuteRetinaInput *executeRetinaInput,
                     Dag **outputDag);

    MustCheck Status
    exportRetinaInt(DgRetina *retina,
                    XcalarApiExportRetinaInput *exportRetinaInput,
                    XcalarApiOutput **outputOut,
                    size_t *outputSizeOut);

    MustCheck Status
    allocMakeRetinaParam(XcalarApiMakeRetinaInput *makeRetinaInput,
                         size_t inputSize,
                         unsigned numRetinas,
                         RetinaInfo **retinaInfos,
                         MakeRetinaParam **makeRetinaParamOut,
                         size_t *makeRetinaParamSizeOut);

    MustCheck Status changeDatasetUserName(const char *oldDatasetName,
                                           const char *userName,
                                           char **newDatasetName);
    MustCheck Status getRetinaObj(const char *retinaName, DgRetina **retinaOut);

    MustCheck Status insertNodeToGlobalIdTable(DagNodeTypes::Node *node);
    MustCheck DagNodeTypes::Node *removeNodeFromGlobalIdTable(
        DagNodeTypes::Node *node);
    MustCheck Status lookupNodeById(DagTypes::NodeId nodeId);
};

// This class is used to publish globally the Retina name and associate it
// with a RetinaId which can be used to get the retina from the dlm node.
class RetinaNsObject final : public NsObject
{
  public:
    static constexpr const char *PathPrefix = "/retina/";

    // Public interfaces
    MustCheck RetinaId getRetinaId() { return retinaId_; }

    // Constructor / Destructor
    RetinaNsObject(RetinaId retinaId)
        : NsObject(sizeof(RetinaNsObject)), retinaId_(retinaId)
    {
    }
    ~RetinaNsObject() {}

  private:
    RetinaId retinaId_;

    // Disallow
    RetinaNsObject(const RetinaNsObject &) = delete;
    RetinaNsObject &operator=(const RetinaNsObject &) = delete;
};

struct PersistedRetinaImportInfo {
    char retinaName[XcalarApiMaxTableNameLen + 1];

    bool loadFromPersistedRetina;
    char persistedRetinaUrl[XcalarApiMaxUrlLen + 1];

    bool overwriteExistingUdf;
    ssize_t retinaCount;
    uint8_t retina[0];
};

struct RetinaExternal {
    uint64_t size;
    PersistedRetinaImportInfo retinaInput;
};

#endif  // _DAGLIB_H_
