// Copyright 2013-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DAG_H_
#define _DAG_H_

#include "dag/DagNodeTypes.h"
#include "dag/DagTypes.h"
#include "dag/DagNodeTypes.h"
#include "primitives/Primitives.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "table/TableNs.h"

namespace xcalar
{
namespace internal
{
namespace durable
{
namespace dag
{
class SerializedDagNodesContainer;
}
}
}
}

namespace xcalar
{
namespace internal
{
namespace durable
{
namespace dag
{
class SerializedDagNode;
}
}
}
}

class Dag : public DagTypes
{
    friend class DagNodeGvm;
    friend class DagLib;
    friend class DagDurable;
    friend class OperatorHandlerJoin;
    friend class Optimizer;
    friend class QueryManager;

  public:
    IntHashTableHook hook;
    enum CloneFlags {
        CloneFlagsNone = 0,
        ConvertNamesToImmediate = 1,
        ExpandRetina = 1 << 1,
        ResetState = 1 << 2,
        CopyAnnotations = 1 << 3,
        ConvertNamesFromImmediate = 1 << 4,
        ConvertUDFNamesToRelative = 1 << 5,
        CopyUDFs = 1 << 6,
    };
    enum PinApiType {
        Pin = 0,
        Unpin = 1,
    };

    enum class TableScope {
        FullyQualOnly,
        LocalOnly,
        FullyQualOrLocal,
    };

    struct OpDetailWrapper {
        OpDetails opDetails;
        Status status;
    };

    MustCheck Status init(void *constructorArgs);
    void markForDelete() { markForDelete_ = true; }
    MustCheck bool isMarkedForDeletion() { return markForDelete_; }
    void destroy();

    // XXX Todo Needs to be bubble up error code and the callers need to handle
    // the same.
    XdbId getXdbIdFromNodeId(DagTypes::NodeId id);

    MustCheck Status getTableIdFromNodeId(DagTypes::NodeId id,
                                          TableNsMgr::TableId *tableIdOut);
    MustCheck Status getNodeIdFromTableId(TableNsMgr::TableId tableId,
                                          DagTypes::NodeId *dagNodeIdOut);

    MustCheck Status setContext(DagTypes::NodeId id, void *context);
    MustCheck Status getContext(DagTypes::NodeId id, void **contextOut);
    MustCheck Status createNewDagNode(XcalarApis api,
                                      XcalarApiInput *apiInput,
                                      size_t apiInputSize,
                                      XdbId xdbId,
                                      TableNsMgr::TableId tableId,
                                      char *nameIn,
                                      uint64_t numParent,
                                      Dag **parentGraphs,
                                      DagTypes::NodeId *parentNodeIds,
                                      DagTypes::NodeId *dagNodeIdInOut);

    // deletes a dag node, but not the underlying entities it represents
    // (datasets, tables)
    MustCheck Status deleteDagNodeById(NodeId dagNodeId, bool dropOnly);
    MustCheck Status dropLastNodeAndReturnInfo(DagTypes::NodeId *dagNodeIdOut,
                                               XdbId *xdbIdOut,
                                               OpDetails *opDetailsOut);
    MustCheck Status getDagNodeRefById(NodeId dagNodeId);
    MustCheck Status readDagNodeRefById(NodeId dagNodeId, uint64_t *refCount);
    void putDagNodeRefById(DagTypes::NodeId dagNodeId);
    MustCheck Status getPutRef(bool isGet, NodeId dagNodeId);
    MustCheck Status getDagNodeStateAndRef(NodeId dagNodeId, DgDagState *state);
    Status changeDagNodeState(NodeId dagNodeId, DgDagState state);
    MustCheck Status dropAndChangeState(NodeId dagNodeId, DgDagState state);
    MustCheck Status getDagNodeState(NodeId dagNodeId, DgDagState *state);
    MustCheck Status getDagNodeSourceSize(NodeId dagNodeId, size_t *sizeOut);
    MustCheck Status renameDagNode(char *oldName, char *newName);
    MustCheck Status getDagNodeIds(const NodeName name,
                                   TableScope tScope,
                                   NodeId *dagNodeIdOut,
                                   XdbId *xdbIdOut,
                                   TableNsMgr::TableId *tableIdOut);
    MustCheck Status getDagNodeId(const NodeName name,
                                  TableScope tScope,
                                  NodeId *dagNodeIdOut);
    MustCheck Status getXdbId(const NodeName name,
                              TableScope tScope,
                              XdbId *xdbIdOut);
    MustCheck Status getDagNodeName(NodeId dagNodeId,
                                    char *nameOut,
                                    size_t size);
    MustCheck Status getDagByName(char *dagName,
                                  TableScope tScope,
                                  XcalarApiOutput **outputOut,
                                  size_t *outputSizeOut);
    MustCheck Status getDagById(NodeId dagNodeId,
                                XcalarApiOutput **outputOut,
                                size_t *outputSizeOut);
    // bulk getDagNode()
    MustCheck Status listDagNodeInfo(const char *namePattern,
                                     XcalarApiOutput **outputOut,
                                     size_t *outputSizeOut,
                                     SourceType srcType,
                                     XcalarApiUserId *user);
    MustCheck Status copyDagNodeOpStatus(Dag *srcDag,
                                         DagTypes::NodeId srcNodeId,
                                         DagTypes::NodeId dstNodeId);
    MustCheck Status copyOpDetails(Dag *dstDag);
    void copyCommentsAndTags(Dag *dstDag);
    MustCheck Status listAvailableNodes(const char *dagNamePattern,
                                        XcalarApiDagOutput **listDagsOut,
                                        size_t *outputSizeOut,
                                        unsigned numApis,
                                        XcalarApis *apiArray);
    MustCheck Status setScalarResult(NodeId dagNodeId, Scalar *scalarIn);
    MustCheck Status getScalarResult(NodeId dagNodeId, Scalar **scalarOut);
    MustCheck Status getDagNodeApi(NodeId dagNodeId, XcalarApis *api);
    MustCheck Status lookupNodeById(DagTypes::NodeId id,
                                    DagNodeTypes::Node **nodeOut);
    MustCheck Status lookupNodesByXdbId(DagTypes::NodeId id,
                                        DagNodeTypes::Node **&nodesOut,
                                        unsigned &numNodes);
    MustCheck Status lookupNodeByTableId(TableNsMgr::TableId id,
                                         DagNodeTypes::Node **nodeOut);
    MustCheck DagNodeTypes::Node *getLastNode();
    MustCheck Status getDagNode(NodeId dagNodeId,
                                XcalarApiDagNode **apiDagNodeOut);
    MustCheck Status getRootNodeIds(NodeId *srcNodeArrayOut[],
                                    uint64_t *numSrcNodesOut);

    MustCheck Status getChildDagNodeId(NodeId dagNodeId,
                                       NodeId **childNodesArrayOut,
                                       uint64_t *numChildOut);

    // XXX can be deleted; just used for XcalarApiGetTableRefCount
    MustCheck Status getChildDagNode(char *dagName,
                                     XcalarApiDagOutput **output,
                                     size_t *outputSize);

    // used by xcApiConstructApiInputFromDagNode()
    MustCheck Status getParentDagNode(NodeId dagNodeId,
                                      XcalarApiDagOutput **output,
                                      size_t *outputSizeOut);
    MustCheck Status getParentDagNodeId(NodeId dagNodeId,
                                        NodeId **parentNodesArrayOut,
                                        uint64_t *numParentOut);
    MustCheck Status getPerNodeOpStats(DagTypes::NodeId dagNodeId,
                                       XcalarApiOutput **outputOut,
                                       size_t *outputSizeOut);
    MustCheck Status getOpStats(NodeId dagNodeId,
                                DgDagState state,
                                XcalarApiOutput **outputOut,
                                size_t *outputSizeOut);
    MustCheck Status getOpProgress(NodeId dagNodeId,
                                   uint64_t &numWorkCompleted,
                                   uint64_t &numWorkTotal);
    MustCheck Status getOpStatus(NodeId dagNodeId, OpStatus **statusOut);

    MustCheck Status getOpStatusFromXdbId(XdbId xdbId, OpStatus **statusOut);

    // drops the underlying entity backed by a DAG node and alter the DAG
    // node's state to DgDagS

    MustCheck Status dropNode(const char *targetName,
                              SourceType srcType,
                              uint64_t *numRefsOut,
                              DagTypes::DagRef **refsOut,
                              bool deleteCompletely = false);

    // archives the underlying entity, and alters the dag node state
    MustCheck Status archiveNode(const char *nodeName);
    MustCheck Status unarchiveNode(const char *nodeName);

    MustCheck Status bulkDropNodes(const char *namePattern,
                                   XcalarApiOutput **output,
                                   size_t *outputSizeOut,
                                   SourceType srcType,
                                   XcalarApiUserId *user,
                                   bool deleteCompletely = false);

    MustCheck Status setOpDetails(NodeId dagNodeId, OpDetails *opDetails);
    MustCheck Status
    updateOpDetails(NodeId dagNodeId,
                    NodeId *failureTableId[XcalarApiMaxFailureEvals],
                    XcalarApiUserId *userId,
                    XcalarApiSessionInfoInput *sessInfo);
    MustCheck Status cancelOp(char *name);
    MustCheck Status getDatasetIdFromDagNodeId(NodeId dagNodeId,
                                               DsDatasetId *datasetIdOut);

    MustCheck Status convertNamesToImmediate();
    MustCheck Status changeAllNodesState(DgDagState state);
    MustCheck Status cloneDag(Dag **dagOut,
                              DagTypes::GraphType dstType,
                              // new dag's udf container must be supplied
                              XcalarApiUdfContainer *udfContainer,
                              uint64_t numTargetNodes,
                              NodeName *targetNodesNameArray,
                              unsigned numPrunedNodes,
                              NodeName *prunedNodeNames,
                              CloneFlags flags);

    // Get the dag and clone it atomically
    MustCheck Status getAndCloneDag(Dag **dagOut,
                                    DagTypes::GraphType srcGraphType,
                                    DagTypes::GraphType dstType,
                                    CloneFlags flags,
                                    DagTypes::SearchMode ignoreProcessing);

    // used by rebuildDag() to reconstitute the underlying entities (datasets,
    // tables, etc) pointed to by a DAG
    MustCheck Status getFirstDagInOrder(NodeId *dagNodeIdOut);

    MustCheck Status getNextDagInOrder(NodeId dagNodeIdIn,
                                       NodeId *dagNodeIdOut);

    // Sometimes we want to include nodes that are in the state
    // DgDagStateProcessing, sometimes we don't. Use ignoreProcessing
    // to indicate intent
    MustCheck Status getActiveDagNodes(GraphType srcGraphType,
                                       NodeName *activeNodeNamesOut[],
                                       NodeId *activenodeIdsOut[],
                                       uint64_t *numActiveNodesOut,
                                       SearchMode ignoreProcessing);

    MustCheck Status updateDagNodeParam(NodeId dagNodeId,
                                        XcalarApiParamInput *paramInput);
    MustCheck Status
    copyDagNodeToNewDagNode(const DagNodeTypes::DagNodeHdr *dagNodeHdr,
                            DagTypes::NodeId dagNodeId,
                            uint64_t numParent,
                            DagTypes::NodeId *parentNodeIds,
                            Xid xdbId,
                            TableNsMgr::TableId tableId,
                            DgDagState state);
    MustCheck Status renameDagNodeLocalEx(char *oldName, const char *newName);
    MustCheck Status tagDagNodeLocalEx(NodeId nodeId, const char *tag);
    MustCheck Status tagDagNodeLocalEx(const char *dagNodeName,
                                       const char *tag,
                                       bool needsLock = false);
    MustCheck Status commentDagNodeLocalEx(const char *dagNodeName,
                                           const char *comment,
                                           bool needsLock = false);
    void removeAndDestroyTransitDagNodes();
    MustCheck Status appendExportDagNode(DagTypes::NodeId newDagNodeId,
                                         char *exportTableName,
                                         DagTypes::NodeId parentNodeId,
                                         size_t apiInputSize,
                                         XcalarApiInput *apiInput);
    MustCheck Status convertLastExportNode();
    MustCheck Status convertUdfsToAbsolute(XcalarApis api,
                                           XcalarApiInput *input);

    MustCheck Status renameDagNodeLocal(char *oldName, const char *newName);
    MustCheck Status tagDagNodes(char *tag,
                                 unsigned numNodes,
                                 DagTypes::NamedInput *nodes);

    MustCheck Status
    commentDagNodes(char *comment,
                    unsigned numNodes,
                    char nodeNames[0][XcalarApiMaxTableNameLen]);
    MustCheck Status pinUnPinDagNode(const char *nodeName, PinApiType reqType);
    MustCheck Status pinUnPinDagNodeLocalEx(const char *nodeName,
                                            PinApiType reqType);
    MustCheck Status isDagNodePinned(DagTypes::NodeId id, bool *isPinnedRet);
    MustCheck DagTypes::GraphType getDagType();
    Status deleteQueryOperationNode(DagNodeTypes::Node *dagNode);

    MustCheck Status lookupNodeByName(const NodeName name,
                                      DagNodeTypes::Node **nodeOut,
                                      TableScope tScope,
                                      bool needsLock = false);
    Status getRetinas(unsigned numTargets,
                      DagTypes::NodeName *targetNodeNames,
                      unsigned *numRetinasOut,
                      RetinaInfo ***retinaInfoOut);
    MustCheck Status checkBeforeDrop(DagNodeTypes::Node *dagNode);
    // inline func
    MustCheck bool isOnOwnerNode();
    void lock();
    void unlock();
    MustCheck uint64_t getNumNode();
    MustCheck DagId getId() const;
    MustCheck NodeId getFirstNodeIdInOrder();
    MustCheck DagNodeTypes::Node *getFirstNode();
    MustCheck bool inSession();
    void initContainer(XcalarApiUdfContainer *container);
    void initUdfContainer(XcalarApiUdfContainer *udfContainer);
    void initSessionContainer(XcalarApiUdfContainer *sessionContainer);
    MustCheck XcalarApiUdfContainer *getUdfContainer();
    MustCheck XcalarApiUdfContainer *getSessionContainer();

    // static inline func
    static MustCheck size_t sizeOfDagOutputHdr(uint64_t numNodes);
    static MustCheck size_t sizeOfDagOutput(uint64_t numNodes,
                                            size_t totalApiInputSize,
                                            size_t totalParentSize,
                                            size_t totalChildrenSize);
    static MustCheck size_t sizeOfDagOutput(uint64_t numNodes,
                                            size_t totalVariableBufSize);

    struct SerializedHdr {
        DagTypes::GraphType graphType;
        uint32_t padding;  // To make the struct a multiple of 8
        uint64_t numSlot;  // dead field!
        ssize_t numNodes;
        DagTypes::NodeId firstNode;
        DagTypes::NodeId lastNode;
    };

  private:
    enum {
        DgUpdateCreateNode,
        DgUpdateDeleteNode,
        DgUpdateRenameNode,
        DgUpdateOpDetails,
        DgSetScalarResult,
        DgRemoveNodeFromNs,
    };

    enum {
        ClonedNodeSlotNum = 193,
        DagNodeIdBfsSlotNum = 31,
    };

    struct ClonedNodeElt {
        NodeId originalNodeId;
        NodeId clonedNodeId;
        struct ClonedNodeElt *next;
    };

    struct SerializedDagNode {
        DagNodeTypes::DagNodeHdr dagNodeHdr;
        ssize_t numParent;
        // This input is not used in the non-autogenerated code, it's here to
        // cause the code generator to generate fields and methods for it.
        XcalarApiInput __inputStub;
        NodeId parentsList[0];
    };

    struct SerializedDagNodesContainer {
        uint64_t magic;
        SerializedHdr containerHdr;
        SerializedDagNode dagNodes[0];
    };

    // Gvm parameters
    struct CreateDagNodeInput {
        NodeId dagNodeId;
        DagId dagId;
        XdbId xdbId;
        TableNsMgr::TableId tableId;
        XcalarApis api;
        char name[MaxNameLen + 1];
        size_t bufSize;
        uint64_t numParent;
        size_t apiInputSize;
        // Pointers to be fixed up at dst
        NodeId *parentNodeIds;
        DagTypes::DagId *parentGraphIds;
        XcalarApiInput *input;
        uint8_t buf[0];
    };

    struct RemoveNodeFromNsInput {
        DagId dagId;
        NodeId dagNodeId;
    };

    struct DagNodeListElt {
        NodeId getId() const
        {
            return node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        }

        DagNodeTypes::Node *node;
        IntHashTableHook hook;
        struct DagNodeListElt *prev;
        struct DagNodeListElt *next;
    };

    struct UpdateOpDetailsInput {
        OpDetails opDetails;
        DagId dagId;
        NodeId dagNodeId;
    };

    struct ChangeStateInput {
        DagId dagId;
        NodeId dagNodeId;
        DgDagState state;
    };

    struct DeleteDagNodeInput {
        DagId dagId;
        NodeId dagNodeId;
        bool dropOnly;
    };

    struct RenameDagNodeInput {
        DagId dagId;
        char oldName[MaxNameLen];
        char newName[MaxNameLen];
    };

    struct GetPutRefInput {
        bool isGet;
        DagId dagId;
        NodeId dagNodeId;
    };

    struct SetScalarResultInput {
        DagId dagId;
        NodeId dagNodeId;
        Scalar scalarIn[0];
    };

    struct RetinaListElt {
        NodeId executeRetinaNodeId;
        // XXX: this is a DagLib::DgRetina, but there is a circular header
        // dependecy. Need to restructure dag/retina library
        void *retina;

        RetinaListElt *next = NULL;
        RetinaListElt *prev = NULL;
    };

    DagId dagId_;
    SerializedHdr hdr_;

    static constexpr unsigned IdHashTableSlots = 13;
    typedef IntHashTable<DagTypes::NodeId,
                         DagNodeTypes::Node,
                         &DagNodeTypes::Node::idHook,
                         &DagNodeTypes::Node::getId,
                         IdHashTableSlots,
                         hashIdentity>
        IdHashTable;
    IdHashTable idHashTable_;

    // XXX Todo
    // Note that DagNode Name is not synonymous with Table name anymore.
    // * We need to track DagNode Names for query parsing only. Eventually when
    // we will have our dedicated Dag library, we can clearly templatize this,
    // so we don't mix query parsing and query execution logic.
    // * Query execution does not need DagNode names. Table name is necessary
    // and sufficient to find a DagNode here.
    static constexpr unsigned NameHashTableSlots = 13;
    typedef StringHashTable<DagNodeTypes::Node,
                            &DagNodeTypes::Node::nameHook,
                            &DagNodeTypes::Node::getName,
                            NameHashTableSlots>
        NameHashTable;
    NameHashTable nameHashTable_;

    static constexpr unsigned XdbIdHashTableSlots = 13;
    typedef IntHashTable<Xid,
                         DagNodeTypes::Node,
                         &DagNodeTypes::Node::xdbIdHook,
                         &DagNodeTypes::Node::getXdbId,
                         XdbIdHashTableSlots,
                         hashIdentity>
        XdbIdHashTable;
    XdbIdHashTable xdbIdHashTable_;

    static constexpr unsigned TableIdHashTableSlots = 13;
    typedef IntHashTable<TableNsMgr::TableId,
                         DagNodeTypes::Node,
                         &DagNodeTypes::Node::tableIdHook,
                         &DagNodeTypes::Node::getTableId,
                         TableIdHashTableSlots,
                         hashIdentity>
        TableIdHashTable;
    TableIdHashTable tableIdHashTable_;

    bool ownerNode_;
    // The lock_() is a simple fat lock on the DAG instance, it should be held
    // when modifying the internal state of the DAG instance. lock_() should not
    // be held across twoPc().
    Mutex lock_;

    // The globalLock is held across the twoPc(). It is used to serialize
    // dagNode creation and deletion, so that if two thread tries to create two
    // DAG node with the same name but different XID, this lock guarantee only
    // one will succeed. This lock will not be needed when DAG module got rid of
    // the functionality of managing namespace.
    Mutex globalLock_;

    // A Dag could belong to a workbook, in which case the udfContainer_ field
    // must be non-NULL and point to a container which has information about the
    // user and workbook - this is used to parse eval strings in the DAG to look
    // up UDFs in the eval string (the UDFs must belong to some container).
    XcalarApiUdfContainer udfContainer_;

    // Defines the scope of the Dag's namespace
    XcalarApiUdfContainer sessionContainer_;

    // Used to indicate the entire Dag is marked for deletion.
    bool markForDelete_ = false;

    MustCheck Status lookupClonedNode(ClonedNodeElt **clonedNodeHashBase,
                                      NodeId nodeIdIn,
                                      NodeId *nodeIdOut);
    MustCheck Status insertClonedNode(ClonedNodeElt **clonedNodeHashBase,
                                      NodeId originalNodeId,
                                      NodeId clonedNodeId);
    void deleteClonedNodeHashTable(ClonedNodeElt **clonedNodeHashBase);
    MustCheck Status isValidNodeName(const char *name, size_t *nameLenOut);
    ClonedNodeElt **allocateCloneNodeHashBase();
    MustCheck Status lookupDroppedNodeByName(const NodeName name,
                                             DagNodeTypes::Node **nodeOut);
    MustCheck Status createSingleNode(XcalarApis api,
                                      const XcalarApiInput *apiInput,
                                      size_t apiInputSize,
                                      XdbId xdbId,
                                      TableNsMgr::TableId tableId,
                                      const char *name,
                                      NodeId dagNodeId,
                                      DagNodeTypes::Node **dagNodeOut);
    MustCheck Status addChildNode(DagNodeTypes::Node *parentNode,
                                  DagNodeTypes::Node *childNode);
    void removeAndCleanChildNode(DagNodeTypes::Node *parentNode,
                                 DagNodeTypes::Node *childNode);
    void removeAndDeleteDagNode(DagNodeTypes::Node *dagNode);
    void removeDagNode(DagNodeTypes::Node *dagNode);
    void removeNodeFromIdList(DagNodeTypes::NodeIdListElt **listEltHead,
                              DagNodeTypes::Node *toRemove);
    void removeNodeFromIdTable(DagNodeTypes::Node *node);
    void removeNodeFromGlobalIdTable(DagNodeTypes::Node *node);
    void removeNodeFromXdbIdTable(DagNodeTypes::Node *node);
    void removeNodeFromTableIdTable(DagNodeTypes::Node *node);
    void addNodeToDag(DagNodeTypes::Node *node);

    void insertNodeToXdbIdTable(DagNodeTypes::Node *node);
    void insertNodeToIdTable(DagNodeTypes::Node *node);
    void insertNodeToGlobalIdTable(DagNodeTypes::Node *node);
    void insertNodeToNameTable(DagNodeTypes::Node *node);
    void insertNodeToTableIdTable(DagNodeTypes::Node *node);
    void serializeCreateDagNodeInput(CreateDagNodeInput *createDagNodeInput,
                                     size_t bufSize,
                                     uint64_t numParents,
                                     NodeId *parentNodeId,
                                     DagTypes::DagId *parentGraphIds,
                                     XcalarApiInput *apiInput,
                                     size_t apiInputSize);
    MustCheck Status copyDagToNewDag(ClonedNodeElt **clonedNodeHashBase,
                                     DagNodeTypes::Node **dagNodesIn,
                                     unsigned numNodesIn,
                                     Dag *dagOut,
                                     unsigned numPrunedNodes,
                                     DagTypes::NodeId *prunedNodeIds,
                                     CloneFlags flags,
                                     uint64_t *numNodesCreated);
    MustCheck Status getDagAncestorTree(DagNodeTypes::Node **dagNodesIn,
                                        unsigned numNodesIn,
                                        unsigned numPrunedNodes,
                                        DagTypes::NodeId *prunedNodeIds,
                                        Dag *parentDag,
                                        DagNodeListElt **resultListHeadOut,
                                        DagNodeListElt **resultListTailOut,
                                        uint64_t *numNodesOut);
    MustCheck Status checkBeforeDelete(DagNodeTypes::Node *dagNode);

    MustCheck Status checkDescendantDatasetRef(DagNodeTypes::Node *dagNode,
                                               DsDatasetId datasetId,
                                               bool *hashRef);
    MustCheck Status changeDagStateLocalNoLock(NodeId dagNodeId,
                                               DgDagState state);
    MustCheck Status gatherOpStatus(NodeId dagNodeId,
                                    OpDetailWrapper *opDetailsWrapperArray,
                                    uint64_t numOpDetails);
    void processOpErrors(OpErrorStats *outAcc,
                         OpErrorStats *in,
                         XcalarApis api);
    void processOpStatuses(OpDetailWrapper *opDetailsWrapperArray,
                           unsigned numOpDetails,
                           OpDetails *out,
                           XcalarApis api);
    MustCheck size_t dgGetSourceSize(SourceType srcType,
                                     XcalarApiDagNode *dagNode,
                                     XcalarApiUserId *user);
    MustCheck Status dgGetDag(DagNodeTypes::Node *dagNode,
                              XcalarApiOutput **outputOut,
                              size_t *outputSizeOut);
    void removeDagNodeLocal(DagNodeTypes::Node *dagNode);
    void removeNodeFromNameTable(DagNodeTypes::Node *node);
    MustCheck bool foundApi(XcalarApis api,
                            unsigned numApis,
                            XcalarApis *apiArray);

    // deletes a dag node AND the underlying entity it represents
    // returns dag node deletion status directly
    // returns underlying entity deletion status via status param
    MustCheck Status cleanAndDeleteDagNode(const char *targetName,
                                           SourceType srcType,
                                           bool dropOnly,
                                           uint64_t *numRefsOut,
                                           DagTypes::DagRef **refsOut);

    MustCheck Status deleteDagNodeLocal(NodeId dagNodeId, bool dropOnly);
    MustCheck Status createNewDagNodeCommon(DagTypes::NodeId dagNodeId,
                                            const char *name,
                                            Xid xdbId,
                                            TableNsMgr::TableId tableId,
                                            XcalarApis api,
                                            size_t apiInputSize,
                                            XcalarApiInput *apiInput,
                                            uint64_t numParent,
                                            DagTypes::NodeId *parentNodeIds,
                                            DagTypes::DagId *parentGraphIds,
                                            DgDagState state,
                                            const OpDetails *opDetails,
                                            DagNodeTypes::Node **newNodeOut);

    MustCheck Status
    createNewDagNodeLocal(CreateDagNodeInput *createDagNodeInput);

    MustCheck Status
    updateOpDetailsOneFailSummTab(XcalarApis api,
                                  DagTypes::NodeId dagNodeId,
                                  UpdateOpDetailsInput *input,
                                  DagTypes::NodeId failureTabId,
                                  uint32_t tabNum,
                                  XcalarApiUserId *userId,
                                  XcalarApiSessionInfoInput *sessInfo);

    MustCheck Status updateOpDetailsFailureSummary(
        XcalarApis api,
        DagTypes::NodeId dagNodeId,
        UpdateOpDetailsInput *input,
        DagTypes::NodeId *failureTabId[XcalarApiMaxFailureEvals],
        XcalarApiUserId *userId,
        XcalarApiSessionInfoInput *sessInfo);

    // static func
    static MustCheck Status updateOpDetailsLocal(void *payload);
    static MustCheck Status dagSetScalarResultLocal(void *payload);
    static void destroyDagNodeFromRef(RefCount *refCountIn);
    static void deserializeCreateDagNodeInput(
        CreateDagNodeInput *createDagNodeInput);
    static void preArtemisUpdateInputSize(
        xcalar::internal::durable::dag::SerializedDagNode *pb,
        SerializedDagNode *nodeToLoad);
    static MustCheck Status
    preArtemisDeserUnion(xcalar::internal::durable::dag::SerializedDagNode *pb,
                         const char *idlSha,
                         SerializedDagNode *nodeToLoad,
                         bool *apiDeserialized);
    static MustCheck Status updateLocalHandler(uint32_t userAction,
                                               const void *payload);
    // inline func
    // Does not include size for apiInput
    MustCheck size_t sizeOfSerializedNodeHdr(SerializedDagNode *node,
                                             uint64_t numParent);
    MustCheck size_t sizeOfSerializedNode(SerializedDagNode *node,
                                          uint64_t numParent);
    static MustCheck Status sparseMemCpy(void *dstIn,
                                         const void *srcIn,
                                         size_t size,
                                         size_t blkSize = PageSize);
    static MustCheck XcalarApiInput *dagApiAlloc(const size_t size);
    static void dagApiFree(DagNodeTypes::Node *dagNode);

    // static inline
    static MustCheck size_t sizeOfCreateDagNodeInput(uint64_t numParent,
                                                     size_t totalApiInputSize);
    MustCheck bool isNodeActive(DagNodeTypes::Node *node,
                                DagTypes::GraphType srcGraphType,
                                DagTypes::SearchMode ignoreProcessing);
    MustCheck Status destroyDagNodes(DestroyOpts destroyOpts);

    MustCheck Status
    cloneDagInt(Dag **dagOut,
                DagTypes::GraphType dstType,
                // new dag's udf container must be supplied below
                XcalarApiUdfContainer *udfContainer,
                uint64_t numTargetNodes,
                DagTypes::NodeName *targetNodesNameArray,
                uint64_t numPrunedNodes,
                DagTypes::NodeName *prunedNodeNames,
                CloneFlags flags);

    MustCheck Status
    getActiveDagNodesInt(DagTypes::GraphType srcGraphType,
                         DagTypes::NodeName *activeNodeNamesOut[],
                         DagTypes::NodeId *activeNodeIdsOut[],
                         uint64_t *numActiveNodesOut,
                         DagTypes::SearchMode ignoreProcessing,
                         bool takeLock);

    static MustCheck DagNodeTypes::Node *allocDagNode();
    static void freeDagNode(DagNodeTypes::Node *dagNode);

    Dag() {}
    ~Dag();
    Dag(const Dag &) = delete;
    Dag &operator=(const Dag &) = delete;
};

class TwoPcMsg2pcDlmRetinaTemplate1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDlmRetinaTemplate1() {}
    virtual ~TwoPcMsg2pcDlmRetinaTemplate1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDlmRetinaTemplate1(const TwoPcMsg2pcDlmRetinaTemplate1 &) =
        delete;
    TwoPcMsg2pcDlmRetinaTemplate1(const TwoPcMsg2pcDlmRetinaTemplate1 &&) =
        delete;
    TwoPcMsg2pcDlmRetinaTemplate1 &operator=(
        const TwoPcMsg2pcDlmRetinaTemplate1 &) = delete;
    TwoPcMsg2pcDlmRetinaTemplate1 &operator=(
        const TwoPcMsg2pcDlmRetinaTemplate1 &&) = delete;
};

// Old struct definitions needed to carry out manual deserialization, for the
// scenario in Xc-10937 - upgrade of Join/Union/Synthesize operator API structs
// from Xcalar versions pre-Artemis to Artemis+. Assumptions being made:
// 0. All pre-Artemis releases had the layout for Union API input, and
//    XcalarApiRenameMap, as in preArtemisXcalarApiRenameMap and
//    preArtemisXcalarApiUnionInput defined below
// 1. The data layout in the bytes array (to which "buf" points) is as follows:
//     a. XcalarApiTableInput srcTables[]
//     b. unsigned renameMapSizes[]
//     c. XcalarApiRenameMap *renameMap[]
//     d. numSrcTables count of "XcalarApiRenameMap renameMap[]" arrays
//        (size of each renameMap[] array is obtained from renameMapSizes[])
// 2. The size occupied by a,b,c above is unchanged (pre-Artemis to Artemis+)
//
// NOTE: These are defined here rather than in LibApisCommon.h, since these
// definitions are needed only for deserialize/upgrade, are essentially frozen,
// and it seems better to leave LibApisCommon.h clean, containing just the
// current definitions.

struct preArtemisXcalarApiRenameMap {
    char oldName[DfMaxFieldNameLen + 1];
    char newName[DfMaxFieldNameLen + 1];
    DfFieldType type;
};

struct preArtemisXcalarApiUnionInput {
    XcalarApiTableInput *srcTables;
    unsigned numSrcTables;
    XcalarApiTableInput dstTable;

    unsigned *renameMapSizes;
    preArtemisXcalarApiRenameMap **renameMap;

    bool dedup;
    int64_t bufCount;
    uint8_t buf[0];
};

// Any further increase in the size of this struct will have a
// ripple effect on the deserialization of workbooks containing
// Join, Synthesize, or Union, which would need to handle upgrade
// from both pre-Artemis and Artemis workbooks, instead of just
// pre-Artemis workbooks.  Hence the size for this struct is being
// locked down to its size in Artemis (520 bytes) via the assert below.
static_assert((sizeof(XcalarApiRenameMap) == 520),
              "size of XcalarApiRenameMap must not change! See Xc-10937");

#endif  // _DAG_H_
