// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#ifndef _DAGTYPES_H_
#define _DAGTYPES_H_

#include "xdb/TableTypes.h"
#include "DagRefTypeEnums.h"

class Dag;

class DagTypes
{
  public:
    enum GraphType { InvalidGraph = 0, WorkspaceGraph, QueryGraph };

    enum { DeleteNodeCompletely = false, RemoveNodeFromNamespace = true };

    enum {
        MaxNameLen = 255,
        InvalidRetinaId = 0,
        InvalidDagNodeId = 0,
        DagIdInvalid = 0,
        CloneWholeDag = INT32_MAX,
        LeftTableIdx = 0,
        RightTableIdx = 1,
        DagMagic = 764279,
        MaxParentNum = 1024,
    };

    typedef Xid DagId;
    typedef Xid NodeId;

    enum SearchMode {
        IncludeNodeStateProcessing = false,
        IgnoreNodeStateProcessing = true,
    };

    enum DestroyOpts {
        DestroyNone = 0,
        DestroyDeleteNodes = 1,
        DestroyDeleteAndCleanNodes = 2,
    };

    enum DagIdToUse {
        // Use the same Dag node ID when creating the Dag node.  This is only
        // used in hydrating the golden retina template.
        ReuseDagId,
        UseNewDagId,
    };

    // To be kept in sync with DagTypes.thrift
    typedef char NodeName[MaxNameLen + 1];
    struct NamedInput {
        bool isTable;
        NodeName name;  // table name or dataset name
        NodeId nodeId;
        Xid xid;  // filled in by callee
    };

    struct DagRef {
        DagRefType type;
        char name[MaxNameLen + 1];
        Xid xid;
        DagRef *prev;
        DagRef *next;
    };

  protected:
    struct DagHdr {
        DagTypes::GraphType graphType;
        uint32_t padding;  // To make the struct a multiple of 8
        uint64_t numSlot;
        uint64_t numNodes;
        DagTypes::NodeId firstNode;
        DagTypes::NodeId lastNode;
    };

    struct CreateNewDagParams {
        DagId dagId;
        uint64_t numSlot;
        NodeId initNodeId;
        DagTypes::GraphType graphType;
    };

    struct AppendExportNodeInput {
        DagId dagId;
        NodeId newNodeId;
        char exportTableName[TableMaxNameLen + 1];
        NodeId parentNodeId;
        size_t apiInputSize;
        // Variable sized XcalarApiInput
        uint8_t buf[0];
    };

    struct PersistedDagHeader {
        uint64_t magic;
        uint64_t majVersion;
        uint64_t headerSize;
        uint64_t dataSize;
        uint64_t footerSize;
        uint8_t reserved[24];
    };

    struct PersistedDagFooter {
        uint64_t magic;
        uint64_t entrySize;  // hdr.headerSize + hdr.dataSize + hdr.footerSize
        uint8_t reserved[44];
        uint32_t checksum;  // must always be last
    };

    struct DagNodeOpInfo {
        DagId dagId;
        NodeId dagNodeId;
    };

    // I wish we could switch based on type of argument...
    enum DagNodeType {
        DagNodeTypeDagNode,
        DagNodeTypeXcalarApiDagNode,
    };

    enum {
        PersistedDagHeaderMagic = 0x98568724a2cull,
        PersistedDagFooterMagic = 0x93721474f4cull,
    };

    enum QueryGraphMajorVersion {
        QueryGraphMajorVersion1 = 1,
        QueryGraphMajorVersion2 = 2,
    };
};
#endif  // _DAGTYPES_H_
