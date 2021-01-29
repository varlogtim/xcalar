#include "service/DagNodeService.h"
#include "usr/Users.h"
#include "dag/Dag.h"
#include "SourceTypeEnum.h"

using namespace xcalar::compute::localtypes::DagNode;
using namespace xcalar::compute::localtypes::Workbook;

DagNodeService::DagNodeService() {}

ServiceAttributes
DagNodeService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    // Just scheduling of DF is on Immediate Runtime, but the actual query
    // execution is parameterized.
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
DagNodeService::pin(const DagNodeInputMsg *request,
                    google::protobuf::Empty *empty)
{
    Status status = StatusOk;
    Dag *sessionGraph = NULL;
    bool trackOpsToSession = false;
    UserMgr *usrMgr = UserMgr::get();
    WorkbookSpecifier workbookSpec = request->scope().workbook();
    status = usrMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
    BailIfFailed(status);
    trackOpsToSession = true;

    status = UserMgr::get()->getDag(&workbookSpec, &sessionGraph);
    BailIfFailed(status);

    status = sessionGraph->pinUnPinDagNode(request->dag_node_name().c_str(),
                                           Dag::Pin);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to pin dag node for \"%s\": %s",
                request->dag_node_name().c_str(),
                strGetFromStatus(status));
    }
CommonExit:
    if (trackOpsToSession) {
        Status status2 =
            usrMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Dec);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to decrement session op counter: %s",
                    strGetFromStatus(status2));
        }
    }
    return status;
}

Status
DagNodeService::unpin(const DagNodeInputMsg *request,
                      google::protobuf::Empty *empty)
{
    Status status = StatusOk;
    Dag *sessionGraph = NULL;
    bool trackOpsToSession = false;
    UserMgr *usrMgr = UserMgr::get();
    WorkbookSpecifier workbookSpec = request->scope().workbook();
    status = usrMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
    BailIfFailed(status);
    trackOpsToSession = true;

    status = UserMgr::get()->getDag(&workbookSpec, &sessionGraph);
    BailIfFailed(status);

    status = sessionGraph->pinUnPinDagNode(request->dag_node_name().c_str(),
                                           Dag::Unpin);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to unpin dag node for \"%s\": %s",
                request->dag_node_name().c_str(),
                strGetFromStatus(status));
    }
CommonExit:
    if (trackOpsToSession) {
        Status status2 =
            usrMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Dec);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to decrement session op counter: %s",
                    strGetFromStatus(status2));
        }
        return status;
    }
    return status;
}
