#include "service/SchemaLoadService.h"
#include "usr/Users.h"

using namespace xcalar::compute::localtypes::SchemaLoad;

SchemaLoadService::SchemaLoadService() {}

ServiceAttributes
SchemaLoadService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return (sattr);
}

Status
SchemaLoadService::appRun(
    const xcalar::compute::localtypes::SchemaLoad::AppRequest *request,
    xcalar::compute::localtypes::SchemaLoad::AppResponse *response)
{
    Status status;

    status = AppLoader::schemaLoad(request, response);
    BailIfFailed(status);

CommonExit:
    return (status);
}
