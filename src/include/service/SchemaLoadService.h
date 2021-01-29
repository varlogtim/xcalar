#ifndef _SCHEMALOADSERVICE_H_
#define _SCHEMALOADSERVICE_H_
#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/SchemaLoad.xcrpc.h"
#include "xcalar/compute/localtypes/SchemaLoad.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace SchemaLoad
{
class SchemaLoadService : public ISchemaLoadService
{
  public:
    SchemaLoadService();
    virtual ~SchemaLoadService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status appRun(
        const xcalar::compute::localtypes::SchemaLoad::AppRequest *request,
        xcalar::compute::localtypes::SchemaLoad::AppResponse *response)
        override;

  private:
    static constexpr const char *ModuleName = "SchemaLoadService";
};
}
}
}
}

#endif
