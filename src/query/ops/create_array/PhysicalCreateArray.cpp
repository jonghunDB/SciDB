/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2018 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <array/Metadata.h>
#include <boost/array.hpp>
#include <system/SystemCatalog.h>
#include <query/Operator.h>
#include <log4cxx/logger.h>
#include <array/TransientCache.h>
#include <rbac/Session.h>

using namespace std;

/****************************************************************************/
namespace scidb {
/****************************************************************************/
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physcial_create_array"));

struct PhysicalCreateArray : PhysicalOperator
{
    PhysicalCreateArray(const string& logicalName,
                        const string& physicalName,
                        const Parameters& parameters,
                        const ArrayDesc& schema)
     : PhysicalOperator(logicalName,physicalName,parameters,schema)
    {}

    virtual std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >& in,std::shared_ptr<Query> query)
    {
        bool const temp(
            param<OperatorParamPhysicalExpression>(2)->
                getExpression()->evaluate().getBool());

        if (query->isCoordinator())
        {
            string const& objName = param<OperatorParamArrayReference>(0)->getObjectName();

            std::string arrayName;
            std::string namespaceName;
            query->getNamespaceArrayNames(objName, namespaceName, arrayName);
            SCIDB_ASSERT(ArrayDesc::isNameUnversioned(arrayName));

            ArrayDesc arrSchema(param<OperatorParamSchema>(1)->getSchema());

            arrSchema.setName(arrayName);
            arrSchema.setNamespaceName(namespaceName);
            arrSchema.setTransient(temp);

         /* Give our subclass a chance to compute missing dimension details
            such as a wild-carded chunk interval, for example...*/

            const size_t redundancy = Config::getInstance()->getOption<size_t> (CONFIG_REDUNDANCY);
            arrSchema.setDistribution(defaultPartitioning(redundancy));
            arrSchema.setResidency(query->getDefaultArrayResidencyForWrite());
            ArrayID uAId = SystemCatalog::getInstance()->getNextArrayId();
            arrSchema.setIds(uAId, uAId, VersionID(0));
            if (!temp) {
                query->setAutoCommit();
            }

            LOG4CXX_TRACE(logger, "PhysicalCreateArray::execute("
                << "namespaceName=" << namespaceName
                << ",arrayName=" << arrSchema.getName() << ")");

            SystemCatalog::getInstance()->addArray(arrSchema);
        }

        if (temp)                                        // 'temp' flag given?
        {
            syncBarrier(0,query);                        // Workers wait here

            string const& arrayName = param<OperatorParamArrayReference>(0)->getObjectName();

            // XXX TODO: this needs to change to eliminate worker catalog access
            ArrayDesc arrSchema;
            SystemCatalog::GetArrayDescArgs args;
            args.result = &arrSchema;
            ArrayDesc::splitQualifiedArrayName(arrayName, args.nsName, args.arrayName);
            if (args.nsName.empty()) {
                args.nsName = query->getNamespaceName();
            }
            args.throwIfNotFound = true;
            SystemCatalog::getInstance()->getArrayDesc(args);

            transient::record(make_shared<MemArray>(arrSchema,query));
        }

        return std::shared_ptr<Array>();
    }

    template<class t>
    std::shared_ptr<t>& param(size_t i) const
    {
        assert(i < _parameters.size());

        return (std::shared_ptr<t>&)_parameters[i];
    }
};

/****************************************************************************/

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArray,     "create_array",      "impl_create_array")

/****************************************************************************/
}
/****************************************************************************/
