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

/*
 * LogicalRename.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <rbac/Rights.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logical_rename"));
}

namespace scidb
{

/**
 * @brief The operator: rename().
 *
 * @par Synopsis:
 *   rename( oldArray, newArray )
 *
 * @par Summary:
 *   Changes the name of an array.
 *
 * @par Input:
 *   - oldArray: an existing array.
 *   - newArray: the new name of the array.
 *
 * @par Output array:
 *   NULL
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalRename: public LogicalOperator
{
public:
    LogicalRename(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.exclusive = true;
        _properties.ddl = true;
        ADD_PARAM_IN_ARRAY_NAME();
        ADD_PARAM_OUT_ARRAY_NAME();
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query) override
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() == 2);
        assert(((std::shared_ptr<OperatorParam>&)_parameters[0])->getParamType() == PARAM_ARRAY_REF);
        assert(((std::shared_ptr<OperatorParam>&)_parameters[1])->getParamType() == PARAM_ARRAY_REF);

        std::string oldArrayName;
        std::string oldNamespaceName;
        std::string newArrayName;
        std::string newNamespaceName;

        const string &oldArrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        query->getNamespaceArrayNames(oldArrayNameOrig, oldNamespaceName, oldArrayName);

        const string &newArrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();
        query->getNamespaceArrayNames(newArrayNameOrig, newNamespaceName, newArrayName);

        if(newNamespaceName != oldNamespaceName)
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CANNOT_RENAME_ACROSS_NAMESPACES,
                _parameters[1]->getParsingContext())
                << ArrayDesc::makeQualifiedArrayName(oldNamespaceName, oldArrayName)
                << ArrayDesc::makeQualifiedArrayName(newNamespaceName, newArrayName);
        }

        bool found = SystemCatalog::getInstance()->containsArray(newNamespaceName, newArrayName);
        if (found)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_ALREADY_EXIST,
                _parameters[1]->getParsingContext()) << newArrayName;
        }
        ArrayDesc arrDesc;
        arrDesc.setDistribution(defaultPartitioning());
        arrDesc.setResidency(query->getDefaultArrayResidency());
        return arrDesc;
    }


    void inferAccess(std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);
        SCIDB_ASSERT(_parameters.size() > 1);

        // from
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& oldArrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(oldArrayNameOrig));

        std::string oldArrayName;
        std::string oldNamespaceName;
        query->getNamespaceArrayNames(oldArrayNameOrig, oldNamespaceName, oldArrayName);

        std::shared_ptr<LockDesc> lock(
            new LockDesc(
                oldNamespaceName,
                oldArrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                LockDesc::RNF));
        std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::RNF);

        // to
        SCIDB_ASSERT(_parameters[1]->getParamType() == PARAM_ARRAY_REF);
        const string &newArrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();
        SCIDB_ASSERT(!ArrayDesc::isNameVersioned(newArrayNameOrig));

        std::string newArrayName;
        std::string newNamespaceName;
        query->getNamespaceArrayNames(newArrayNameOrig, newNamespaceName, newArrayName);
        lock.reset(new LockDesc(newNamespaceName,
                                newArrayName,
                                query->getQueryID(),
                                Cluster::getInstance()->getLocalInstanceId(),
                                LockDesc::COORD,
                                LockDesc::XCL));
        resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::XCL);

        // Need to delete from old namespace, and to create in new namespace.
        rbac::RightsMap& rights = *query->getRights();
        rights.upsert(rbac::ET_NAMESPACE, oldNamespaceName, rbac::P_NS_DELETE);
        rights.upsert(rbac::ET_NAMESPACE, newNamespaceName, rbac::P_NS_CREATE);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRename, "rename")

}  // namespace ops
