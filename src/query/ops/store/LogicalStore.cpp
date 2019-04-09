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
 * LogicalStore.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <rbac/Rights.h>
#include "UniqueNameAssigner.h"

using namespace std;

namespace scidb {
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.logical_store"));

/**
 * @brief The operator: store().
 *
 * @par Synopsis:
 *   store( srcArray, outputArray )
 *
 * @par Summary:
 *   Stores an array to the database. Each execution of store() causes a new version of the array to be created.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - outputArray: an existing array in the database, with the same schema as srcArray.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
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
class LogicalStore: public  LogicalOperator
{
public:
    LogicalStore(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        _properties.noNesting = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_OUT_ARRAY_NAME()
    }

    void inferAccess(std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);
        SCIDB_ASSERT(_parameters.size() > 0);
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& objName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(objName));

        ArrayDesc srcDesc;
        SCIDB_ASSERT(!srcDesc.isTransient());

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        // Throw an exception if the namespace does not exist.
        SystemCatalog& sysCat = *SystemCatalog::getInstance();
        NamespaceDesc nsDesc(namespaceName);
        if (!sysCat.findNamespaceByName(nsDesc)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR,
                                   SCIDB_LE_CANNOT_RESOLVE_NAMESPACE)
                << namespaceName;
        }
        SCIDB_ASSERT(nsDesc.isIdValid());

        SystemCatalog::GetArrayDescArgs args;
        args.result = &srcDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.throwIfNotFound = false;
        args.catalogVersion = SystemCatalog::ANY_VERSION;
        bool found = SystemCatalog::getInstance()->getArrayDesc(args);

        const LockDesc::LockMode lockMode =
            srcDesc.isTransient() ? LockDesc::XCL : LockDesc::WR;

        std::shared_ptr<LockDesc>  lock(
            make_shared<LockDesc>(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                lockMode));
        std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::WR);

        // And ask for needed namespace privs...
        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName,
                                   (found ? rbac::P_NS_UPDATE : rbac::P_NS_CREATE));
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        SCIDB_ASSERT(schemas.size() == 1);
        SCIDB_ASSERT(_parameters.size() == 1);

        const string& objName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(objName));

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        //Ensure attributes names uniqueness.
        ArrayDesc dstDesc;
        ArrayDesc const& srcDesc = schemas[0];
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);

        SystemCatalog::GetArrayDescArgs args;
        args.result = &dstDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.throwIfNotFound = false;
        args.catalogVersion = arrayId;
        bool found = SystemCatalog::getInstance()->getArrayDesc(args);

        if (!found)
        {
            UniqueNameAssigner assigner;
            for (auto const& attr : srcDesc.getAttributes()) {
                assigner.insertName(attr.getName());
            }
            for (auto const& dim : srcDesc.getDimensions()) {
                assigner.insertName(dim.getBaseName());
            }

            Attributes outAttrs;
            outAttrs.reserve(srcDesc.getAttributes().size());
            for (auto const& attr : srcDesc.getAttributes()) {
                outAttrs.push_back(AttributeDesc(
                    attr.getId(),
                    assigner.assignUniqueName(attr.getName()),
                    attr.getType(),
                    attr.getFlags(),
                    attr.getDefaultCompressionMethod(),
                    attr.getAliases(),
                    &attr.getDefaultValue(),
                    attr.getDefaultValueExpr()));
            }
            Dimensions outDims;
            outDims.reserve(srcDesc.getDimensions().size());
            for (auto const& dim : srcDesc.getDimensions()) {
                outDims.push_back(DimensionDesc(
                    assigner.assignUniqueName(dim.getBaseName()),
                    dim.getStartMin(),
                    dim.getCurrStart(),
                    dim.getCurrEnd(),
                    dim.getEndMax(),
                    dim.getRawChunkInterval(),
                    dim.getChunkOverlap()));
            }

            ArrayDistPtr arrDist = srcDesc.getDistribution();
            ArrayResPtr arrRes   = srcDesc.getResidency();
            const bool distribution_and_residency_not_propagated(true);
            if (distribution_and_residency_not_propagated) {
                //XXX TODO: At some point we will take the distribution of the input,
                //XXX TODO: but currently the distribution/residency is not propagated
                //XXX TODO: through the pipeline correctly, so we are forcing it.
                //XXX TODO: Another complication is that SGs are inserted before the physical execution,
                //XXX TODO: During the logical phase, we dont yet know the true distribution
                //XXX TODO: coming into the store() from its children.
                const size_t redundancy = Config::getInstance()->getOption<size_t>(CONFIG_REDUNDANCY);
                arrDist = defaultPartitioning(redundancy);
                arrRes = query->getDefaultArrayResidencyForWrite();
            }
            /* Notice that when storing to a non-existant array, we do not propagate the
               transience of the source array to the target ...*/
            ArrayDesc schema(
                namespaceName, arrayName,
                outAttrs, outDims,
                arrDist, arrRes,
                srcDesc.getFlags() & (~ArrayDesc::TRANSIENT));

            return schema;
        }

        // Check schemas to ensure that the source array can be stored in the destination.  We
        // can ignore overlaps and chunk intervals because our physical operator implements
        // requiresRedimensionOrRepartition() to get automatic repartitioning.
        //
        ArrayDesc::checkConformity(srcDesc, dstDesc,
                                   ArrayDesc::IGNORE_PSCHEME |
                                   ArrayDesc::IGNORE_OVERLAP |
                                   ArrayDesc::IGNORE_INTERVAL |
                                   ArrayDesc::SHORT_OK_IF_EBM);

        Dimensions const& dstDims = dstDesc.getDimensions();
        Dimensions newDims(dstDims.size()); //XXX need this ?
        for (size_t i = 0; i < dstDims.size(); i++) {
            DimensionDesc const& dim = dstDims[i];
            newDims[i] = DimensionDesc(dim.getBaseName(),
                                       dim.getNamesAndAliases(),
                                       dim.getStartMin(), dim.getCurrStart(),
                                       dim.getCurrEnd(), dim.getEndMax(),
                                       dim.getRawChunkInterval(), dim.getChunkOverlap());
        }

        dstDesc.setDimensions(newDims);
        SCIDB_ASSERT(dstDesc.getId() == dstDesc.getUAId() && dstDesc.getName() == arrayName);
        SCIDB_ASSERT(dstDesc.getUAId() > 0);
        return dstDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalStore, "store")

}  // namespace scidb
