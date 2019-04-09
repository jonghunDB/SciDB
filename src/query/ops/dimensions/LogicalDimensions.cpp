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

/**
 * @file LogicalDimensions.cpp
 * @author knizhnik@garret.ru
 * @brief Operator for examining array dimensions.
 */

#include <array/Metadata.h>
#include <query/Operator.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: dimensions().
 *
 * @par Synopsis:
 *   dimensions( srcArray )
 *
 * @par Summary:
 *   List the dimensions of the source array.
 *
 * @par Input:
 *   - srcArray: a source array.
 *
 * @par Output array:
 *        <
 *   <br>   name: string
 *   <br>   start: int64,
 *   <br>   length: uint64
 *   <br>   chunk_interval: int64
 *   <br>   chunk_overlap: uint64
 *   <br>   low: int64
 *   <br>   high: int64
 *   <br>   type: string
 *   <br> >
 *   <br> [
 *   <br>   No: start=0, end=#dimensions less 1, chunk interval=#dimensions.
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
class LogicalDimensions: public LogicalOperator
{
public:
    LogicalDimensions(const string& logicalName, const string& alias)
        : LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_IN_ARRAY_NAME();
    }

    void inferAccess(shared_ptr<Query>& query) override
    {
        // Need catalog read lock and namespace read rights.
        LogicalOperator::inferAccess(query);
        string ns, ary;
        string objName(param<OperatorParamReference>(0)->getObjectName());
        query->getNamespaceArrayNames(objName, ns, ary);
        query->getRights()->upsert(rbac::ET_NAMESPACE, ns, rbac::P_NS_READ);
    }

    ArrayDesc inferSchema(vector< ArrayDesc> inputSchemas, shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayNameOrig = param<OperatorParamReference>(0)->getObjectName();
        string arrayName;
        string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

        ArrayDesc arrayDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &arrayDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.catalogVersion = query->getCatalogVersion(namespaceName, arrayName);
        args.versionId = LAST_VERSION;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        vector<AttributeDesc> attributes(8);
        attributes[0] = AttributeDesc(0, "name",  TID_STRING, 0, CompressorType::NONE);
        attributes[1] = AttributeDesc(1, "start",  TID_INT64, 0, CompressorType::NONE);
        attributes[2] = AttributeDesc(2, "length",  TID_UINT64, 0, CompressorType::NONE);

        // Negative chunk intervals now have special meanings, see DimensionDesc::SpecialIntervals.
        // Unfortunately we can't display '*' or '?' for those values here, oh well.  Being signed
        // also makes RLE and coordinate math easier. We often add the chunk size to a pair of
        // coordinates (which are signed) and having unsigned / signed issues there might be
        // difficult.
        attributes[3] = AttributeDesc(3, "chunk_interval",  TID_INT64, 0, CompressorType::NONE);
        attributes[4] = AttributeDesc(4, "chunk_overlap",  TID_UINT64, 0, CompressorType::NONE);
        attributes[5] = AttributeDesc(5, "low",  TID_INT64, 0, CompressorType::NONE);
        attributes[6] = AttributeDesc(6, "high",  TID_INT64, 0, CompressorType::NONE);
        attributes[7] = AttributeDesc(7, "type",  TID_STRING, 0, CompressorType::NONE);
        vector<DimensionDesc> dimensions(1);

        size_t nDims = arrayDesc.getDimensions().size();
        size_t end    = nDims>0 ? nDims-1 : 0;
        dimensions[0] = DimensionDesc("No", 0, 0, end, end, nDims, 0);

        stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(psLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("Dimensions", attributes, dimensions,
                         localDist,
                         query->getDefaultArrayResidency());
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalDimensions, "dimensions")

} //namespace
